package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

// Named is sql.Named, for a Template's arguments without importing
// database/sql.
func Named(name string, value any) sql.NamedArg { return sql.NamedArg{Name: name, Value: value} }

var (
	errNamedArg          = errors.New("barm: a named argument binds only through a Template's With")
	errTemplateNested    = errors.New("barm: a Template's query runs on its own, not inside another")
	errTemplateRelations = errors.New("barm: relations do not load through a Template")
)

// Template is a query rendered once, its SQL kept with where each named
// argument sits, and run again with new values for them. A named argument with
// no value stands for each, and With binds them by name:
//
//	var qb = barm.NewBuilder(barm.Postgres)
//
//	var insertPost = qb.Insert[CreatePost]().Table("posts").Select(
//		qb.Select[User]().ColumnExpr("u.id, ?, ?", barm.Named("title", nil), barm.Named("body", nil)).
//			Where("u.id = ?", barm.Named("author", nil))).TemplateAs[Post]()
//
//	p, err := insertPost.With(barm.Named("title", t), barm.Named("body", b), barm.Named("author", a)).
//		Via(tx).One(ctx)
//
// Nothing of the builder is kept, so a builder made only to be templated is
// garbage once Template returns. A bound template queues in a batch as any
// query does, and a loop queueing it per element pays for its arguments alone.
//
// Template renders the query as written, and TemplateAs as BuildAs does: a
// select narrowed to U's columns, a write returning them. That is the only
// shape it runs in: its OneAs reads U off whatever columns the SQL returns,
// and its One adds no LIMIT. A query with a named argument fails to build any
// way but these.
//
// Relations do not load through a template, which keeps the query's SQL and
// not the query, and Exec scans no RETURNING rows back, since there are no
// values of the query's own to scan them into. A template never changes, so
// it is safe for any number of goroutines at once.
type Template[T any] struct {
	h     IDB
	name  string
	t     target
	query string
	args  []any     // as rendered, a named argument wherever With's values go
	names []string  // each name once, in the order the query first binds it
	slots []slot    // where in args each named argument sits
	row   []rowSlot // where in args each column of a row given to Values sits
	err   error
}

type slot struct{ at, name int }

type rowSlot struct {
	at int
	f  *field
}

// rowField marks where a column of a template's row binds.
type rowField struct{ f *field }

// newTemplate keeps a rendered query and finds its named arguments.
func newTemplate[T any](r runner, t target, query string, args []any, err error) *Template[T] {
	if errors.Is(err, errNamedArg) {
		err = nil
	}
	tp := &Template[T]{h: r.h, name: r.name, t: t, query: query, args: args, err: err}
	if err == nil {
		tp.index()
	}
	return tp
}

func (t *Template[T]) index() {
	for at, v := range t.args {
		if r, ok := v.(rowField); ok {
			t.row = append(t.row, rowSlot{at, r.f})
			continue
		}
		n, ok := v.(sql.NamedArg)
		if !ok {
			continue
		}
		switch {
		case n.Name == "":
			t.err = errors.New("barm: a template's named argument has no name")
			return
		case n.Value != nil:
			t.err = fmt.Errorf("barm: the template's argument %q has a value, which With gives instead", n.Name)
			return
		}
		i := slices.Index(t.names, n.Name)
		if i < 0 {
			i = len(t.names)
			t.names = append(t.names, n.Name)
		}
		t.slots = append(t.slots, slot{at, i})
	}
}

// With returns the query with each named argument bound to the value args
// gives its name. Every name takes one.
func (t *Template[T]) With(args ...sql.NamedArg) *Bound[T] {
	return &Bound[T]{t: t, h: t.h, args: args}
}

// InsertTemplate is an insert's Template, which takes the row it writes as T
// and reads what it returns as U:
//
//	var insertUser = qb.Insert[User]().TemplateAs[User]()
//
//	u, err := insertUser.Values(&User{Name: "ann"}).Via(db).One(ctx)
//
// Its columns are those the insert would write for a row setting every one: a
// column with a default only when Column names it, and then written as given,
// zero or not.
type InsertTemplate[T, U any] struct{ *Template[U] }

// Values returns the query writing row, with each named argument bound to the
// value args gives its name. The row is read as it binds, so a change to it
// after that is not written.
func (t *InsertTemplate[T, U]) Values(row *T, args ...sql.NamedArg) *Bound[U] {
	return &Bound[U]{t: t.Template, h: t.h, args: args, row: row}
}

// bind fills the named arguments in with args, and the row's columns with
// row's, encoded for the session they run in.
func (t *Template[T]) bind(s *session, args []sql.NamedArg, row any) ([]any, error) {
	if t.err != nil {
		return nil, t.err
	}
	rv := reflect.ValueOf(row)
	switch {
	case len(t.row) > 0 && (row == nil || rv.IsNil()):
		return nil, errors.New("barm: the template writes a row, which Values gives")
	case len(t.row) == 0 && row != nil:
		return nil, errors.New("barm: the template writes no row of its own, so Values takes none")
	}
	if len(args) != len(t.names) {
		return nil, fmt.Errorf("barm: the template takes %d named arguments, With was given %d", len(t.names), len(args))
	}
	out := slices.Clone(t.args)
	for _, arg := range args {
		i := slices.Index(t.names, arg.Name)
		if i < 0 {
			return nil, fmt.Errorf("barm: the template has no argument named %q", arg.Name)
		}
		for _, s := range t.slots {
			if s.name == i {
				out[s.at] = arg.Value
			}
		}
	}
	// as many arguments as names, so a name given twice leaves another out
	for _, sl := range t.slots {
		if _, ok := out[sl.at].(sql.NamedArg); ok {
			return nil, fmt.Errorf("barm: With was not given %q", t.names[sl.name])
		}
	}
	if len(t.row) > 0 {
		snap := snapshot(rv.Elem()) // one copy, however many columns come off it
		for _, r := range t.row {
			v, err := fieldArg(r.f, fieldValue(snap, r.f.index), s.nativeJSON, s.nativeArrays)
			if err != nil {
				return nil, err
			}
			out[r.at] = v
		}
	}
	return out, nil
}

// Bound is a Template's query with its arguments, see Template.
type Bound[T any] struct {
	t    *Template[T]
	h    IDB // the template's, unless Via changed it
	args []sql.NamedArg
	row  any // the *T an InsertTemplate writes, nil for any other
}

// Via runs the query on h rather than where the template's was started, which
// for a template built on a NewBuilder DB is the only place it can run. Unlike
// a builder's Via it changes q, which With made for this one call.
func (q *Bound[T]) Via(h IDB) *Bound[T] {
	q.h = h
	return q
}

// One reads the first row as T, or ErrNoRows.
func (q *Bound[T]) One(ctx context.Context) (T, error) { return q.OneAs[T](ctx) }

// Slice reads every row as T.
func (q *Bound[T]) Slice(ctx context.Context) ([]T, error) { return q.SliceAs[T](ctx) }

// OneAs reads the first row as U, mapping the columns the query returns by
// name, or ErrNoRows.
func (q *Bound[T]) OneAs[U any](ctx context.Context) (U, error) {
	args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		var zero U
		return zero, err
	}
	r := runner{q.h, q.t.name}
	return r.one[U](ctx, q.t.t, q.t.query, args)
}

// SliceAs reads every row as U, mapping the columns the query returns by name.
func (q *Bound[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		return nil, err
	}
	r := runner{q.h, q.t.name}
	return r.slice[U](ctx, q.t.t, q.t.query, args, 0)
}

// Exec runs the query for its row count.
func (q *Bound[T]) Exec(ctx context.Context) (sql.Result, error) {
	args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		return nil, err
	}
	r := runner{q.h, q.t.name}
	return r.exec(ctx, q.t.t, q.t.query, args)
}

// Build returns the template's SQL with the arguments bound, encoded for the
// handle it runs on.
func (q *Bound[T]) Build() (string, []any, error) { return q.buildIn(q.h.sess()) }

// buildIn is Build for a batch, whose session the arguments are encoded for.
func (q *Bound[T]) buildIn(s *session) (string, []any, error) {
	args, err := q.t.bind(s, q.args, q.row)
	if err != nil {
		return "", nil, err
	}
	return q.t.query, args, nil
}

func (*Bound[T]) render(*builder) error { return errTemplateNested }
func (q *Bound[T]) argCount() int       { return len(q.t.args) }
func (q *Bound[T]) target() target      { return q.t.t }
func (q *Bound[T]) stmtName() string    { return q.t.name }
