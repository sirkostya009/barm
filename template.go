package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"unsafe"
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
// An IdentSlot is filled by name too, with an identifier rather than a bound
// value, spliced into the SQL on every call. Each value is SQL of its own, so
// a prepared template runs one, and Prepare gives each another name.
//
// Relations do not load through a template, which keeps the query's SQL and
// not the query, and Exec scans no RETURNING rows back, since there are no
// values of the query's own to scan them into. A template never changes, so
// it is safe for any number of goroutines at once.
type Template[T any] struct {
	h      IDB
	name   string
	t      target
	query  string
	args   []any     // as rendered, a named argument wherever With's values go
	names  []string  // each name once, in the order the query first binds it
	slots  []slot    // where in args each named argument sits
	idents []slot    // where in query each IdentSlot sits, by byte offset, and which of inames it takes
	inames []int     // the names IdentSlots take, each once
	row    []rowSlot // where in args each column of a row given to Values sits
	err    error
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
	var idents identSlotsError
	if errors.As(err, &idents) || errors.Is(err, errNamedArg) {
		err = nil
	}
	tp := &Template[T]{h: r.h, name: r.name, t: t, query: query, args: args, err: err}
	if err == nil {
		tp.index(idents)
	}
	return tp
}

func (t *Template[T]) index(idents identSlotsError) {
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
		t.slots = append(t.slots, slot{at, t.nameIndex(n.Name)})
	}
	bound := len(t.names)
	for _, id := range idents {
		if id.name == "" {
			t.err = errors.New("barm: a template's IdentSlot has no name")
			return
		}
		i := t.nameIndex(id.name)
		if i < bound {
			t.err = fmt.Errorf("barm: the template's %q is both an argument and an IdentSlot", id.name)
			return
		}
		k := slices.Index(t.inames, i)
		if k < 0 {
			k = len(t.inames)
			t.inames = append(t.inames, i)
		}
		t.idents = append(t.idents, slot{id.at, k})
	}
}

func (t *Template[T]) nameIndex(name string) int {
	i := slices.Index(t.names, name)
	if i < 0 {
		i = len(t.names)
		t.names = append(t.names, name)
	}
	return i
}

// With returns the query with each named argument bound to the value args
// gives its name. Every name takes one.
func (t *Template[T]) With(args ...sql.NamedArg) *Bound[T] {
	return &Bound[T]{t: t, h: t.h, args: args}
}

// Prepare returns the template running as the prepared statement name, or
// unprepared for "", leaving t as it was. A name binds to one SQL text, so a
// template with an IdentSlot takes a name of its own for each value it runs
// prepared:
//
//	byAuthor.Prepare("posts_by_" + tenant).With(barm.Named("schema", tenant), ...)
func (t *Template[T]) Prepare(name string) *Template[T] {
	c := *t
	c.name = name
	return &c
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

// Prepare is Template.Prepare for an insert's template.
func (t *InsertTemplate[T, U]) Prepare(name string) *InsertTemplate[T, U] {
	return &InsertTemplate[T, U]{t.Template.Prepare(name)}
}

// Values returns the query writing row, with each named argument bound to the
// value args gives its name. The row is read as it binds, so a change to it
// after that is not written.
func (t *InsertTemplate[T, U]) Values(row *T, args ...sql.NamedArg) *Bound[U] {
	return &Bound[U]{t: t.Template, h: t.h, args: args, row: row}
}

// bind fills the named arguments in with args, and the row's columns with
// row's, encoded for the session they run in, and returns the SQL its
// IdentSlots take.
func (t *Template[T]) bind(s *session, args []sql.NamedArg, row any) (string, []any, error) {
	if t.err != nil {
		return "", nil, t.err
	}
	rv := reflect.ValueOf(row)
	switch {
	case len(t.row) > 0 && (row == nil || rv.IsNil()):
		return "", nil, errors.New("barm: the template writes a row, which Values gives")
	case len(t.row) == 0 && row != nil:
		return "", nil, errors.New("barm: the template writes no row of its own, so Values takes none")
	}
	if len(args) != len(t.names) {
		return "", nil, fmt.Errorf("barm: the template takes %d named arguments, With was given %d", len(t.names), len(args))
	}
	// which of args each name takes; with as many arguments as names, a name
	// given once each means none is missing
	var buf [8]int
	given := buf[:0]
	if len(t.names) > len(buf) {
		given = make([]int, 0, len(t.names))
	}
	given = given[:len(t.names)]
	for i := range given {
		given[i] = -1
	}
	for k, arg := range args {
		i := slices.Index(t.names, arg.Name)
		switch {
		case i < 0:
			return "", nil, fmt.Errorf("barm: the template has no argument named %q", arg.Name)
		case given[i] >= 0:
			return "", nil, fmt.Errorf("barm: With was given %q twice", arg.Name)
		}
		given[i] = k
	}
	query := t.query
	if len(t.idents) > 0 {
		var err error
		query, err = t.splice(args, given)
		if err != nil {
			return "", nil, err
		}
	}
	out := slices.Clone(t.args)
	for _, sl := range t.slots {
		out[sl.at] = args[given[sl.name]].Value
	}
	if len(t.row) > 0 {
		snap := snapshot(rv.Elem()) // one copy, however many columns come off it
		for _, r := range t.row {
			v, err := fieldArg(r.f, fieldValue(snap, r.f.index), s.nativeJSON, s.nativeArrays)
			if err != nil {
				return "", nil, err
			}
			out[r.at] = v
		}
	}
	return query, out, nil
}

// splice writes the identifiers args gives into the SQL, its buffer sized up
// front.
func (t *Template[T]) splice(args []sql.NamedArg, given []int) (string, error) {
	var vbuf [4]string
	vals := vbuf[:0]
	for _, i := range t.inames {
		v, err := identValue(args[given[i]])
		if err != nil {
			return "", err
		}
		vals = append(vals, v)
	}
	n := len(t.query)
	for _, id := range t.idents {
		n += len(vals[id.name]) + 2 // quoted; a quote or dot inside grows it
	}

	d := t.h.Dialect() // what the rest of the SQL was rendered for
	b := make([]byte, 0, n)
	prev := 0
	for _, id := range t.idents {
		b = append(b, t.query[prev:id.at]...)
		b = d.AppendIdent(b, vals[id.name])
		prev = id.at
	}
	b = append(b, t.query[prev:]...)
	return unsafe.String(unsafe.SliceData(b), len(b)), nil
}

// identValue is the identifier an IdentSlot is given: a string, or an Ident.
func identValue(a sql.NamedArg) (string, error) {
	var v string
	switch av := a.Value.(type) {
	case string:
		v = av
	case ident:
		v = string(av)
	default:
		return "", fmt.Errorf("barm: the template's IdentSlot %q takes a string, With gave %T", a.Name, a.Value)
	}
	if v == "" {
		return "", fmt.Errorf("barm: the template's IdentSlot %q was given an empty name", a.Name)
	}
	return v, nil
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

// Seq streams every row as T. See SelectQuery.Seq.
func (q *Bound[T]) Seq(ctx context.Context) iter.Seq2[T, error] { return q.SeqAs[T](ctx) }

// OneAs reads the first row as U, mapping the columns the query returns by
// name, or ErrNoRows.
func (q *Bound[T]) OneAs[U any](ctx context.Context) (U, error) {
	query, args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		var zero U
		return zero, err
	}
	rn := runner{q.h, q.t.name}
	return rn.one[U](ctx, q.t.t, query, args)
}

// SliceAs reads every row as U, mapping the columns the query returns by name.
func (q *Bound[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	query, args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		return nil, err
	}
	rn := runner{q.h, q.t.name}
	return rn.slice[U](ctx, q.t.t, query, args, 0)
}

// SeqAs streams every row as U, mapping the columns the query returns by name.
func (q *Bound[T]) SeqAs[U any](ctx context.Context) iter.Seq2[U, error] {
	query, args, err := q.t.bind(q.h.sess(), q.args, q.row)
	rn := runner{q.h, q.t.name}
	return rn.seq[U](ctx, q.t.t, query, args, err)
}

// Exec runs the query for its row count.
func (q *Bound[T]) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := q.t.bind(q.h.sess(), q.args, q.row)
	if err != nil {
		return nil, err
	}
	rn := runner{q.h, q.t.name}
	return rn.exec(ctx, q.t.t, query, args)
}

// Build returns the template's SQL with the arguments bound, encoded for the
// handle it runs on.
func (q *Bound[T]) Build() (string, []any, error) { return q.buildIn(q.h.sess()) }

// buildIn is Build for a batch, whose session the arguments are encoded for.
func (q *Bound[T]) buildIn(s *session) (string, []any, error) {
	return q.t.bind(s, q.args, q.row)
}

func (q *Bound[T]) stmtName() string { return q.t.name }

func (*Bound[T]) render(*builder) error { return errTemplateNested }
func (q *Bound[T]) argCount() int       { return len(q.t.args) }
func (q *Bound[T]) target() target      { return q.t.t }
