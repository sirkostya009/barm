// UpdateQuery builds and runs an UPDATE of T's table. Columns come from Set or
// from a value's fields, and rows are picked by Where or WherePK, never
// implicitly.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"reflect"
	"slices"
)

// UpdateQuery builds an UPDATE, either from explicit Set expressions or from a
// struct value keyed by its primary key.
type UpdateQuery[T any] struct {
	runner
	table     string // set by Table; otherwise the model's
	schema    string
	model     *model
	ret       *model // what RETURNING scans into; the model unless a result type changed it
	value     reflect.Value
	sets      []frag
	fields    []*field
	wheres    []frag
	returning []frag
	err       error
	withClause
}

func newUpdate[T any](r runner) *UpdateQuery[T] {
	m, err := modelOf[T]()
	return &UpdateQuery[T]{runner: r, model: m, ret: m, err: err}
}

// rowHint knows the count only when the primary key alone picks the row; a
// WHERE of any other shape could match anything, and a wrong guess is worse than
// none.
func (q *UpdateQuery[T]) rowHint() int64 {
	if len(q.wheres) == 1 && q.wheres[0].pk() {
		return 1
	}
	return 0
}

func (q *UpdateQuery[T]) tableName() string {
	if q.table != "" {
		return q.table
	}
	return q.model.table
}

// Schema qualifies the table this writes to, as `schema.table`. It applies to
// a name given with Table too.
func (q *UpdateQuery[T]) Schema(name string) *UpdateQuery[T] {
	q.schema = name
	return q
}

// Table overrides where this writes, for a T whose BaseModel names another
// table or none at all.
func (q *UpdateQuery[T]) Table(name string) *UpdateQuery[T] {
	q.table = name
	return q
}

// Clone returns a copy to build on separately. A builder changes in place and
// hands itself back from every call, so two queries built on from one partial
// query without it would share, and overwrite, each other's clauses.
func (q *UpdateQuery[T]) Clone() *UpdateQuery[T] {
	c := *q
	c.sets, c.fields, c.wheres = slices.Clip(c.sets), slices.Clip(c.fields), slices.Clip(c.wheres)
	c.returning, c.ctes = slices.Clip(c.returning), slices.Clip(c.ctes)
	return &c
}

// Via returns a copy of the query that runs on h instead, leaving q bound to
// where it was started. See [SelectQuery.Via].
func (q *UpdateQuery[T]) Via(h IDB) *UpdateQuery[T] {
	c := *q
	c.h = h
	return &c
}

// Apply threads the query through fns, for composing reusable clauses. A nil fn
// is skipped.
func (q *UpdateQuery[T]) Apply(fns ...func(*UpdateQuery[T]) *UpdateQuery[T]) *UpdateQuery[T] {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// Prepare caches this query as a named prepared statement on the DB.
func (q *UpdateQuery[T]) Prepare(name string) *UpdateQuery[T] {
	q.name = name
	return q
}

// Value binds the struct whose columns are written. It picks no rows: that is
// Where's job, or WherePK's for the row the value is.
func (q *UpdateQuery[T]) Value(v *T) *UpdateQuery[T] {
	if v == nil {
		q.err = errors.New("barm: nil pointer passed to Update")
		return q
	}
	q.value = reflect.ValueOf(v).Elem()
	return q
}

// Column restricts the updated columns to the named ones.
func (q *UpdateQuery[T]) Column(names ...string) *UpdateQuery[T] {
	for _, n := range names {
		f, err := q.model.column(n)
		if err != nil {
			q.err = err
			return q
		}
		q.fields = append(q.fields, f)
	}
	return q
}

// From adds a table the update reads from, as SQL written out, joined to the
// updated table by Where: From("data") for a CTE, From("teams AS t"). Several
// are listed in order.
func (q *UpdateQuery[T]) From(expr string, args ...any) *UpdateQuery[T] {
	q.sets = append(q.sets, frag{sql: expr, args: args, kind: fragFrom})
	return q
}

// Set adds a raw assignment, e.g. Set("hits = hits + ?", 1).
func (q *UpdateQuery[T]) Set(expr string, args ...any) *UpdateQuery[T] {
	q.sets = append(q.sets, frag{sql: expr, args: args})
	return q
}

func (q *UpdateQuery[T]) Where(expr string, args ...any) *UpdateQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args})
	return q
}

func (q *UpdateQuery[T]) WhereOr(expr string, args ...any) *UpdateQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args, kind: fragOr})
	return q
}

// WhereGroup adds the conditions fn adds as one, in parentheses. See
// [SelectQuery.WhereGroup].
func (q *UpdateQuery[T]) WhereGroup(fn func(*UpdateQuery[T]) *UpdateQuery[T]) *UpdateQuery[T] {
	return q.whereGroup(fn, 0)
}

// WhereOrGroup is WhereGroup joined to the conditions before it with OR.
func (q *UpdateQuery[T]) WhereOrGroup(fn func(*UpdateQuery[T]) *UpdateQuery[T]) *UpdateQuery[T] {
	return q.whereGroup(fn, fragOr)
}

func (q *UpdateQuery[T]) whereGroup(fn func(*UpdateQuery[T]) *UpdateQuery[T], kind fragKind) *UpdateQuery[T] {
	start := len(q.wheres)
	if r := fn(q); r != q {
		if q.err == nil {
			q.err = errGroupReturn
		}
		return q
	}
	q.wheres = group(q.wheres, start, kind)
	return q
}

// WherePK adds the primary key of the value given to Value as a condition, in
// its place among the others: `.WherePK().Where("version = ?", v)` updates
// that row only if its version still matches. The key is read when the query is
// built, so Value may come after.
func (q *UpdateQuery[T]) WherePK() *UpdateQuery[T] {
	q.wheres = append(q.wheres, frag{kind: fragPK})
	return q
}

// Returning adds a RETURNING clause. OneAs and SliceAs read the rows it
// produces; Exec ignores them.
func (q *UpdateQuery[T]) Returning(expr string, args ...any) *UpdateQuery[T] {
	q.returning = append(q.returning, frag{sql: expr, args: args})
	return q
}

// With adds a common table expression, e.g. With("recent", sub) for
// `WITH "recent" AS (<sub>)`. The body renders into this query, so the two
// share one argument numbering. Read from it by naming it: Table("recent").
func (q *UpdateQuery[T]) With(name string, sub Query) *UpdateQuery[T] {
	q.addWith(name, sub, false)
	return q
}

// WithRecursive adds a CTE that refers to itself, and marks the whole WITH
// clause RECURSIVE. The body is the anchor unioned with the recursive term:
//
//	db.Select[node]().Table("tree").WithRecursive("tree",
//		db.Select[node]().Where("parent_id IS NULL").
//			UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id")))
func (q *UpdateQuery[T]) WithRecursive(name string, sub Query) *UpdateQuery[T] {
	q.addWith(name, sub, true)
	return q
}

// WithExpr adds a CTE written out in full, for what a name and a body cannot
// express on their own — a column list, or a materialization hint:
//
//	q.WithExpr(`recent(id, seen) AS MATERIALIZED (SELECT id, seen FROM hits WHERE seen > ?)`, cutoff)
//
// The expression is SQL barm does not parse, the same as Join and Where.
func (q *UpdateQuery[T]) WithExpr(expr string, args ...any) *UpdateQuery[T] {
	q.addWithExpr(expr, args)
	return q
}

func (q *UpdateQuery[T]) Build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

func (q *UpdateQuery[T]) argCount() int {
	n := q.withClause.argCount() + len(q.model.fields)
	for _, fs := range [][]frag{q.sets, q.wheres, q.returning} {
		for _, f := range fs {
			n += len(f.args)
		}
	}
	return n
}

// render writes the UPDATE into b.
func (q *UpdateQuery[T]) render(b *builder) error {
	if q.err != nil {
		return q.err
	}
	if q.tableName() == "" {
		return errNoTable
	}
	b.hint(q.argCount())
	err := b.with(q.withClause)
	if err != nil {
		return err
	}
	b.str("UPDATE ").table(q.schema, q.tableName()).str(" SET ")

	n := 0
	value := q.value
	if value.IsValid() {
		value = snapshot(value) // one copy for every column bound off it
		set := func(f *field) {
			if n > 0 {
				b.str(", ")
			}
			b.assign(f, value)
			n++
		}
		if len(q.fields) > 0 {
			for _, f := range q.fields {
				set(f)
			}
		} else {
			for i := range q.model.fields {
				if f := &q.model.fields[i]; !f.pk && !f.skipupdate {
					set(f)
				}
			}
		}
	}
	for _, s := range q.sets {
		if s.from() {
			continue
		}
		if n > 0 {
			b.str(", ")
		}
		b.frag(s)
		n++
	}
	if n == 0 {
		return errors.New("barm: Update has nothing to set")
	}
	b.from(" FROM ", q.sets)

	err = b.where(q.wheres, q.model, value)
	if err != nil {
		return err
	}
	b.returning(q.returning)
	return nil
}

// assign writes `col = ?`, bound to the field's value in v.
func (b *builder) assign(f *field, v reflect.Value) {
	b.ident(f.name).str(" = ").placeholder(b.value(f, fieldValue(v, f.index)))
}

func (q *UpdateQuery[T]) target() target {
	return target{op: "UPDATE", table: q.tableName(), schema: q.schema}
}

func (q *UpdateQuery[T]) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	return q.exec(ctx, q.target(), query, args)
}

// One runs the update and scans the single returned row into T, the query's own
// model type.
func (q *UpdateQuery[T]) One(ctx context.Context) (T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		var zero T
		return zero, err
	}
	return q.one[T](ctx, q.target(), query, args)
}

// Slice runs the update and scans every returned row into T.
func (q *UpdateQuery[T]) Slice(ctx context.Context) ([]T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		return nil, err
	}
	return q.slice[T](ctx, q.target(), query, args, q.rowHint())
}

// Seq runs the update and streams the returned rows as T. A build failure is
// reported through the sequence, like every other error.
func (q *UpdateQuery[T]) Seq(ctx context.Context) iter.Seq2[T, error] {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	return q.seq[T](ctx, q.target(), query, args, err)
}

// OneAs runs the update and scans the single returned row into U.
func (q *UpdateQuery[T]) OneAs[U any](ctx context.Context) (U, error) {
	c := q.retype[U]()
	return c.One(ctx)
}

// SliceAs runs the update and scans every returned row into U.
func (q *UpdateQuery[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	c := q.retype[U]()
	return c.Slice(ctx)
}

// SeqAs runs the update and streams the returned rows as U.
func (q *UpdateQuery[T]) SeqAs[U any](ctx context.Context) iter.Seq2[U, error] {
	c := q.retype[U]()
	return c.Seq(ctx)
}

// BuildAs renders the update as the *As calls would run it, RETURNING clause
// included.
func (q *UpdateQuery[T]) BuildAs[U any]() (string, []any, error) {
	c := q.retype[U]()
	c.expandReturning()
	return c.Build()
}

// retype copies the query for a U result. Only what RETURNING scans into
// changes; the row being written is still T's. The copy is a plain conversion:
// T is in none of the fields, so every instantiation shares one underlying
// struct. It is returned by value so the copy stays on the caller's stack.
func (q *UpdateQuery[T]) retype[U any]() UpdateQuery[U] {
	c := UpdateQuery[U](*q)
	c.returnAs(modelOf[U]())
	return c
}

// returnAs is retype for a result model already looked up.
func (q *UpdateQuery[T]) returnAs(m *model, err error) {
	q.ret = m
	if err != nil && q.err == nil {
		q.err = err
	}
}

// batchAs renders the query as BuildAs does for the model m.
func (q *UpdateQuery[T]) batchAs(m *model, err error, _ bool) (string, []any, error) {
	c := *q
	c.returnAs(m, err)
	c.expandReturning()
	return c.Build()
}

// expandReturning settles the RETURNING clause: the result type's columns
// unless Returning named its own. A failure lands on q.err, where Build
// reports it.
func (q *UpdateQuery[T]) expandReturning() {
	r, err := q.ret.returningCols(q.Dialect(), q.returning)
	if err != nil {
		if q.err == nil {
			q.err = err
		}
		return
	}
	q.returning = r
}
