// DeleteQuery builds and runs a DELETE against T's table. Rows are picked by
// Where, or by WherePK from the value given, never implicitly.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"reflect"
	"slices"
)

// DeleteQuery builds a DELETE.
type DeleteQuery[T any] struct {
	runner
	table     string // set by Table; otherwise the model's
	schema    string
	model     *model
	ret       *model // what RETURNING scans into; the model unless a result type changed it
	value     reflect.Value
	wheres    []frag
	returning []frag
	err       error
	withClause
}

func newDelete[T any](r runner) *DeleteQuery[T] {
	m, err := modelOf[T]()
	return &DeleteQuery[T]{runner: r, model: m, ret: m, err: err}
}

// rowHint knows the count only when the primary key alone picks the row; a
// WHERE of any other shape could match anything, and a wrong guess is worse than
// none.
func (q *DeleteQuery[T]) rowHint() int64 {
	if len(q.wheres) == 1 && q.wheres[0].pk() {
		return 1
	}
	return 0
}

func (q *DeleteQuery[T]) tableName() string {
	if q.table != "" {
		return q.table
	}
	return q.model.table
}

// Schema qualifies the table this writes to, as `schema.table`. It applies to
// a name given with Table too.
func (q *DeleteQuery[T]) Schema(name string) *DeleteQuery[T] {
	q.schema = name
	return q
}

// Table overrides where this writes, for a T whose BaseModel names another
// table or none at all.
func (q *DeleteQuery[T]) Table(name string) *DeleteQuery[T] {
	q.table = name
	return q
}

// Clone returns a copy to build on separately. A builder changes in place and
// hands itself back from every call, so two queries built on from one partial
// query without it would share, and overwrite, each other's clauses.
func (q *DeleteQuery[T]) Clone() *DeleteQuery[T] {
	c := *q
	c.wheres, c.returning, c.ctes = slices.Clip(c.wheres), slices.Clip(c.returning), slices.Clip(c.ctes)
	return &c
}

// Via returns a copy of the query that runs on h instead, leaving q bound to
// where it was started. See [SelectQuery.Via].
func (q *DeleteQuery[T]) Via(h IDB) *DeleteQuery[T] {
	c := *q
	c.h = h
	return &c
}

// Apply threads the query through fns, for composing reusable clauses. A nil fn
// is skipped.
func (q *DeleteQuery[T]) Apply(fns ...func(*DeleteQuery[T]) *DeleteQuery[T]) *DeleteQuery[T] {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// Prepare caches this query as a named prepared statement on the DB.
func (q *DeleteQuery[T]) Prepare(name string) *DeleteQuery[T] {
	q.name = name
	return q
}

// Value binds the struct whose primary key WherePK matches. On its own it picks
// no rows.
func (q *DeleteQuery[T]) Value(v *T) *DeleteQuery[T] {
	if v == nil {
		q.err = errors.New("barm: nil pointer passed to Delete")
		return q
	}
	q.value = reflect.ValueOf(v).Elem()
	return q
}

// Using adds a table the delete matches rows against, as SQL written out,
// joined to the deleted table by Where: Using("data") for a CTE. Several are
// listed in order.
func (q *DeleteQuery[T]) Using(expr string, args ...any) *DeleteQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args, kind: fragFrom})
	return q
}

func (q *DeleteQuery[T]) Where(expr string, args ...any) *DeleteQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args})
	return q
}

func (q *DeleteQuery[T]) WhereOr(expr string, args ...any) *DeleteQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args, kind: fragOr})
	return q
}

// WhereGroup adds the conditions fn adds as one, in parentheses. See
// [SelectQuery.WhereGroup].
func (q *DeleteQuery[T]) WhereGroup(fn func(*DeleteQuery[T]) *DeleteQuery[T]) *DeleteQuery[T] {
	return q.whereGroup(fn, 0)
}

// WhereOrGroup is WhereGroup joined to the conditions before it with OR.
func (q *DeleteQuery[T]) WhereOrGroup(fn func(*DeleteQuery[T]) *DeleteQuery[T]) *DeleteQuery[T] {
	return q.whereGroup(fn, fragOr)
}

func (q *DeleteQuery[T]) whereGroup(fn func(*DeleteQuery[T]) *DeleteQuery[T], kind fragKind) *DeleteQuery[T] {
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
// its place among the others: `.WherePK().Where("version = ?", v)` deletes that
// row only if its version still matches. The key is read when the query is
// built, so Value may come after.
func (q *DeleteQuery[T]) WherePK() *DeleteQuery[T] {
	q.wheres = append(q.wheres, frag{kind: fragPK})
	return q
}

// Returning adds a RETURNING clause. OneAs and SliceAs read the rows it
// produces; Exec ignores them.
func (q *DeleteQuery[T]) Returning(expr string, args ...any) *DeleteQuery[T] {
	q.returning = append(q.returning, frag{sql: expr, args: args})
	return q
}

// With adds a common table expression, e.g. With("recent", sub) for
// `WITH "recent" AS (<sub>)`. The body renders into this query, so the two
// share one argument numbering. Read from it by naming it: Table("recent").
func (q *DeleteQuery[T]) With(name string, sub Query) *DeleteQuery[T] {
	q.addWith(name, sub, false)
	return q
}

// WithRecursive adds a CTE that refers to itself, and marks the whole WITH
// clause RECURSIVE. The body is the anchor unioned with the recursive term:
//
//	db.Select[node]().Table("tree").WithRecursive("tree",
//		db.Select[node]().Where("parent_id IS NULL").
//			UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id")))
func (q *DeleteQuery[T]) WithRecursive(name string, sub Query) *DeleteQuery[T] {
	q.addWith(name, sub, true)
	return q
}

// WithExpr adds a CTE written out in full, for what a name and a body cannot
// express on their own — a column list, or a materialization hint:
//
//	q.WithExpr(`recent(id, seen) AS MATERIALIZED (SELECT id, seen FROM hits WHERE seen > ?)`, cutoff)
//
// The expression is SQL barm does not parse, the same as Join and Where.
func (q *DeleteQuery[T]) WithExpr(expr string, args ...any) *DeleteQuery[T] {
	q.addWithExpr(expr, args)
	return q
}

func (q *DeleteQuery[T]) Build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

func (q *DeleteQuery[T]) argCount() int {
	n := q.withClause.argCount()
	for _, f := range q.wheres {
		n += len(f.args)
		if f.pk() {
			n += len(q.model.pks)
		}
	}
	for _, f := range q.returning {
		n += len(f.args)
	}
	return n
}

// render writes the DELETE into b.
func (q *DeleteQuery[T]) render(b *builder) error {
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
	b.str("DELETE FROM ").table(q.schema, q.tableName())
	b.from(" USING ", q.wheres)
	err = b.where(q.wheres, q.model, q.value)
	if err != nil {
		return err
	}
	b.returning(q.returning)
	return nil
}

func (q *DeleteQuery[T]) target() target {
	return target{op: "DELETE", table: q.tableName(), schema: q.schema}
}

func (q *DeleteQuery[T]) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	return q.exec(ctx, q.target(), query, args)
}

// One runs the delete and scans the single returned row into T, the query's own
// model type.
func (q *DeleteQuery[T]) One(ctx context.Context) (T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		var zero T
		return zero, err
	}
	return q.one[T](ctx, q.target(), query, args)
}

// Slice runs the delete and scans every returned row into T, which is how you
// see what a bulk delete actually removed.
func (q *DeleteQuery[T]) Slice(ctx context.Context) ([]T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		return nil, err
	}
	return q.slice[T](ctx, q.target(), query, args, q.rowHint())
}

// Seq runs the delete and streams the removed rows as T, so a big delete need
// not be collected into a slice to be logged or shipped somewhere. A build
// failure is reported through the sequence, like every other error.
func (q *DeleteQuery[T]) Seq(ctx context.Context) iter.Seq2[T, error] {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	return q.seq[T](ctx, q.target(), query, args, err)
}

// OneAs runs the delete and scans the single returned row into U.
func (q *DeleteQuery[T]) OneAs[U any](ctx context.Context) (U, error) {
	c := q.retype[U]()
	return c.One(ctx)
}

// SliceAs runs the delete and scans every returned row into U.
func (q *DeleteQuery[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	c := q.retype[U]()
	return c.Slice(ctx)
}

// SeqAs runs the delete and streams the removed rows as U.
func (q *DeleteQuery[T]) SeqAs[U any](ctx context.Context) iter.Seq2[U, error] {
	c := q.retype[U]()
	return c.Seq(ctx)
}

// BuildAs renders the delete as the *As calls would run it, RETURNING clause
// included.
func (q *DeleteQuery[T]) BuildAs[U any]() (string, []any, error) {
	c := q.retype[U]()
	c.expandReturning()
	return c.Build()
}

// retype copies the query for a U result. Only what RETURNING scans into
// changes; the rows being removed are still picked by T. The copy is a plain
// conversion: T is in none of the fields, so every instantiation shares one
// underlying struct. It is returned by value so the copy stays on the caller's
// stack.
func (q *DeleteQuery[T]) retype[U any]() DeleteQuery[U] {
	c := DeleteQuery[U](*q)
	c.returnAs(modelOf[U]())
	return c
}

// returnAs is retype for a result model already looked up.
func (q *DeleteQuery[T]) returnAs(m *model, err error) {
	q.ret = m
	if err != nil && q.err == nil {
		q.err = err
	}
}

// batchAs renders the query as BuildAs does for the model m.
func (q *DeleteQuery[T]) batchAs(m *model, err error, _ bool) (string, []any, error) {
	c := *q
	c.returnAs(m, err)
	c.expandReturning()
	return c.Build()
}

// expandReturning settles the RETURNING clause: the result type's columns
// unless Returning named its own. A failure lands on q.err, where Build
// reports it.
func (q *DeleteQuery[T]) expandReturning() {
	r, err := q.ret.returningCols(q.Dialect(), q.returning)
	if err != nil {
		if q.err == nil {
			q.err = err
		}
		return
	}
	q.returning = r
}
