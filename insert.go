// InsertQuery builds and runs an INSERT of one or many values of T. Zero auto
// keys and zero columns with a default are left to the database. ON CONFLICT is
// written by hand through On and Set, and RETURNING writes what the database
// generated back into the values that were inserted.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strings"
)

// InsertQuery builds a multi-row INSERT from struct values.
type InsertQuery[T any] struct {
	table     string // set by Table; otherwise the model's
	schema    string
	model     *model
	ret       *model          // what RETURNING scans into; the model unless a result type changed it
	rows      []reflect.Value // the values to write, addressable for RETURNING
	fields    []*field
	returning []frag
	conflict  *upsert
	err       error
	runner
	withClause
}

func newInsert[T any](r runner) *InsertQuery[T] {
	m, err := modelOf[T]()
	return &InsertQuery[T]{runner: r, model: m, ret: m, err: err}
}

// Schema qualifies the table this writes to, as `schema.table`. It applies to
// a name given with Table too.
func (q *InsertQuery[T]) Schema(name string) *InsertQuery[T] {
	q.schema = name
	return q
}

// Table overrides where this writes, for a T whose BaseModel names another
// table or none at all.
func (q *InsertQuery[T]) Table(name string) *InsertQuery[T] {
	q.table = name
	return q
}

// Clone returns a copy to build on separately. A builder changes in place and
// hands itself back from every call, so two queries built on from one partial
// query without it would share, and overwrite, each other's clauses.
func (q *InsertQuery[T]) Clone() *InsertQuery[T] {
	c := *q
	c.rows, c.fields = slices.Clip(c.rows), slices.Clip(c.fields)
	c.returning, c.ctes = slices.Clip(c.returning), slices.Clip(c.ctes)
	return &c
}

// Via returns a copy of the query that runs on h instead, leaving q bound to
// where it was started. See [SelectQuery.Via].
func (q *InsertQuery[T]) Via(h IDB) *InsertQuery[T] {
	c := *q
	c.h = h
	return &c
}

// Apply threads the query through fns, for composing reusable clauses. A nil fn
// is skipped.
func (q *InsertQuery[T]) Apply(fns ...func(*InsertQuery[T]) *InsertQuery[T]) *InsertQuery[T] {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// Prepare caches this query as a named prepared statement on the DB.
func (q *InsertQuery[T]) Prepare(name string) *InsertQuery[T] {
	q.name = name
	return q
}

// Values adds rows to write, named for the VALUES clause it renders. Pointers,
// because RETURNING and LastInsertId scan back into them; a slice goes in
// spread, Values(rows...).
func (q *InsertQuery[T]) Values(values ...*T) *InsertQuery[T] {
	for _, v := range values {
		if v == nil {
			q.err = errors.New("barm: nil pointer passed to Insert")
			return q
		}
		q.rows = append(q.rows, reflect.ValueOf(v).Elem())
	}
	return q
}

// Column restricts the inserted columns.
func (q *InsertQuery[T]) Column(names ...string) *InsertQuery[T] {
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

// upsert is an insert's ON clause and the assignments Set adds to it. It is
// replaced rather than changed, so a Clone setting its own leaves the other be.
type upsert struct {
	on  frag
	set []frag
}

// On appends a conflict clause, e.g. On("CONFLICT (email) DO NOTHING"), or one
// that updates, whose assignments Set adds: On("CONFLICT (email) DO UPDATE").
// barm does not read it, so rows RETURNING hands back go to the values in
// order, and only when every value came back — see Exec.
func (q *InsertQuery[T]) On(expr string, args ...any) *InsertQuery[T] {
	q.conflict = &upsert{on: frag{sql: expr, args: args}}
	return q
}

// Set adds an assignment to an ON clause that updates, e.g. Set("name =
// excluded.name"). The assignments follow SET where the clause ends in DO
// UPDATE, as Postgres and sqlite write it, and the clause itself on MySQL's ON
// DUPLICATE KEY UPDATE.
func (q *InsertQuery[T]) Set(expr string, args ...any) *InsertQuery[T] {
	if q.conflict == nil {
		if q.err == nil {
			q.err = errors.New("barm: Set follows On")
		}
		return q
	}
	c := *q.conflict
	c.set = append(slices.Clip(c.set), frag{sql: expr, args: args})
	q.conflict = &c
	return q
}

// Returning adds a RETURNING clause; results are scanned back into the
// pointers passed to Value.
func (q *InsertQuery[T]) Returning(expr string, args ...any) *InsertQuery[T] {
	q.returning = append(q.returning, frag{sql: expr, args: args})
	return q
}

// insertFields picks the columns to write: explicit ones, otherwise every field
// the database cannot fill in itself. A column left out is a column the database
// defaults — which is the point of `default:`, and of an auto primary key that
// nothing set.
func (q *InsertQuery[T]) insertFields() []*field {
	if len(q.fields) > 0 {
		return q.fields
	}
	out := make([]*field, 0, len(q.model.fields))
	for i := range q.model.fields {
		f := &q.model.fields[i]
		if f.def != "" && q.allZero(f) {
			continue
		}
		out = append(out, f)
	}
	return out
}

func (q *InsertQuery[T]) allZero(f *field) bool {
	for _, row := range q.rows {
		if !fieldValue(row, f.index).IsZero() {
			return false
		}
	}
	return true
}

// snapshot copies a row so its fields can be bound without allocating one by
// one. reflect copies an addressable value on its way into an interface — the
// interface must not alias a variable that could still change — so binding N
// columns straight off the caller's struct costs N allocations. Reading them off
// an unaddressable copy instead costs one, for the whole row, however wide it is.
//
// It also means the arguments are a snapshot: a value mutated after Build, while
// the query is in flight, cannot change what was already bound.
func snapshot(v reflect.Value) reflect.Value { return reflect.ValueOf(v.Interface()) }

func fieldValue(v reflect.Value, index []int) reflect.Value {
	for k, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				// Nothing behind it to read: the field the rest of the path leads
				// to is its zero.
				return reflect.Zero(v.Type().Elem().FieldByIndex(index[k:]).Type)
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v
}

// rowHint is how many rows a RETURNING clause will produce: one per row written.
func (q *InsertQuery[T]) rowHint() int64 { return int64(len(q.rows)) }

func (q *InsertQuery[T]) tableName() string {
	if q.table != "" {
		return q.table
	}
	return q.model.table
}

// With adds a common table expression, e.g. With("recent", sub) for
// `WITH "recent" AS (<sub>)`. The body renders into this query, so the two
// share one argument numbering. Read from it by naming it: Table("recent").
func (q *InsertQuery[T]) With(name string, sub Query) *InsertQuery[T] {
	q.addWith(name, sub, false)
	return q
}

// WithRecursive adds a CTE that refers to itself, and marks the whole WITH
// clause RECURSIVE. The body is the anchor unioned with the recursive term:
//
//	db.Select[node]().Table("tree").WithRecursive("tree",
//		db.Select[node]().Where("parent_id IS NULL").
//			UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id")))
func (q *InsertQuery[T]) WithRecursive(name string, sub Query) *InsertQuery[T] {
	q.addWith(name, sub, true)
	return q
}

// WithExpr adds a CTE written out in full, for what a name and a body cannot
// express on their own — a column list, or a materialization hint:
//
//	q.WithExpr(`recent(id, seen) AS MATERIALIZED (SELECT id, seen FROM hits WHERE seen > ?)`, cutoff)
//
// The expression is SQL barm does not parse, the same as Join and Where.
func (q *InsertQuery[T]) WithExpr(expr string, args ...any) *InsertQuery[T] {
	q.addWithExpr(expr, args)
	return q
}

func (q *InsertQuery[T]) Build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

// argCount sizes the args slice in one shot: every column of every row, plus
// whatever the CTEs and the conflict clause bind.
func (q *InsertQuery[T]) argCount() int {
	n := len(q.rows)*len(q.model.fields) + q.withClause.argCount()
	if c := q.conflict; c != nil {
		n += len(c.on.args)
		for _, s := range c.set {
			n += len(s.args)
		}
	}
	return n
}

// render writes the INSERT into b.
func (q *InsertQuery[T]) render(b *builder) error {
	if q.err != nil {
		return q.err
	}
	d := q.Dialect()
	if q.tableName() == "" {
		return errNoTable
	}
	if len(q.rows) == 0 {
		return errors.New("barm: Insert has no values")
	}
	fields := q.insertFields()
	if len(fields) == 0 {
		return errors.New("barm: Insert has no columns")
	}

	b.hint(q.argCount())
	err := b.with(q.withClause)
	if err != nil {
		return err
	}
	b.str("INSERT INTO ").
		table(q.schema, q.tableName()).
		str(" (")
	for i, f := range fields {
		if i > 0 {
			b.str(", ")
		}
		b.ident(f.name)
	}
	b.str(") VALUES ")
	for i, row := range q.rows {
		if i > 0 {
			b.str(", ")
		}
		b.byte('(')
		snap := snapshot(row) // one copy, however many columns come off it
		for j, f := range fields {
			if j > 0 {
				b.str(", ")
			}
			v := fieldValue(snap, f.index)
			// The column survived insertFields, so some row sets it — but this
			// row does not, and binding its zero would overwrite the default
			// the other rows are relying on. DEFAULT says that without barm
			// having to be right about what the default is; the expression from
			// the tag is the fallback for dialects that will not take it.
			if f.def != "" && v.IsZero() {
				if d.HasDefaultKeyword() {
					b.str("DEFAULT")
				} else {
					b.str(f.def)
				}
				continue
			}
			b.placeholder(b.value(f, v))
		}
		b.byte(')')
	}
	q.renderConflict(b)
	return nil
}

// Exec runs the insert. With a RETURNING clause the returned columns are
// scanned back into the pointers given to Value; without one, and on a dialect
// exposing LastInsertId, a single auto primary key is filled in — except after
// a conflict clause on a dialect with RETURNING, which is how the key comes
// back there.
func (q *InsertQuery[T]) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	if len(q.returning) > 0 {
		n, err := q.execReturning(ctx, query, args)
		return rowsAffected(n), err
	}
	res, err := q.exec(ctx, query, args)
	if err != nil || len(q.rows) != 1 {
		return res, err
	}
	q.setLastInsertID(res)
	return res, nil
}

func (q *InsertQuery[T]) setLastInsertID(res sql.Result) {
	if len(q.model.pks) != 1 || !q.model.pks[0].auto {
		return
	}
	// sqlite's last insert id is the connection's rather than the statement's:
	// an insert that conflicted and wrote nothing, or updated instead, reports
	// the previous insert's. A dialect with RETURNING reads the key back through
	// that; one without — MySQL — reports it per statement, and 0 when nothing
	// was inserted.
	if q.conflict != nil && q.Dialect().HasReturning() {
		return
	}
	id, err := res.LastInsertId()
	if err != nil || id == 0 {
		return
	}
	f := fieldValue(q.rows[0], q.model.pks[0].index)
	if !f.CanSet() || !f.IsZero() {
		return
	}
	if f.CanInt() {
		f.SetInt(id)
		return
	}
	if f.CanUint() {
		if id > 0 {
			f.SetUint(uint64(id))
		}
		return
	}
	// A nullable key takes it the way a row would have: sql.NullInt64 and
	// sql.Null[int64] are Scanners, and an int64 is what the driver hands one.
	if s, ok := reflect.TypeAssert[sql.Scanner](f.Addr()); ok {
		_ = s.Scan(id)
	}
}

// execReturning scans the returned rows back into the values and reports how
// many came back.
func (q *InsertQuery[T]) execReturning(ctx context.Context, query string, args []any) (int64, error) {
	rows, err := q.query(ctx, query, args)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return q.scanReturning(rows)
}

// hasReturning reports whether Exec reads rows back, which a batch needs to
// know to read them too.
func (q *InsertQuery[T]) hasReturning() bool { return len(q.returning) > 0 }

// scanReturning scans the returned rows back into the values, in order, and
// reports how many came back. The caller closes rows.
func (q *InsertQuery[T]) scanReturning(rows Rows) (int64, error) {
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	p, err := planFor(q.model.typ, cols)
	if err != nil {
		return 0, err
	}
	if q.conflict != nil {
		return q.scanMatched(rows, p, len(cols))
	}
	dest := make([]any, len(cols))
	hs := p.holders()
	sink := new(any)

	var n int64
	for i := 0; rows.Next(); i++ {
		n++
		if i >= len(q.rows) || !q.rows[i].CanSet() {
			err = rows.Scan(discard(dest, sink)...)
			if err != nil {
				return n, err
			}
			continue
		}
		for j, path := range p.paths {
			if path == nil {
				dest[j] = sink
				continue
			}
			dest[j] = p.at(q.rows[i], j, hs)
		}
		err = rows.Scan(dest...)
		if err != nil {
			return n, err
		}
	}
	err = rows.Err()
	if err == nil && n != int64(len(q.rows)) {
		// A trigger can drop a row, shifting the ones after it onto the wrong
		// values: better to say so than to leave keys where they do not belong.
		err = fmt.Errorf("barm: RETURNING gave %d rows for %d values, so they may have landed on the wrong ones", n, len(q.rows))
	}
	return n, err
}

// renderConflict writes the ON clause and what Set added to it, then RETURNING.
func (q *InsertQuery[T]) renderConflict(b *builder) {
	if c := q.conflict; c != nil {
		b.str(" ON ").frag(c.on)
		if len(c.set) > 0 {
			if endsFold(strings.TrimRight(c.on.sql, " \t\n"), "do update") {
				b.str(" SET ")
			} else {
				b.byte(' ')
			}
			for i, s := range c.set {
				if i > 0 {
					b.str(", ")
				}
				b.frag(s)
			}
		}
	}
	b.returning(q.returning)
}

// endsFold reports whether s ends with the lower-case suffix, in any case.
func endsFold(s, suffix string) bool {
	return len(s) >= len(suffix) && strings.EqualFold(s[len(s)-len(suffix):], suffix)
}

// scanMatched reads every returned row before writing any back, since an insert
// with an ON clause may skip values. barm does not read the clause, so nothing
// says which value a row belongs to: rows go back in order when every value
// came back, and otherwise none do, rather than land in the wrong ones.
func (q *InsertQuery[T]) scanMatched(rows Rows, p *plan, ncols int) (int64, error) {
	dest := make([]any, ncols)
	hs := p.holders()
	sink := new(any)
	var got []reflect.Value
	for rows.Next() {
		v := reflect.New(q.model.typ).Elem()
		for j, path := range p.paths {
			if path == nil {
				dest[j] = sink
				continue
			}
			dest[j] = p.at(v, j, hs)
		}
		err := rows.Scan(dest...)
		if err != nil {
			return int64(len(got)), err
		}
		got = append(got, v)
	}
	n := int64(len(got))
	err := rows.Err()
	if err != nil {
		return n, err
	}
	if len(got) != len(q.rows) {
		return n, fmt.Errorf("barm: RETURNING gave %d rows for %d values, and nothing says which is which — read them with Slice", len(got), len(q.rows))
	}
	for i, v := range got {
		copyReturned(q.rows[i], v, p)
	}
	return n, nil
}

// copyReturned writes the returned columns of v into row.
func copyReturned(row, v reflect.Value, p *plan) {
	for _, path := range p.paths {
		if path != nil {
			reflect.ValueOf(fieldAt(row, path)).Elem().Set(fieldValue(v, path))
		}
	}
}

func discard(dest []any, sink any) []any {
	for i := range dest {
		dest[i] = sink
	}
	return dest
}

// One runs the insert and scans the single returned row into T, the query's own
// model type.
func (q *InsertQuery[T]) One(ctx context.Context) (T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		var zero T
		return zero, err
	}
	return q.one[T](ctx, query, args)
}

// Slice runs the insert and scans every returned row into T, which is how a
// multi-row insert reads its generated keys back.
func (q *InsertQuery[T]) Slice(ctx context.Context) ([]T, error) {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	if err != nil {
		return nil, err
	}
	return q.slice[T](ctx, query, args, q.rowHint())
}

// Seq runs the insert and streams the returned rows as T. A build failure is
// reported through the sequence, like every other error.
func (q *InsertQuery[T]) Seq(ctx context.Context) iter.Seq2[T, error] {
	c := *q
	c.expandReturning()
	query, args, err := c.Build()
	return q.seq[T](ctx, query, args, err)
}

// OneAs runs the insert and scans the single returned row into U.
func (q *InsertQuery[T]) OneAs[U any](ctx context.Context) (U, error) {
	c := q.retype[U]()
	return c.One(ctx)
}

// SliceAs runs the insert and scans every returned row into U.
func (q *InsertQuery[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	c := q.retype[U]()
	return c.Slice(ctx)
}

// SeqAs runs the insert and streams the returned rows as U.
func (q *InsertQuery[T]) SeqAs[U any](ctx context.Context) iter.Seq2[U, error] {
	c := q.retype[U]()
	return c.Seq(ctx)
}

// BuildAs renders the insert as the *As calls would run it, RETURNING clause
// included.
func (q *InsertQuery[T]) BuildAs[U any]() (string, []any, error) {
	c := q.retype[U]()
	c.expandReturning()
	return c.Build()
}

// retype copies the query for a U result. Only what RETURNING scans into
// changes; the rows being written are still T's. The copy is a plain
// conversion: T is in none of the fields, so every instantiation shares one
// underlying struct. It is returned by value so the copy stays on the caller's
// stack.
func (q *InsertQuery[T]) retype[U any]() InsertQuery[U] {
	c := InsertQuery[U](*q)
	ret, err := modelOf[U]()
	c.ret = ret
	if err != nil && c.err == nil {
		c.err = err
	}
	return c
}

// expandReturning settles the RETURNING clause: the result type's columns
// unless Returning named its own. A failure lands on q.err, where Build
// reports it.
func (q *InsertQuery[T]) expandReturning() {
	r, err := q.ret.returningCols(q.Dialect(), q.returning)
	if err != nil {
		if q.err == nil {
			q.err = err
		}
		return
	}
	q.returning = r
}
