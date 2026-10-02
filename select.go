// SelectQuery builds and runs a SELECT against T's table, or any table given
// to Table. Terminals scan into T, or into another type through the As
// variants, and Count and Exists rewrite the query around the same filters.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strconv"
)

// SelectQuery builds a SELECT that scans into T. The terminal calls (Seq,
// Slice, One) take no type argument — T came from Select[T]. The *As forms
// settle the projection on a copy, so a query is never retyped in place.
//
// T supplies the table as well as the columns, and Table overrides it — which
// is how a query reaches a table no model names, or a join.
type SelectQuery[T any] struct {
	model  *model
	proj   *model // fields to project; the model itself unless a result type narrowed it
	table  *frag
	schema string
	withClause
	dist    bool
	unions  []union
	cols    []frag
	colName []string // per-cols column name, empty when the expression is opaque
	joins   []frag
	wheres  []frag
	groups  []frag
	havings []frag
	orders  []frag
	limit   int64
	offset  int64
	lock    string
	rels    []relation
	err     error
	runner
}

func newSelect[T any](r runner) *SelectQuery[T] {
	m, err := modelOf[T]()
	return &SelectQuery[T]{runner: r, model: m, proj: m, err: err}
}

// retype settles the projection for a result type. A query started from a table
// name takes its schema from the result; one started from a model narrows to the
// result's columns, so asking for a three-field struct selects three columns
// rather than the whole row. An explicit Column or ColumnExpr wins over both,
// which is what lets a projection be read into a scalar.
//
// It returns a copy, so the query it was called on is never retyped in place.
// The copy is a plain conversion: T is in none of the fields, so every
// instantiation shares one underlying struct.
func (q *SelectQuery[T]) retype[U any]() SelectQuery[U] {
	c := SelectQuery[U](*q)
	m, err := modelOf[U]()
	if err != nil && c.err == nil {
		c.err = err
	}
	if len(c.model.fields) == 0 {
		c.model = m
	}
	if len(c.cols) == 0 && len(m.fields) > 0 {
		err = c.model.covers(m)
		if err != nil && c.err == nil {
			c.err = err
		}
		// A union's branches have to match its columns, so it keeps them, and U
		// takes what it maps of the row.
		if len(c.unions) == 0 || len(c.proj.fields) == 0 {
			c.proj = m
		}
	}
	return c
}

// fail records the first error; the rest of the builder keeps going so that a
// chain reads the same whether or not it went wrong.
func (q *SelectQuery[T]) fail(err error) {
	if q.err == nil {
		q.err = err
	}
}

// Clone returns a copy to build on separately. A builder changes in place and
// hands itself back from every call, so two queries built on from one partial
// query without it would share, and overwrite, each other's clauses.
func (q *SelectQuery[T]) Clone() *SelectQuery[T] {
	c := *q
	// Capped at their length, so the first append on either side reallocates
	// rather than writing where the other one reads.
	c.cols, c.colName, c.joins = slices.Clip(c.cols), slices.Clip(c.colName), slices.Clip(c.joins)
	c.wheres, c.groups, c.havings = slices.Clip(c.wheres), slices.Clip(c.groups), slices.Clip(c.havings)
	c.orders, c.unions, c.rels, c.ctes = slices.Clip(c.orders), slices.Clip(c.unions), slices.Clip(c.rels), slices.Clip(c.ctes)
	return &c
}

// Via returns a copy of the query that runs on h instead, leaving q bound to
// where it was started. A query built once can then run on a DB, a transaction
// or a held connection alike, from any number of goroutines:
//
//	u, err := byEmail.Via(tx).One(ctx)
//
// The copy shares q's clauses, which is what keeps it off the heap. Running it
// is safe, but building further on it is for a query taken through Clone.
// h should speak the dialect the query was built for: Column has quoted its
// names already.
func (q *SelectQuery[T]) Via(h IDB) *SelectQuery[T] {
	c := *q
	c.h = h
	return &c
}

// Apply threads the query through fns, for composing reusable clauses:
//
//	byTenant := func(id int64) func(*barm.SelectQuery[User]) *barm.SelectQuery[User] {
//		return func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
//			return q.Where("tenant_id = ?", id)
//		}
//	}
//	users, err := db.Select[User]().Apply(byTenant(id), recent).Slice(ctx)
//
// nil values are skipped.
func (q *SelectQuery[T]) Apply(fns ...func(*SelectQuery[T]) *SelectQuery[T]) *SelectQuery[T] {
	for _, fn := range fns {
		if fn != nil {
			q = fn(q)
		}
	}
	return q
}

// Prepare caches this query as a named prepared statement on the DB. The name
// pins one SQL text: the statement is prepared once, on first execution, and
// reused by every later query sharing the name.
func (q *SelectQuery[T]) Prepare(name string) *SelectQuery[T] {
	q.name = name
	return q
}

// Table overrides the table expression, e.g. Table("users AS u").
func (q *SelectQuery[T]) Table(expr string, args ...any) *SelectQuery[T] {
	q.table = &frag{sql: expr, args: args}
	return q
}

// Schema qualifies the model's table, as `schema.table`. A custom table
// expression set with [SelectQuery.Table] overrides it, since that owns its own naming.
// Relations loaded with Relation inherit it unless they set their own.
func (q *SelectQuery[T]) Schema(name string) *SelectQuery[T] {
	q.schema = name
	return q
}

// Column restricts the projection to the named columns.
func (q *SelectQuery[T]) Column(names ...string) *SelectQuery[T] {
	d := q.Dialect()
	for _, n := range names {
		q.cols = append(q.cols, frag{sql: string(d.AppendIdent(nil, n))})
		q.colName = append(q.colName, n)
	}
	return q
}

// ColumnExpr adds a raw projection expression, e.g. ColumnExpr("u.*") or
// ColumnExpr("(?) AS n", sub). Its columns map by the names the result reports.
func (q *SelectQuery[T]) ColumnExpr(expr string, args ...any) *SelectQuery[T] {
	q.cols = append(q.cols, frag{sql: expr, args: args})
	q.colName = append(q.colName, "")
	return q
}

func (q *SelectQuery[T]) Distinct() *SelectQuery[T] { q.dist = true; return q }

func (q *SelectQuery[T]) Where(expr string, args ...any) *SelectQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args})
	return q
}

func (q *SelectQuery[T]) WhereOr(expr string, args ...any) *SelectQuery[T] {
	q.wheres = append(q.wheres, frag{sql: expr, args: args, or: true})
	return q
}

func (q *SelectQuery[T]) Join(expr string, args ...any) *SelectQuery[T] {
	q.joins = append(q.joins, frag{sql: expr, args: args})
	return q
}

func (q *SelectQuery[T]) GroupBy(expr string, args ...any) *SelectQuery[T] {
	q.groups = append(q.groups, frag{sql: expr, args: args})
	return q
}

func (q *SelectQuery[T]) Having(expr string, args ...any) *SelectQuery[T] {
	q.havings = append(q.havings, frag{sql: expr, args: args})
	return q
}

func (q *SelectQuery[T]) OrderBy(expr string, args ...any) *SelectQuery[T] {
	q.orders = append(q.orders, frag{sql: expr, args: args})
	return q
}

// Limit caps the rows returned. A negative n is an error rather than no limit,
// so a page size taken from input cannot ask for the whole table.
func (q *SelectQuery[T]) Limit(n int64) *SelectQuery[T] {
	if n < 0 {
		q.fail(fmt.Errorf("barm: negative Limit %d", n))
	}
	q.limit = n
	return q
}

// Offset skips n rows. A negative n is an error.
func (q *SelectQuery[T]) Offset(n int64) *SelectQuery[T] {
	if n < 0 {
		q.fail(fmt.Errorf("barm: negative Offset %d", n))
	}
	q.offset = n
	return q
}

// For appends a locking clause, e.g. For("UPDATE").
func (q *SelectQuery[T]) For(clause string) *SelectQuery[T] { q.lock = clause; return q }

// With adds a common table expression, e.g. With("recent", sub) for
// `WITH "recent" AS (<sub>)`. The body renders into this query, so the two
// share one argument numbering. Read from it by naming it: Table("recent").
func (q *SelectQuery[T]) With(name string, sub Query) *SelectQuery[T] {
	q.addWith(name, sub, false)
	return q
}

// WithRecursive adds a CTE that refers to itself, and marks the whole WITH
// clause RECURSIVE. The body is the anchor unioned with the recursive term:
//
//	db.Select[node]().Table("tree").WithRecursive("tree",
//		db.Select[node]().Where("parent_id IS NULL").
//			UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id")))
func (q *SelectQuery[T]) WithRecursive(name string, sub Query) *SelectQuery[T] {
	q.addWith(name, sub, true)
	return q
}

// WithExpr adds a CTE written out in full, for what a name and a body cannot
// express on their own — a column list, or a materialization hint:
//
//	q.WithExpr(`recent(id, seen) AS MATERIALIZED (SELECT id, seen FROM hits WHERE seen > ?)`, cutoff)
//
// The expression is SQL barm does not parse, the same as Join and Where.
func (q *SelectQuery[T]) WithExpr(expr string, args ...any) *SelectQuery[T] {
	q.addWithExpr(expr, args)
	return q
}

// Union appends `UNION <sub>`, dropping duplicate rows. The branch renders into
// this query, so both share one argument numbering.
//
// A union is written between this query's conditions and its ORDER BY, which is
// where SQL wants it: a trailing ORDER BY, LIMIT or OFFSET applies to the whole
// union rather than to either branch. A branch that carries its own would land
// there too, so put them on the outer query instead.
func (q *SelectQuery[T]) Union(sub Query) *SelectQuery[T] {
	q.unions = append(q.unions, union{sub: sub})
	return q
}

// UnionAll appends `UNION ALL <sub>`, keeping duplicate rows. It is what the
// recursive term of a WithRecursive body hangs off.
func (q *SelectQuery[T]) UnionAll(sub Query) *SelectQuery[T] {
	q.unions = append(q.unions, union{sub: sub, all: true})
	return q
}

// Build renders the query and its arguments, projecting T.
func (q *SelectQuery[T]) Build() (string, []any, error) { return q.build() }

// BuildAs renders the query as it would run for a U result.
func (q *SelectQuery[T]) BuildAs[U any]() (string, []any, error) {
	c := q.retype[U]()
	return c.build()
}

func (q *SelectQuery[T]) build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

// render writes the SELECT into b.
func (q *SelectQuery[T]) render(b *builder) error {
	if q.err != nil {
		return q.err
	}
	if q.table == nil && q.model.table == "" {
		return errNoTable
	}

	b.hint(q.argCount())
	err := b.with(q.withClause)
	if err != nil {
		return err
	}
	b.str("SELECT ")
	if q.dist {
		b.str("DISTINCT ")
	}
	switch {
	case len(q.cols) > 0:
		for i, c := range q.cols {
			if i > 0 {
				b.str(", ")
			}
			b.frag(c)
		}
	case len(q.proj.fields) > 0:
		fields := q.proj.fields
		// A custom table expression owns its own naming, so columns stay
		// unqualified there.
		if q.table == nil {
			b.ident(q.model.ref()).byte('.')
		}
		b.ident(fields[0].name)
		for _, f := range fields[1:] {
			b.str(", ")
			// A custom table expression owns its own naming, so columns stay
			// unqualified there.
			if q.table == nil {
				b.ident(q.model.ref()).byte('.')
			}
			b.ident(f.name)
		}
	default:
		b.byte('*')
	}

	b.str(" FROM ")
	if q.table != nil {
		b.frag(*q.table)
	} else {
		b.tableRef(q.schema, q.model)
	}

	for _, j := range q.joins {
		b.byte(' ').frag(j)
	}
	b.conds(" WHERE ", q.wheres)
	if len(q.groups) > 0 {
		b.str(" GROUP BY ")
		for i, g := range q.groups {
			if i > 0 {
				b.str(", ")
			}
			b.frag(g)
		}
	}
	b.conds(" HAVING ", q.havings)
	for _, u := range q.unions {
		if u.all {
			b.str(" UNION ALL ")
		} else {
			b.str(" UNION ")
		}
		err = u.sub.render(b)
		if err != nil {
			return err
		}
	}
	if len(q.orders) > 0 {
		b.str(" ORDER BY ")
		for i, o := range q.orders {
			if i > 0 {
				b.str(", ")
			}
			b.frag(o)
		}
	}
	switch {
	case q.limit > 0:
		b.str(" LIMIT ")
		b.b = strconv.AppendInt(b.b, q.limit, 10)
	case q.offset > 0:
		if n := b.d.NoLimit(); n != "" {
			b.str(" LIMIT ").str(n)
		}
	}
	if q.offset > 0 {
		b.str(" OFFSET ")
		b.b = strconv.AppendInt(b.b, q.offset, 10)
	}
	if q.lock != "" {
		b.str(" FOR ").str(q.lock)
	}
	return nil
}

// union is one branch appended to a query with UNION or UNION ALL.
type union struct {
	sub Query
	all bool
}

// argCount sums the bind arguments across every fragment, so the args slice is
// sized once instead of regrowing.
func (q *SelectQuery[T]) argCount() int {
	n := q.withClause.argCount()
	if q.table != nil {
		n += len(q.table.args)
	}
	for _, u := range q.unions {
		n += u.sub.argCount()
	}
	for _, fs := range [][]frag{q.cols, q.joins, q.wheres, q.groups, q.havings, q.orders} {
		for _, f := range fs {
			n += len(f.args)
		}
	}
	return n
}

// Rows runs the query and returns its rows, for the caller to read and close.
func (q *SelectQuery[T]) Rows(ctx context.Context) (Rows, error) {
	query, args, err := q.build()
	if err != nil {
		return nil, err
	}
	return q.query(ctx, query, args)
}

// Seq streams rows as an iterator. The error is reported through the second
// value and terminates the sequence; the underlying rows are always closed.
func (q *SelectQuery[T]) Seq(ctx context.Context) iter.Seq2[T, error] {
	query, args, err := q.build()
	if err == nil && len(q.rels) > 0 {
		// Loading a relation means knowing every key first, which is the one
		// thing a stream does not have. Saying so beats a query per row.
		err = errors.New("barm: Relation cannot stream — relations need the whole result, so use Slice or One")
	}
	return q.seq[T](ctx, query, args, err)
}

// SeqAs streams the rows as U instead of the query's own type.
func (q *SelectQuery[T]) SeqAs[U any](ctx context.Context) iter.Seq2[U, error] {
	c := q.retype[U]()
	return c.Seq(ctx)
}

// Slice collects every row into a slice. It collects them directly rather than
// ranging over Seq: the sequence costs a yield call per row, around 14% over a
// thousand of them, and buys nothing here.
func (q *SelectQuery[T]) Slice(ctx context.Context) ([]T, error) {
	query, args, err := q.build()
	if err != nil {
		return nil, err
	}
	// A LIMIT is the only row-count hint a select has.
	out, err := q.slice[T](ctx, query, args, q.limit)
	if err != nil || len(q.rels) == 0 || len(out) == 0 {
		return out, err
	}
	return out, q.loadRelations(ctx, out)
}

// SliceAs collects the rows as U instead of the query's own type.
func (q *SelectQuery[T]) SliceAs[U any](ctx context.Context) ([]U, error) {
	c := q.retype[U]()
	return c.Slice(ctx)
}

// ErrNoRows is returned by One when the query yields nothing.
var ErrNoRows = sql.ErrNoRows

// One returns the first row, or ErrNoRows. It runs with LIMIT 1, whatever Limit
// said.
func (q *SelectQuery[T]) One(ctx context.Context) (T, error) {
	c := *q
	c.limit = 1
	query, args, err := c.build()
	if err != nil {
		var zero T
		return zero, err
	}
	v, err := c.one[T](ctx, query, args)
	if err != nil || len(c.rels) == 0 {
		return v, err
	}
	rows := []T{v}
	err = c.loadRelations(ctx, rows)
	if err != nil {
		return v, err
	}
	return rows[0], nil
}

// OneAs returns the single row as U instead of the query's own type.
func (q *SelectQuery[T]) OneAs[U any](ctx context.Context) (U, error) {
	c := q.retype[U]()
	return c.One(ctx)
}

// scanRow scans the first row of rows into v, a pointer, mapping the columns by
// the names the result reports. No row is ErrNoRows.
func scanRow(rows Rows, v any) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	dest, err := rowDest(rows, v, cols)
	if err != nil {
		return err
	}
	if !rows.Next() {
		err = rows.Err()
		if err == nil {
			err = ErrNoRows
		}
		return err
	}
	return rows.Scan(dest...)
}

// rowDest points one row's Scan destinations at v's fields, by column name.
func rowDest(rows Rows, v any, names []string) ([]any, error) {
	rv := reflect.ValueOf(v).Elem()
	p, err := planFor(rv.Type(), names)
	if err != nil {
		return nil, err
	}
	if p.direct {
		if len(names) != 1 {
			return nil, fmt.Errorf("barm: cannot scan %d columns into %s", len(names), rv.Type())
		}
		return []any{v}, nil
	}
	dest := make([]any, len(names))
	hs := p.holders(rows)
	var sink *any // only when some column has no field, which is rare
	for i, path := range p.paths {
		if path == nil {
			if sink == nil {
				sink = new(any)
			}
			dest[i] = sink
			continue
		}
		dest[i] = p.at(rv, i, hs)
	}
	return dest, nil
}

// The fixed projections of CountQuery and ExistsQuery, shared rather than built
// per call: the copy that takes them only reads them.
var (
	countCols  = []frag{{sql: "count(*)"}}
	existsCols = []frag{{sql: "1"}}
	opaqueName = []string{""}
)

// CountQuery renders the COUNT(*) form of this query. A query whose own shape
// decides what a row is — groups, distinct rows, union branches, locked rows —
// is counted from the outside, as SELECT count(*) FROM (...), since putting
// count(*) in its place would count something else or not be SQL at all.
func (q *SelectQuery[T]) CountQuery() (string, []any, error) {
	c := *q
	c.orders, c.limit, c.offset = nil, 0, 0
	c.name = "" // different SQL text than the query this was derived from
	if !c.dist && len(c.groups) == 0 && len(c.unions) == 0 && c.lock == "" {
		c.cols, c.colName = countCols, opaqueName
		return c.build()
	}
	if !c.dist && len(c.unions) == 0 {
		c.cols, c.colName = existsCols, opaqueName // a group or a locked row counts whatever it selects
	}
	b := c.builder().str("SELECT count(*) FROM (")
	err := c.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.str(") AS ").ident("barm_count").done()
}

// Count runs the query as a COUNT(*).
func (q *SelectQuery[T]) Count(ctx context.Context) (int64, error) {
	query, args, err := q.CountQuery()
	if err != nil {
		return 0, err
	}
	c := *q
	c.name = "" // different SQL text than the query this was derived from
	return c.one[int64](ctx, query, args)
}

// Exists reports whether the query matches any row, as
// SELECT EXISTS (SELECT 1 ... LIMIT 1) — the engine stops at the first match
// instead of counting every one.
func (q *SelectQuery[T]) Exists(ctx context.Context) (bool, error) {
	query, args, err := q.ExistsQuery()
	if err != nil {
		return false, err
	}
	c := *q
	c.name = "" // different SQL text than the query this was derived from
	return c.one[bool](ctx, query, args)
}

// ExistsQuery renders the EXISTS form of this query.
func (q *SelectQuery[T]) ExistsQuery() (string, []any, error) {
	c := *q
	if len(c.unions) == 0 { // a union's branches have to match its columns, so it keeps them
		c.cols, c.colName = existsCols, opaqueName
	}
	c.orders, c.limit, c.offset = nil, 1, 0

	// The wrapper binds no arguments of its own, so the inner numbering holds.
	b := c.builder().str("SELECT EXISTS (")
	err := c.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.byte(')').done()
}
