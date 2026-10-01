// SQL assembly shared by every builder. A builder renders into one pooled
// buffer and one argument list, which is what lets a CTE or a union branch
// nest inside another query and keep a single placeholder numbering. It also
// resolves ?N markers, dedups repeated arguments, and wraps
// WHERE conditions in parentheses where precedence needs it.

package barm

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"unsafe"
)

type frag struct {
	sql  string
	args []any
	or   bool // for WHERE/HAVING groups
	pk   bool // where WherePK sits among the conditions
}

// builder accumulates SQL text and bind arguments.
type builder struct {
	d    Dialect
	b    []byte
	args []any

	pos   []int           // placeholder assigned to each arg of the fragment being written
	bound []boundArg      // dedupable args already bound
	index map[dataKey]int // bound, once it outgrows a linear scan

	// err is what went wrong writing a fragment, which has no way to return
	// one; done reports it.
	err   error
	dedup bool
}

// boundArg is a dedupable argument already bound, with its 1-based placeholder.
type boundArg struct {
	k dataKey
	n int
}

// dataKey identifies an argument by its backing array rather than its contents,
// so no comparison walks the payload and nothing can panic on an uncomparable
// argument.
type dataKey struct {
	p     unsafe.Pointer
	n     int
	bytes bool // a string and a []byte over the same memory are different values
}

// maxLinearBound is how many dedupable arguments are scanned before they are
// indexed. Below it a scan beats hashing; above it a wide INSERT goes quadratic.
const maxLinearBound = 32

const (
	initialBuf   = 256
	maxRetainBuf = 64 << 10 // buffers larger than 64 kib are not kept around
)

var builderPool = sync.Pool{New: func() any {
	return &builder{b: make([]byte, 0, initialBuf)}
}}

// builder takes a pooled builder configured for this query. Argument dedup is
// skipped for prepared queries: their SQL text is pinned to a name, so it must
// depend on the query's shape alone.
func (r *runner) builder() *builder {
	b := builderPool.Get().(*builder) //nolint:forcetypeassert // the pool's New only ever stores *builder
	d := r.Dialect()
	b.d, b.b, b.args = d, b.b[:0], nil
	b.dedup, b.bound, b.index, b.err = r.h.sess().dedup && r.name == "" && d.NumberedArgs(), b.bound[:0], nil, nil
	return b
}

// done renders the query, or reports what a fragment could not, and returns
// the builder to the pool. The byte buffer is kept for the next query; the args
// slice is not, because it is handed to the caller.
//
// Pooling the args buffer too was measured and is a loss: the caller owns the
// result, so an exact-size copy has to be allocated anyway, and preserving the
// buffer only adds that copy and a clear on top of the same allocation count.
// hint() already sizes the slice in one shot where the width is known.
func (b *builder) done() (string, []any, error) {
	err := b.err
	if err != nil {
		b.release()
		return "", nil, err
	}
	query, args := string(b.b), b.args
	b.release()
	return query, args, nil
}

// release returns the builder to the pool without rendering. Safe to call on an
// error path.
func (b *builder) release() {
	if cap(b.b) > maxRetainBuf {
		b.b = make([]byte, 0, initialBuf) // one outsized query shouldn't pin memory
	}
	clear(b.bound) // it points into the caller's arguments
	b.d, b.args, b.index, b.err, b.dedup = nil, nil, nil, nil, false
	builderPool.Put(b)
}

// hint reserves room for n bind arguments, so a wide INSERT does not regrow.
func (b *builder) hint(n int) *builder { //nolint:unparam // kept for chaining, like every other builder method
	if cap(b.args)-len(b.args) < n {
		// A query nested in an argument renders after the enclosing one has bound
		// some already, and those stay.
		b.args = slices.Grow(b.args, n)
	}
	return b
}

func (b *builder) str(s string) *builder {
	b.b = append(b.b, s...)
	return b
}

func (b *builder) byte(c byte) *builder {
	b.b = append(b.b, c)
	return b
}

func (b *builder) ident(name string) *builder {
	b.b = b.d.AppendIdent(b.b, name)
	return b
}

func (b *builder) placeholder(v any) *builder { //nolint:unparam // kept for chaining, like every other builder method
	if b.dedup {
		return b.placeholderDedup(v)
	}
	b.args = append(b.args, v)
	b.b = b.d.AppendPlaceholder(b.b, len(b.args))
	return b
}

// placeholderDedup is placeholder for a query with argument dedup on, kept apart
// so the common path stays as short as it was.
func (b *builder) placeholderDedup(v any) *builder {
	k, ok := keyOf(v)
	if ok {
		if n := b.findBound(k); n != 0 {
			return b.placeholderAt(n)
		}
	}
	b.args = append(b.args, v)
	n := len(b.args)
	if ok {
		b.remember(k, n)
	}
	b.b = b.d.AppendPlaceholder(b.b, n)
	return b
}

// placeholderAt writes a marker for an argument that is already bound.
func (b *builder) placeholderAt(n int) *builder {
	b.b = b.d.AppendPlaceholder(b.b, n)
	return b
}

// findBound returns the placeholder already bound to k, or 0.
func (b *builder) findBound(k dataKey) int {
	if b.index != nil {
		return b.index[k]
	}
	for _, a := range b.bound {
		if a.k == k {
			return a.n
		}
	}
	return 0
}

// remember records a newly bound argument, indexing them all once a scan would
// stop paying off.
func (b *builder) remember(k dataKey, n int) {
	if b.index != nil {
		b.index[k] = n
		return
	}
	b.bound = append(b.bound, boundArg{k, n})
	if len(b.bound) > maxLinearBound {
		// sized for every argument the hint still expects, most of which a wide
		// INSERT's string columns will be
		b.index = make(map[dataKey]int, max(cap(b.args), 4*len(b.bound)))
		for _, a := range b.bound {
			b.index[a.k] = a.n
		}
	}
}

// keyOf returns the identity of a value whose identity barm can establish.
//
// Empty values are excluded: a nil []byte is NULL to the driver while an empty
// one is an empty blob, and their data pointers are not reliably distinct — so
// merging them would change meaning.
func keyOf(v any) (dataKey, bool) {
	switch t := v.(type) {
	case string:
		return dataKey{p: unsafe.Pointer(unsafe.StringData(t)), n: len(t)}, len(t) > 0
	case []byte:
		return dataKey{p: unsafe.Pointer(unsafe.SliceData(t)), n: len(t), bytes: true}, len(t) > 0
	}
	return dataKey{}, false
}

// frag writes a raw fragment. `?` takes the next argument in order, `?N` takes
// the N-th (1-based) and reuses its placeholder when the dialect numbers them,
// and `??` emits a literal question mark. A marker with no argument behind it is
// written through unchanged.
//
// The two forms count separately: `?N` does not advance the bare `?` cursor, so
// mixing them in one fragment is legal but rarely what you want.
//
// Because `?N` is written by hand, the rendered SQL depends only on the
// fragment, never on the values — which is what keeps prepared statements and
// server-side plan caches stable.
func (b *builder) frag(f frag) *builder {
	// A query bound as an argument renders its own fragments partway through this
	// one, so each fragment's positions are a window of pos above the enclosing
	// fragment's, and indexed from base rather than held as a slice that the
	// nested one's growing would leave stale.
	base := len(b.pos)
	if n := len(f.args); n > 0 { // only fragments that bind need the bookkeeping
		if cap(b.pos)-base < n {
			b.pos = append(b.pos, make([]int, n)...)
		} else {
			b.pos = b.pos[:base+n]
			clear(b.pos[base:])
		}
	}
	next := 0

	for i := 0; i < len(f.sql); i++ {
		c := f.sql[i]
		if c != '?' {
			b.b = append(b.b, c)
			continue
		}
		if i+1 < len(f.sql) && f.sql[i+1] == '?' {
			b.b = append(b.b, '?')
			i++
			continue
		}

		arg := next
		if n, width := parseIndex(f.sql[i+1:]); width > 0 {
			arg, i = n-1, i+width
		} else {
			next++
		}
		if arg < 0 || arg >= len(f.args) {
			b.b = append(b.b, '?')
			continue
		}
		if b.d.NumberedArgs() && b.pos[base+arg] != 0 {
			b.placeholderAt(b.pos[base+arg])
			continue
		}
		v := f.args[arg]
		if l, ok := v.(inList); ok {
			b.list(l.v) // spelled out again at every reference: one placeholder is not a list
			continue
		}
		if q, ok := v.(Query); ok {
			// Rendered in place, so it shares this query's numbering, and again at
			// every reference, since it is SQL rather than a value to bind.
			b.sub(q)
			continue
		}
		before := len(b.args)
		b.placeholder(v)
		if len(b.args) == before+1 {
			b.pos[base+arg] = len(b.args)
		}
	}
	b.pos = b.pos[:base]
	// Trailing args with no marker are ignored, keeping the numbering honest.
	return b
}

// In spells a slice out as a list of placeholders, one per element, for an
// IN clause. The parentheses are the SQL's to write:
//
//	Where("id IN (?)", barm.In(ids))  // id IN ($1, $2, $3)
//
// Without it a slice is one argument, which the driver sends as an array, and
// `= ANY(?)` is how Postgres matches against one. An empty slice is a build
// error, since `IN ()` is not SQL.
func In(slice any) any { return inList{slice} }

type inList struct{ v any }

func (b *builder) list(v any) {
	if vs, ok := v.([]any); ok {
		if len(vs) == 0 {
			b.fail(errEmptyList)
		}
		for i, e := range vs {
			if i > 0 {
				b.str(", ")
			}
			b.placeholder(e)
		}
		return
	}
	rv := reflect.ValueOf(v)
	if k := rv.Kind(); k != reflect.Slice && k != reflect.Array {
		b.fail(fmt.Errorf("barm: In takes a slice, got %T", v))
		return
	}
	if rv.Len() == 0 {
		b.fail(errEmptyList)
	}
	for i := range rv.Len() {
		if i > 0 {
			b.str(", ")
		}
		b.placeholder(rv.Index(i).Interface())
	}
}

func (b *builder) sub(q Query) {
	err := q.render(b)
	if err != nil {
		b.fail(err)
	}
}

// value is what field f's value v binds as: v itself, NULL for a nullzero
// field at its zero, or v encoded as JSON for a json field. A nil pointer, map
// or slice there is NULL too, rather than the JSON literal null.
func (b *builder) value(f *field, v reflect.Value) any {
	if f.nullzero && v.IsZero() {
		return nil
	}
	if !f.json {
		return v.Interface()
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
		if v.IsNil() {
			return nil
		}
	}
	data, err := json.Marshal(v.Interface())
	if err != nil {
		b.fail(fmt.Errorf("barm: column %q: %w", f.name, err))
		return nil
	}
	return unsafe.String(unsafe.SliceData(data), len(data)) // the encoding is ours alone, so no copy
}

// fail records the first error a fragment runs into, for done to report.
func (b *builder) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// parseIndex reads the decimal index of a `?N` marker.
func parseIndex(s string) (n, width int) {
	for width < len(s) && s[width] >= '0' && s[width] <= '9' {
		n = n*10 + int(s[width]-'0')
		width++
	}
	return n, width
}

func (b *builder) conds(kw string, conds []frag) *builder { //nolint:unparam // kept for chaining, like every other builder method
	if len(conds) == 0 {
		return b
	}
	b.str(kw)
	for i, c := range conds {
		b.cond(i, c)
	}
	return b
}

// cond writes one condition, joined to the one before it.
func (b *builder) cond(i int, c frag) {
	if i > 0 {
		if c.or {
			b.str(" OR ")
		} else {
			b.str(" AND ")
		}
	}
	if needsParens(c.sql) {
		b.byte('(').frag(c).byte(')')
	} else {
		b.frag(c)
	}
}

// where writes the WHERE clause of an UPDATE or DELETE, which must have one:
// the conditions as given, with v's primary key where WherePK put it. The key
// is written straight into the builder rather than as fragments, which would
// need their text and argument lists allocated per key.
func (b *builder) where(conds []frag, m *model, v reflect.Value) error {
	if len(conds) == 0 {
		return errors.New("barm: no WHERE clause — say which rows, with Where or WherePK")
	}
	b.str(" WHERE ")
	for i, c := range conds {
		if !c.pk {
			b.cond(i, c)
			continue
		}
		if !v.IsValid() {
			return errors.New("barm: WherePK needs the row whose key it matches — pass it with Value")
		}
		if len(m.pks) == 0 {
			return fmt.Errorf("barm: WherePK needs a primary key, and %s has none", m.typ)
		}
		if i > 0 {
			if c.or {
				b.str(" OR ")
			} else {
				b.str(" AND ")
			}
		}
		// A composite key is several conditions, kept together beside others.
		wrap := len(m.pks) > 1 && len(conds) > 1
		if wrap {
			b.byte('(')
		}
		// No snapshot here: a primary key is usually one column, and copying the
		// whole row to bind one value costs more than boxing it. Update passes a
		// value it has already snapshotted, where boxing is free anyway.
		for j, pk := range m.pks {
			if j > 0 {
				b.str(" AND ")
			}
			b.assign(pk, v)
		}
		if wrap {
			b.byte(')')
		}
	}
	return nil
}

// needsParens wraps composite conditions so AND/OR precedence stays intact. The
// keywords are matched in place rather than on an upper-cased copy, which cost
// an allocation per condition.
func needsParens(sql string) bool {
	for i := 1; i < len(sql); i++ {
		if bounds(sql[i-1]) && (keywordAt(sql[i:], "or") || keywordAt(sql[i:], "and")) {
			return true
		}
	}
	return false
}

// keywordAt reports whether s starts with the lower-case keyword kw, in any
// case, as a whole word.
func keywordAt(s, kw string) bool {
	if len(s) <= len(kw) || !bounds(s[len(kw)]) {
		return false
	}
	for i := range len(kw) {
		if s[i]|0x20 != kw[i] {
			return false
		}
	}
	return true
}

// bounds reports whether c can end a word next to a keyword: SQL takes any
// whitespace there, and a parenthesis or a quote needs none.
func bounds(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v', '(', ')', '\'', '"', '`':
		return true
	}
	return false
}

// returning writes a RETURNING clause, if there is one.
func (b *builder) returning(frags []frag) *builder { //nolint:unparam // kept for chaining, like every other builder method
	if len(frags) == 0 {
		return b
	}
	b.str(" RETURNING ")
	for i, f := range frags {
		if i > 0 {
			b.str(", ")
		}
		b.frag(f)
	}
	return b
}

// cte is one item of a WITH clause: a named body rendered from a builder, or a
// fragment written by hand when the item needs something the name alone cannot
// carry — a column list, or a materialization hint.
type cte struct {
	name string
	sub  Query
	expr frag
}

// withClause is the WITH clause, shared by all four builders. RECURSIVE is a
// property of the clause rather than of any one item, so one recursive CTE
// marks the whole of it and the others may still be ordinary.
type withClause struct {
	ctes      []cte
	recursive bool
}

func (w *withClause) addWith(name string, sub Query, recursive bool) {
	w.ctes = append(w.ctes, cte{name: name, sub: sub})
	w.recursive = w.recursive || recursive
}

func (w *withClause) addWithExpr(expr string, args []any) {
	w.ctes = append(w.ctes, cte{expr: frag{sql: expr, args: args}})
}

// argCount sums the arguments the clause will bind, for the parent's hint.
func (w *withClause) argCount() int {
	n := 0
	for _, c := range w.ctes {
		if c.sub != nil {
			n += c.sub.argCount()
		} else {
			n += len(c.expr.args)
		}
	}
	return n
}

// with writes the WITH clause ahead of the statement it belongs to. Its bodies
// render into this same builder, so their arguments take the first placeholder
// numbers — which is the order they appear in the text.
func (b *builder) with(w withClause) error {
	if len(w.ctes) == 0 {
		return nil
	}
	b.str("WITH ")
	if w.recursive {
		b.str("RECURSIVE ")
	}
	for i, c := range w.ctes {
		if i > 0 {
			b.str(", ")
		}
		if c.sub == nil {
			b.frag(c.expr)
			continue
		}
		b.ident(c.name).str(" AS (")
		err := c.sub.render(b)
		if err != nil {
			return err
		}
		b.byte(')')
	}
	b.byte(' ')
	return nil
}

// errEmptyList is what an empty In reports: `IN ()` is a syntax error on
// Postgres and MySQL, and `IN (NULL)` would match nothing under NOT too, so what
// an empty filter means is the caller's to decide before the query.
var errEmptyList = errors.New("barm: empty slice passed to In — decide what an empty filter means before querying")

// errNoTable is what a query with nothing to name in its FROM or INTO reports.
var errNoTable = errors.New("barm: no table — embed barm.BaseModel with a table tag, or name one with Table")

// table writes a table name, under its schema when there is one.
func (b *builder) table(schema, name string) *builder {
	if schema != "" {
		b.ident(schema).byte('.')
	}
	return b.ident(name)
}

// tableRef writes `table AS alias` (or just the table when unaliased).
func (b *builder) tableRef(schema string, m *model) *builder {
	b.table(schema, m.table)
	if m.alias != "" && m.alias != m.table {
		b.str(" AS ").ident(m.alias)
	}
	return b
}

func (m *model) ref() string {
	if m.alias != "" {
		return m.alias
	}
	return m.table
}
