// Row scanning. A plan maps result columns to struct fields once per type and
// column list and is cached, so scanning a row is just filling destinations.
// Scalars, types with their own Scan and pointer rows are handled too.

package barm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// Rows is the subset of *sql.Rows that barm scans from. A driver outside
// database/sql — pgx, say — satisfies it with a thin adapter, which is what lets
// the scanning here be reused off the database/sql path.
type Rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

var (
	scannerType = reflect.TypeFor[sql.Scanner]()
	timeType    = reflect.TypeFor[time.Time]()
)

// isScannable reports whether a type is scanned as a single value rather than
// destructured into columns.
func isScannable(t reflect.Type) bool {
	if t.Implements(scannerType) || reflect.PointerTo(t).Implements(scannerType) {
		// A Scan of its own is how the type asked to be read, tags or not. One
		// it only inherited from an embedded field must not turn a model that
		// maps columns of its own into a single value.
		if s := deref(t); !embedsScanner(s) || !mapsColumns(s) {
			return true
		}
	}
	t = deref(t)
	return t == timeType || t.Kind() != reflect.Struct
}

// embedsScanner reports whether a struct embeds a Scanner, which is where a
// Scan it did not declare would come from. reflect cannot tell a promoted
// method from a declared one, so this is what stands in for it.
func embedsScanner(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for sf := range t.Fields() {
		if sf.Anonymous && (sf.Type.Implements(scannerType) || reflect.PointerTo(sf.Type).Implements(scannerType)) {
			return true
		}
	}
	return false
}

// mapsColumns reports whether a struct maps columns of its own.
func mapsColumns(t reflect.Type) bool {
	for sf := range t.Fields() {
		if sf.Type == baseModelType {
			return true
		}
		if tag := sf.Tag.Get("barm"); tag != "" && tag != "-" {
			return true
		}
	}
	return false
}

// planKey identifies a plan by its type and the columns it maps. The columns are
// hashed rather than joined, so a lookup allocates nothing; the entry keeps them
// so a hash collision cannot hand back the wrong plan.
type planKey struct {
	t    reflect.Type
	cols uint64
}

type planEntry struct {
	cols []string
	p    *plan
}

var planSeed = maphash.MakeSeed()

// hashCols mixes the column names in order — order decides which field each
// column lands in, so it has to change the key.
func hashCols(cols []string) uint64 {
	h := uint64(1469598103934665603)
	for _, c := range cols {
		h = (h ^ maphash.String(planSeed, c)) * 1099511628211
	}
	return h
}

// plan maps result columns onto struct field index paths. A nil path discards
// the column.
type plan struct {
	paths  [][]int
	direct bool // scan the single column straight into the value
	// viaPointer marks a plan whose paths step through a pointer field. Those
	// need a fresh value per row — a reused one would have every row's copy
	// sharing the pointee, so they would all show the last row's data.
	viaPointer bool
	// held marks the columns read through a holder, nullzero, json or array,
	// nil when there are none.
	held   []bool
	fields []*field // the held columns' fields
}

// at is where column i of the row rv scans to: the field itself, or the field
// behind hs[i] when the column needs a holder. hs comes from holders.
func (p *plan) at(rv reflect.Value, i int, hs []holder) any {
	d := fieldAt(rv, p.paths[i])
	if hs != nil && p.held[i] && !hs[i].direct {
		hs[i].v = reflect.ValueOf(d).Elem()
		return &hs[i]
	}
	return d
}

// holders is what at needs, one set per destination slice; nil, and free, when
// no column of the plan needs one from rows.
//
// Rows that decode arrays themselves take an array column straight, NULL being
// a nil slice there too, and rows that decode JSON a json one, unless it is
// nullzero.
func (p *plan) holders(rows Rows) []holder {
	if p.held == nil {
		return nil
	}
	nj, ok := rows.(NativeJSON)
	nativeJSON := ok && nj.NativeJSON()
	na, ok := rows.(NativeArrays)
	nativeArrays := ok && na.NativeArrays()
	var hs []holder
	for i, f := range p.fields {
		if f == nil {
			continue
		}
		direct := f.array && nativeArrays || f.json && nativeJSON && !f.nullzero
		if direct && hs == nil {
			continue
		}
		if hs == nil {
			hs = make([]holder, len(p.paths))
			for j := range i {
				hs[j].direct = true
			}
		}
		hs[i].json, hs[i].array, hs[i].direct = f.json, f.array, direct
	}
	return hs
}

// holder reads a column the field cannot take as it comes. NULL is the field's
// zero value either way. A json column is decoded into the field, an array one
// parsed from its literal; anything else converts as database/sql would convert
// it into the field.
type holder struct {
	v      reflect.Value
	json   bool
	array  bool
	direct bool // the column scans straight into the field after all
}

func (n *holder) Scan(src any) error {
	if src == nil {
		n.v.SetZero()
		return nil
	}
	if n.array {
		var s string
		switch src := src.(type) {
		case string:
			s = src
		case []byte:
			s = string(src) // elements keep pieces of it, and the driver reuses src
		default:
			return fmt.Errorf("barm: cannot read %T as an array into %s", src, n.v.Type())
		}
		return decodeArray(s, n.v)
	}
	if n.json {
		var data []byte
		switch src := src.(type) {
		case []byte:
			data = src
		case string:
			data = unsafe.Slice(unsafe.StringData(src), len(src)) // only read, never kept
		default:
			return fmt.Errorf("barm: cannot decode %T as json into %s", src, n.v.Type())
		}
		n.v.SetZero() // decoding merges into what is there, and a row reused or written back holds something
		return json.Unmarshal(data, n.v.Addr().Interface())
	}
	if s, ok := reflect.TypeAssert[sql.Scanner](n.v.Addr()); ok {
		return s.Scan(src)
	}
	// database/sql's own conversions are reachable only through its Null
	// types, so each kind goes through the one that holds it.
	switch t := n.v.Type(); {
	case t == timeType:
		var x sql.NullTime
		err := x.Scan(src)
		n.v.Set(reflect.ValueOf(x.Time))
		return err
	case t.Kind() == reflect.String:
		var x sql.NullString
		err := x.Scan(src)
		n.v.SetString(x.String)
		return err
	case t.Kind() == reflect.Bool:
		var x sql.NullBool
		err := x.Scan(src)
		n.v.SetBool(x.Bool)
		return err
	case n.v.CanInt():
		var x sql.Null[int64]
		err := x.Scan(src)
		if err == nil && n.v.OverflowInt(x.V) {
			return fmt.Errorf("barm: %v overflows %s", x.V, t)
		}
		n.v.SetInt(x.V)
		return err
	case n.v.CanUint():
		var x sql.Null[uint64]
		err := x.Scan(src)
		if err == nil && n.v.OverflowUint(x.V) {
			return fmt.Errorf("barm: %v overflows %s", x.V, t)
		}
		n.v.SetUint(x.V)
		return err
	case n.v.CanFloat():
		var x sql.NullFloat64
		err := x.Scan(src)
		n.v.SetFloat(x.Float64)
		return err
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		var x sql.Null[[]byte]
		err := x.Scan(src)
		n.v.SetBytes(x.V)
		return err
	}
	// The rule database/sql ends with, which is what reaches arrays: a driver
	// value of the same kind that converts, as a [16]byte does into a named one.
	if sv := reflect.ValueOf(src); sv.Kind() == n.v.Kind() && sv.Type().ConvertibleTo(n.v.Type()) {
		n.v.Set(sv.Convert(n.v.Type()))
		return nil
	}
	return fmt.Errorf("barm: cannot scan %T into %s", src, n.v.Type())
}

var planCache sync.Map // planKey -> *planEntry

func planFor(t reflect.Type, cols []string) (*plan, error) {
	key := planKey{t, hashCols(cols)}
	if v, ok := planCache.Load(key); ok {
		if e := v.(*planEntry); slices.Equal(e.cols, cols) { //nolint:forcetypeassert // planCache only ever stores *planEntry
			return e.p, nil
		}
		return buildPlan(t, cols) // a collision: correct, just uncached
	}
	p, err := buildPlan(t, cols)
	if err != nil {
		return nil, err
	}
	v, _ := planCache.LoadOrStore(key, &planEntry{cols: slices.Clone(cols), p: p})
	return v.(*planEntry).p, nil //nolint:forcetypeassert // planCache only ever stores *planEntry
}

func buildPlan(t reflect.Type, cols []string) (*plan, error) {
	if isScannable(t) {
		return &plan{direct: true}, nil
	}
	m, err := modelOfType(t)
	if err != nil {
		return nil, err
	}
	p := &plan{paths: make([][]int, len(cols))}
	for i, c := range cols {
		f, ok := m.field(c)
		if !ok {
			// tolerate qualified names coming back from joins: "u.id" -> "id"
			if _, short, cut := strings.Cut(c, "."); cut {
				f, ok = m.field(short)
			}
		}
		if !ok {
			continue
		}
		p.paths[i] = f.index
		if f.nullzero || f.json || f.array {
			if p.held == nil {
				p.held, p.fields = make([]bool, len(cols)), make([]*field, len(cols))
			}
			p.held[i], p.fields[i] = true, f
		}
	}
	if t.Kind() == reflect.Pointer {
		p.viaPointer = true // a pointer row: each one needs a value of its own to point at
		return p, nil
	}
	for _, path := range p.paths {
		if stepsThroughPointer(t, path) {
			p.viaPointer = true
			break
		}
	}
	return p, nil
}

// stepsThroughPointer reports whether reaching a field means dereferencing one.
func stepsThroughPointer(t reflect.Type, index []int) bool {
	for _, i := range index[:max(len(index)-1, 0)] {
		t = t.Field(i).Type
		if t.Kind() == reflect.Pointer {
			return true
		}
	}
	return false
}

// fieldAt walks an index path, allocating nil pointers along the way, and
// returns an addressable destination for Rows.Scan.
func fieldAt(v reflect.Value, index []int) any {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	for _, i := range index[:len(index)-1] {
		v = v.Field(i)
		for v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
	}
	return v.Field(index[len(index)-1]).Addr().Interface()
}

// scanner reuses its destination slice across rows, and where it can, the row
// value itself: taking the address of a fresh T on every row is what makes the
// scan allocate per row. Callers get a copy, so reuse is invisible to them.
type scanner[T any] struct {
	p    *plan
	dest []any
	sink any
	hs   []holder
	row  T // the reused scan target, unused when the plan steps through pointers
}

func newScanner[T any](rows Rows) (*scanner[T], error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	t := reflect.TypeFor[T]()
	p, err := planFor(t, cols)
	if err != nil {
		return nil, err
	}
	if p.direct && len(cols) != 1 {
		var t T
		return nil, fmt.Errorf("barm: cannot scan %d columns into %T", len(cols), t)
	}
	s := &scanner[T]{p: p, dest: make([]any, len(cols)), hs: p.holders(rows)}
	if !p.direct && !p.viaPointer {
		s.bind(reflect.ValueOf(&s.row).Elem()) // one address for every row
	}
	return s, nil
}

// bind points the destination slice at rv's fields.
func (s *scanner[T]) bind(rv reflect.Value) {
	for i, path := range s.p.paths {
		if path == nil {
			s.dest[i] = &s.sink
			continue
		}
		s.dest[i] = s.p.at(rv, i, s.hs)
	}
}

func (s *scanner[T]) scan(rows Rows) (T, error) {
	if s.p.direct {
		var v T
		err := rows.Scan(&v)
		return v, err
	}
	if s.p.viaPointer {
		// a fresh value per row, so each row owns what its pointers reach
		var v T
		s.bind(reflect.ValueOf(&v).Elem())
		err := rows.Scan(s.dest...)
		return v, err
	}
	// Scan overwrites every mapped column, but a Scanner handed a NULL may
	// leave its destination alone, so the row starts zeroed. This compiles to
	// stores, not an allocation.
	var zero T
	s.row = zero
	err := rows.Scan(s.dest...)
	return s.row, err
}

// maxPreallocBytes bounds a preallocation by memory rather than by element
// count. A LIMIT is a maximum, not a count, so what a wrong guess costs is bytes
// — and an element count says nothing about that: a thousand int64s is 8 KiB, a
// thousand wide rows is half a megabyte.
const maxPreallocBytes = 64 << 10

// prealloc turns a row-count hint into a capacity that cannot waste more than
// maxPreallocBytes of backing array. Only the element's own size counts, which
// is what the array holds; strings and slices point elsewhere.
func prealloc[T any](hint int64) int {
	if hint <= 0 {
		return 0
	}
	var zero T
	size := int64(unsafe.Sizeof(zero)) //nolint:gosec // a single struct's size never approaches uintptr's range
	if size == 0 {
		return int(hint) // zero-size elements share one address and cost nothing
	}
	return int(min(hint, maxPreallocBytes/size))
}

// scanSlice reads every remaining row into a slice, preallocating when the
// caller knows a bound. The caller closes rows. Nothing in database/sql or pgx
// reports a row count before iteration — the count arrives after the last row,
// so a LIMIT is the only hint available.
func scanSlice[T any](rows Rows, hint int64) ([]T, error) {
	s, err := newScanner[T](rows)
	if err != nil {
		return nil, err
	}
	var out []T
	if n := prealloc[T](hint); n > 0 {
		out = make([]T, 0, n)
	}
	for rows.Next() {
		v, err := s.scan(rows)
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	err = rows.Err()
	if err != nil {
		return out, err
	}
	if out == nil {
		// No rows, and no bound to have preallocated against. An empty result is
		// an empty slice rather than a nil one, so a caller never has to care
		// which of the two it got — encoding/json does.
		out = []T{}
	}
	return out, nil
}
