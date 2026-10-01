package barm

import (
	"reflect"
	"slices"
	"testing"
	"unsafe"
)

// fakeRows yields n rows without a database, which the Rows interface makes
// cheap to do.
type fakeRows struct{ n, i int }

func (f *fakeRows) Columns() ([]string, error) { return []string{"id", "age"}, nil }
func (f *fakeRows) Next() bool                 { f.i++; return f.i <= f.n }
func (f *fakeRows) Err() error                 { return nil }
func (f *fakeRows) Close() error               { return nil }

func (f *fakeRows) Scan(dest ...any) error {
	*(dest[0].(*int64)) = int64(f.i) //nolint:forcetypeassert // dest always comes from row{}'s two fields
	*(dest[1].(*int)) = f.i          //nolint:forcetypeassert // dest always comes from row{}'s two fields
	return nil
}

type row struct {
	ID  int64 `barm:"id"`
	Age int   `barm:"age"`
}

func TestScanSliceHint(t *testing.T) {
	t.Parallel()
	// the hint only sizes the slice; the rows still decide the length
	got, err := scanSlice[row](&fakeRows{n: 3}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || cap(got) != 100 {
		t.Errorf("len = %d, cap = %d, want 3 and 100", len(got), cap(got))
	}

	// an outsized hint is capped by memory, not by a count
	got, err = scanSlice[row](&fakeRows{n: 2}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if want := maxPreallocBytes / int(unsafe.Sizeof(row{})); cap(got) != want {
		t.Errorf("cap = %d, want %d", cap(got), want)
	}

	// ... so a wide row gets fewer slots than a narrow one, for the same hint
	type wide struct {
		ID  int64     `barm:"id"`
		Age int       `barm:"age"`
		Pad [256]byte `barm:"-"`
	}
	narrow, _ := scanSlice[row](&fakeRows{n: 1}, 1<<30)
	fat, _ := scanSlice[wide](&fakeRows{n: 1}, 1<<30)
	if cap(fat) >= cap(narrow) {
		t.Errorf("wide cap %d is not below narrow cap %d", cap(fat), cap(narrow))
	}
	if cap(fat)*int(unsafe.Sizeof(wide{})) > maxPreallocBytes {
		t.Errorf("wide preallocation is %d bytes, over the cap", cap(fat)*int(unsafe.Sizeof(wide{})))
	}

	// A zero-size element has nothing to bound, and the guard that says so is
	// what keeps 65536/0 out of the generated code. It is stripped everywhere
	// else — see the constants in the assembly — so it costs nothing to keep.
	if n := prealloc[struct{}](1 << 20); n != 1<<20 {
		t.Errorf("zero-size prealloc = %d, want the hint through unchanged", n)
	}

	// more rows than the hint still works, it just grows
	got, err = scanSlice[row](&fakeRows{n: 10}, 2)
	if err != nil || len(got) != 10 {
		t.Errorf("len = %d, err = %v", len(got), err)
	}
}

func BenchmarkScanSlice(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run("grow/"+itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := scanSlice[row](&fakeRows{n: n}, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("hint/"+itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := scanSlice[row](&fakeRows{n: n}, int64(n))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// Addr is embedded by pointer in the test below; it has to be exported, because
// reflect cannot reach through an unexported embedded field.
type Addr struct {
	City string `barm:"city"`
}

// rowsOf yields the given values, one column per element.
type rowsOf struct {
	cols []string
	vals [][]any
	i    int
}

func (r *rowsOf) Columns() ([]string, error) { return r.cols, nil }
func (r *rowsOf) Next() bool                 { r.i++; return r.i <= len(r.vals) }
func (r *rowsOf) Err() error                 { return nil }
func (r *rowsOf) Close() error               { return nil }

func (r *rowsOf) Scan(dest ...any) error {
	for i, v := range r.vals[r.i-1] {
		switch d := dest[i].(type) {
		case *int64:
			*d = v.(int64) //nolint:forcetypeassert // vals is built by each test to match its own column types
		case *string:
			*d = v.(string) //nolint:forcetypeassert // vals is built by each test to match its own column types
		}
	}
	return nil
}

// The scanner reuses one row value, so a plan that steps through a pointer must
// opt out: otherwise every row's copy would share the pointee.
func TestScanThroughPointer(t *testing.T) {
	t.Parallel()
	type person struct {
		ID    int64 `barm:"id"`
		*Addr       // embedded, behind a pointer
	}

	if p := mustPlan(t, reflect.TypeFor[person](), []string{"id", "city"}); !p.viaPointer {
		t.Fatal("plan did not notice the pointer hop")
	}

	got, err := scanSlice[person](&rowsOf{
		cols: []string{"id", "city"},
		vals: [][]any{{int64(1), "Kyiv"}, {int64(2), "Lviv"}},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows", len(got))
	}
	if got[0].City != "Kyiv" || got[1].City != "Lviv" {
		t.Errorf("cities = %q, %q — the rows shared a pointee", got[0].City, got[1].City)
	}
	if got[0].Addr == got[1].Addr {
		t.Error("both rows point at the same struct")
	}
}

// Reuse must not let one row's values survive into the next.
func TestScanReuseDoesNotBleed(t *testing.T) {
	t.Parallel()
	type row2 struct {
		ID   int64  `barm:"id"`
		Name string `barm:"name"`
	}
	if p := mustPlan(t, reflect.TypeFor[row2](), []string{"id", "name"}); p.viaPointer {
		t.Fatal("this plan should reuse its row")
	}

	got, err := scanSlice[row2](&rowsOf{
		cols: []string{"id", "name"},
		vals: [][]any{{int64(1), "ann"}, {int64(2), "bo"}},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != (row2{1, "ann"}) || got[1] != (row2{2, "bo"}) {
		t.Errorf("rows = %+v", got)
	}
}

// An unexported embedded struct is skipped rather than mapped, since scanning
// into it would panic.
func TestUnexportedEmbeddedIsSkipped(t *testing.T) {
	t.Parallel()
	type hidden struct {
		City string `barm:"city"`
	}
	type person struct {
		ID int64 `barm:"id"`
		hidden
	}
	if cols := mustModel[person](t).names; len(cols) != 1 || cols[0] != "id" {
		t.Errorf("columns = %v, want [id]", cols)
	}
}

// The plan cache is keyed by a hash of the column names, so it has to keep the
// names too: different columns, and the same columns in a different order, are
// different plans.
func TestPlanCacheKeying(t *testing.T) {
	t.Parallel()
	type row3 struct {
		ID   int64  `barm:"id"`
		Name string `barm:"name"`
	}
	rt := reflect.TypeFor[row3]()

	byName := mustPlan(t, rt, []string{"id", "name"})
	if mustPlan(t, rt, []string{"id", "name"}) != byName {
		t.Error("the same columns did not hit the cache")
	}

	// order decides which field each column lands in
	flipped := mustPlan(t, rt, []string{"name", "id"})
	if slices.Equal(flipped.paths[0], byName.paths[0]) {
		t.Error("reordered columns reused the same mapping")
	}

	// a narrower result is its own plan
	if narrow := mustPlan(t, rt, []string{"id"}); len(narrow.paths) != 1 {
		t.Errorf("narrow plan has %d paths, want 1", len(narrow.paths))
	}

	// and a column the model does not map is discarded, not misrouted
	unknown := mustPlan(t, rt, []string{"id", "nope"})
	if unknown.paths[1] != nil {
		t.Errorf("unmapped column got a path: %v", unknown.paths[1])
	}
}

// A nullzero array takes what a plain database/sql scan would: a driver value
// of the same kind that converts, named or not. A []byte is not one, there or
// here.
func TestNullZeroArray(t *testing.T) {
	t.Parallel()
	type key [4]byte
	type driverKey [4]byte // a driver's own named type: convertible, not assignable
	var k key
	n := holder{v: reflect.ValueOf(&k).Elem()}
	err := n.Scan([4]byte{1, 2, 3, 4})
	if err != nil || k != (key{1, 2, 3, 4}) {
		t.Errorf("array: %v, %v", k, err)
	}
	err = n.Scan(driverKey{5, 6, 7, 8})
	if err != nil || k != (key{5, 6, 7, 8}) {
		t.Errorf("named array: %v, %v", k, err)
	}
	err = n.Scan(nil)
	if err != nil || k != (key{}) {
		t.Errorf("NULL: %v, %v", k, err)
	}
	err = n.Scan([]byte{1, 2, 3, 4})
	if err == nil {
		t.Error("a []byte into an array should fail, as it does without nullzero")
	}
}

// Decoding into a value that already holds something replaces it: json merges
// into maps and structs, and a reused row or a value written back is not empty.
func TestJSONHolderReplaces(t *testing.T) {
	t.Parallel()
	m := map[string]int{"old": 1}
	h := holder{v: reflect.ValueOf(&m).Elem(), json: true}
	err := h.Scan(`{"new":2}`)
	if err != nil || len(m) != 1 || m["new"] != 2 {
		t.Errorf("map = %v, %v, want only new", m, err)
	}
	err = h.Scan([]byte(`{"b":3}`))
	if err != nil || len(m) != 1 || m["b"] != 3 {
		t.Errorf("map = %v, %v, want only b", m, err)
	}
	err = h.Scan(nil)
	if err != nil || m != nil {
		t.Errorf("NULL: %v, %v", m, err)
	}
}
