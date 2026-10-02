package barm

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func encodeArray(t *testing.T, v any) string {
	t.Helper()
	b, err := appendArray(nil, reflect.ValueOf(v))
	if err != nil {
		t.Fatalf("%#v: %v", v, err)
	}
	return string(b)
}

func TestArrayLiteral(t *testing.T) {
	t.Parallel()
	s := "x"
	for _, tc := range []struct {
		v    any
		want string
	}{
		{[]int64{}, `{}`},
		{[]int64{1, -2, 3}, `{1,-2,3}`},
		{[]uint8{1, 2}, `{1,2}`},
		{[]string{"a", "", "NULL", `q"t`, `b\s`, "a,b", "{x}", " sp "}, `{"a","","NULL","q\"t","b\\s","a,b","{x}"," sp "}`},
		{[]*string{&s, nil}, `{"x",NULL}`},
		{[]bool{true, false}, `{t,f}`},
		{[]float64{1.5, math.Inf(1), math.Inf(-1), 0.1}, `{1.5,Infinity,-Infinity,0.1}`},
		{[][]int{{1, 2}, {3, 4}}, `{{1,2},{3,4}}`},
		{[][]byte{{1, 0xab}}, `{"\\x01ab"}`},
		{[]time.Time{time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)}, `{"2026-01-02T03:04:05.000000006Z"}`},
		{[]any{1, "a", nil}, `{1,"a",NULL}`},
		{&[]int{7}, `{7}`},
	} {
		if got := encodeArray(t, tc.v); got != tc.want {
			t.Errorf("%#v = %s, want %s", tc.v, got, tc.want)
		}
	}
	_, err := appendArray(nil, reflect.ValueOf([]struct{}{{}}))
	if err == nil {
		t.Error("a struct element should not encode")
	}
}

func decodeInto[S any](t *testing.T, s string) S {
	t.Helper()
	var out S
	err := decodeArray(s, reflect.ValueOf(&out).Elem())
	if err != nil {
		t.Fatalf("%s: %v", s, err)
	}
	return out
}

func TestArrayParse(t *testing.T) {
	t.Parallel()
	check := func(got, want any) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}
	}
	check(decodeInto[[]int64](t, `{}`), []int64{})
	check(decodeInto[[]int64](t, `{1,-2,3}`), []int64{1, -2, 3})
	check(decodeInto[[]int64](t, ` { 1 , 2 } `), []int64{1, 2})
	check(decodeInto[[]int32](t, `[0:1]={5,6}`), []int32{5, 6})
	check(decodeInto[[]string](t, `{a,"","NULL","q\"t","b\\s","a,b",plain\,esc}`), []string{"a", "", "NULL", `q"t`, `b\s`, "a,b", "plain,esc"})
	check(decodeInto[[]string](t, `{a,NULL,null}`), []string{"a", "", ""})
	x := "x"
	check(decodeInto[[]*string](t, `{x,NULL}`), []*string{&x, nil})
	check(decodeInto[[]bool](t, `{t,f,true}`), []bool{true, false, true})
	check(decodeInto[[]float64](t, `{1.5,Infinity,-Infinity}`), []float64{1.5, math.Inf(1), math.Inf(-1)})
	check(decodeInto[[][]int](t, `{{1,2},{3,4}}`), [][]int{{1, 2}, {3, 4}})
	check(decodeInto[[][]byte](t, `{"\\x01ab"}`), [][]byte{{1, 0xab}})
	check(decodeInto[*[]int](t, `{7}`), &[]int{7})
	check(decodeInto[[]time.Time](t, `{"2026-01-02 03:04:05.5+01","2026-01-02 03:04:05","2026-01-02","2026-01-02T03:04:05Z"}`), []time.Time{
		time.Date(2026, 1, 2, 3, 4, 5, 5e8, time.FixedZone("", 3600)),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})

	for _, bad := range []string{``, `1,2`, `{1,2`, `{1,2}x`, `{"a}`, `{1;2}`, `{abc}`} {
		var out []int
		err := decodeArray(bad, reflect.ValueOf(&out).Elem())
		if err == nil {
			t.Errorf("%q parsed as %v", bad, out)
		}
	}
}

// Whatever appendArray writes, decodeArray reads back the same.
func TestArrayRoundTrip(t *testing.T) {
	t.Parallel()
	for _, v := range []any{
		[]string{"", "NULL", `"\`, "a b", "{,}", "ünï"},
		[]int16{math.MinInt16, 0, math.MaxInt16},
		[]uint64{math.MaxUint64},
		[]float32{1.25, -0},
		[][]string{{"a"}, {"b"}},
		[]bool{},
	} {
		lit := encodeArray(t, v)
		out := reflect.New(reflect.TypeOf(v))
		err := decodeArray(lit, out.Elem())
		if err != nil {
			t.Fatalf("%s: %v", lit, err)
		}
		if !reflect.DeepEqual(out.Elem().Interface(), v) {
			t.Errorf("%#v went through %s as %#v", v, lit, out.Elem().Interface())
		}
	}
}

func TestArrayTagNeedsASlice(t *testing.T) {
	t.Parallel()
	type scalar struct {
		N int `barm:"n,array"`
	}
	type both struct {
		N []int `barm:"n,array,json"`
	}
	type ptr struct {
		N *[]int `barm:"n,array"`
	}
	for _, tc := range []struct {
		typ  reflect.Type
		want string
	}{
		{reflect.TypeFor[scalar](), "not a slice"},
		{reflect.TypeFor[both](), "both json and array"},
	} {
		_, err := modelOfType(tc.typ)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.typ, err, tc.want)
		}
	}
	_, err := modelOfType(reflect.TypeFor[ptr]())
	if err != nil {
		t.Errorf("a pointer to a slice: %v", err)
	}
}

type arraysOnly struct{ Rows }

func (arraysOnly) NativeArrays() bool { return true }

type jsonOnly struct{ Rows }

func (jsonOnly) NativeJSON() bool { return true }

// JSON and arrays are left to the driver each on its own say.
func TestNativeJSONAndArraysApart(t *testing.T) {
	t.Parallel()
	type row struct {
		Meta map[string]any `barm:"meta,json"`
		Ints []int64        `barm:"ints,array"`
	}
	p, err := planFor(reflect.TypeFor[row](), []string{"meta", "ints"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		rows   Rows
		direct [2]bool
	}{
		{arraysOnly{}, [2]bool{false, true}},
		{jsonOnly{}, [2]bool{true, false}},
	} {
		hs := p.holders(tc.rows)
		for i, want := range tc.direct {
			if hs[i].direct != want {
				t.Errorf("%T: column %d direct = %v, want %v", tc.rows, i, hs[i].direct, want)
			}
		}
	}

	b := &builder{nativeArrays: true}
	m, err := modelOfType(reflect.TypeFor[row]())
	if err != nil {
		t.Fatal(err)
	}
	v := reflect.ValueOf(row{Meta: map[string]any{"k": 1}, Ints: []int64{1}})
	if _, ok := b.value(m.byName["meta"], v.Field(0)).(string); !ok {
		t.Error("json was left to a driver that only takes arrays")
	}
	if _, ok := b.value(m.byName["ints"], v.Field(1)).([]int64); !ok {
		t.Error("an array was encoded for a driver that takes arrays")
	}
}
