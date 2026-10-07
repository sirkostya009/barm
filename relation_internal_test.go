package barm

import (
	"reflect"
	"strings"
	"testing"
)

// The key predicate is one array per column where the dialect has arrays and
// every column a type to cast to, and row values spelled out otherwise.
func TestCompositeKeyPredicate(t *testing.T) {
	t.Parallel()
	str, num := reflect.TypeFor[string](), reflect.TypeFor[int]()
	keys := relKeys{n: 2}
	keys.cols[0], keys.cols[1] = []any{"a", "b"}, []any{1, 2}
	keys.types[0], keys.types[1] = str, num
	keys.casts[0], keys.casts[1] = "text", "bigint"
	names := []string{`"t"."id"`, `"t"."v"`}

	sql, args := keyPredicate(Postgres, names, &keys)
	if sql != `("t"."id", "t"."v") IN (SELECT * FROM unnest(?::text[], ?::bigint[]))` {
		t.Errorf("postgres: %s", sql)
	}
	if !reflect.DeepEqual(args, []any{[]string{"a", "b"}, []int{1, 2}}) {
		t.Errorf("postgres args: %#v", args)
	}

	uncast := keys
	uncast.casts[1] = ""
	for _, tc := range []struct {
		name string
		d    Dialect
		k    relKeys
	}{{"sqlite", SQLite, keys}, {"mysql", MySQL, keys}, {"postgres without a cast", Postgres, uncast}} {
		sql, args := keyPredicate(tc.d, names, &tc.k)
		if sql != `("t"."id", "t"."v") IN ((?, ?), (?, ?))` {
			t.Errorf("%s: %s", tc.name, sql)
		}
		if !reflect.DeepEqual(args, []any{"a", 1, "b", 2}) {
			t.Errorf("%s args: %#v", tc.name, args)
		}
	}
}

func TestCompositeRelationTags(t *testing.T) {
	t.Parallel()
	type child struct {
		BaseModel `barm:"table:c"`

		A int `barm:"a"`
	}
	type parent struct {
		BaseModel `barm:"table:p"`

		A    int     `barm:"a"`
		Two  []child `barm:"rel:a=a,rel:b=b"`
		Bad  []child `barm:"rel:a=a,rel:b"`
		Five []child `barm:"rel:a=a,rel:b=b,rel:c=c,rel:d=d,rel:e=e"`
	}
	m, err := modelOfType(reflect.TypeFor[parent]())
	if err != nil {
		t.Fatal(err)
	}
	two, ok := m.rel("Two")
	if !ok || !reflect.DeepEqual(two.parentCols, []string{"a", "b"}) || !reflect.DeepEqual(two.childCols, []string{"a", "b"}) {
		t.Errorf("Two = %+v", two)
	}
	if _, ok := m.rel("Bad"); ok {
		t.Error("a pair without = should not make a relation")
	}
	_, _, err = NewBuilder(Postgres).Select[parent]().Relation[child]("Five").Build()
	if err == nil || !strings.Contains(err.Error(), "5 columns") {
		t.Errorf("five key columns: %v", err)
	}
}
