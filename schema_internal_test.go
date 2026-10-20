package barm

import (
	"reflect"
	"testing"
	"unsafe"
)

func mustModel[T any](t *testing.T) *model {
	t.Helper()
	m, err := modelOf[T]()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mustPlan(t *testing.T, rt reflect.Type, cols []string) *plan {
	t.Helper()
	p, err := planFor(rt, cols)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTableComesFromBaseModel(t *testing.T) {
	t.Parallel()
	type Untagged struct{ ID int64 }
	type Tagged struct {
		BaseModel `barm:"table:widgets"`
		ID        int64 `barm:"id"`
	}

	if got := mustModel[Untagged](t).table; got != "" {
		t.Errorf("table = %q, want empty — nothing is guessed from the type name", got)
	}
	if got := mustModel[Tagged](t).table; got != "widgets" {
		t.Errorf("table = %q, want widgets", got)
	}
}

func TestTagOptions(t *testing.T) {
	t.Parallel()
	type Row struct {
		BaseModel `barm:"table:rows,alias:r"`

		ID      int64  `barm:"id,pk,autoincrement"`
		Ident   int64  `barm:"ident,identity"`
		Name    string `barm:"name"`
		Skipped string `barm:"-"`
		Unknown string `barm:"unknown,bogus,nonsense:1"`
	}

	m := mustModel[Row](t)
	if m.table != "rows" || m.alias != "r" {
		t.Errorf("table = %q, alias = %q", m.table, m.alias)
	}
	if got := m.names; len(got) != 4 {
		t.Fatalf("columns = %v — `-` should not name a column", got)
	}

	id, _ := m.field("id")
	if !id.pk || !id.auto {
		t.Errorf("id = %+v, want pk and auto", id)
	}
	ident, _ := m.field("ident")
	if ident.pk || !ident.auto {
		t.Errorf("ident = %+v, want auto only", ident)
	}
	name, _ := m.field("name")
	if name.pk || name.auto {
		t.Errorf("name = %+v, want neither", name)
	}
	// an unknown option is ignored, and does not disturb the column name
	if _, ok := m.field("unknown"); !ok {
		t.Error("unknown-option field lost its column")
	}
}

func TestColumnsComeFromTags(t *testing.T) {
	t.Parallel()
	type Row struct {
		BaseModel  `barm:"table:rows"`
		Kept       string `barm:"kept"`
		Ignored    string
		unexported string `barm:"nope"` //nolint:unused // read via reflection to prove it's skipped, not by Go code
	}

	cols := mustModel[Row](t).names
	if len(cols) != 1 || cols[0] != "kept" {
		t.Errorf("columns = %v, want [kept] — an untagged field is not a column", cols)
	}
}

// Every write returning a model's columns shares one list per dialect, and a
// query adding to its own copy leaves that list alone.
func TestReturningColsShared(t *testing.T) {
	t.Parallel()
	type row struct {
		BaseModel `barm:"table:rows"`
		ID        int64  `barm:"id,pk"`
		Name      string `barm:"name"`
	}
	m := mustModel[row](t)
	first, err := m.returningCols(Postgres, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(first, frag{sql: "extra"})
	again, err := m.returningCols(Postgres, nil)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.SliceData(first) != unsafe.SliceData(again) {
		t.Error("the list was built again")
	}
	if len(again) != 2 || cap(again) != 2 {
		t.Errorf("the shared list changed: %v", again)
	}
}
