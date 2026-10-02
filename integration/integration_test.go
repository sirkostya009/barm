// Package integration exercises barm against a real database. It lives in its
// own module so the driver and its transitive dependencies stay out of barm's
// go.mod.
package integration_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
	_ "modernc.org/sqlite"
)

type User struct {
	barm.BaseModel `barm:"table:users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

func open(t *testing.T) *barm.DB {
	t.Helper()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/test.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		email TEXT NOT NULL,
		age INTEGER NOT NULL,
		created_at TIMESTAMP NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestRoundTrip(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	now := time.Now().UTC().Truncate(time.Second)

	alice := &User{Name: "alice", Email: "a@x.io", Age: 30, CreatedAt: now}
	bob := &User{Name: "bob", Email: "b@x.io", Age: 17, CreatedAt: now}
	if _, err := db.Insert[User]().Values(alice, bob).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	users, err := db.Select[User]().Where("age >= ?", 18).Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || users[0].Name != "alice" || users[0].ID == 0 {
		t.Fatalf("users = %+v", users)
	}
	if !users[0].CreatedAt.Equal(now) {
		t.Errorf("created_at = %v, want %v", users[0].CreatedAt, now)
	}

	// scalar projection through the same builder
	names, err := db.Select[User]().ColumnExpr("name").OrderBy("name").SliceAs[string](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("names = %v", names)
	}

	n, err := db.Select[User]().Count(ctx)
	if err != nil || n != 2 {
		t.Fatalf("count = %d, err = %v", n, err)
	}

	one, err := db.Select[User]().Where("name = ?", "bob").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if one.Age != 17 {
		t.Errorf("one = %+v", one)
	}

	if _, err := db.Select[User]().Where("name = ?", "nobody").One(ctx); err != barm.ErrNoRows {
		t.Errorf("err = %v, want ErrNoRows", err)
	}
}

func TestSeqStopsEarly(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for i := range 100 {
		u := &User{Name: "u", Email: "e", Age: i, CreatedAt: time.Now()}
		if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	seen := 0
	for u, err := range db.Select[User]().OrderBy("age").Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		seen++
		if u.Age == 4 {
			break
		}
	}
	if seen != 5 {
		t.Errorf("seen = %d, want 5", seen)
	}
}

func TestInsertReturning(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "carol", Email: "c@x.io", Age: 41, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Returning("id").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if u.ID == 0 {
		t.Fatal("RETURNING did not fill in the primary key")
	}

	// the result still reports what was written, as Exec's does everywhere else
	a, b := &User{Name: "a", CreatedAt: time.Now()}, &User{Name: "b", CreatedAt: time.Now()}
	res, err := db.Insert[User]().Values(a, b).Returning("id").Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("Exec with RETURNING returned a nil result")
	}
	if n, err := res.RowsAffected(); err != nil || n != 2 {
		t.Errorf("rows affected = %d, %v, want 2", n, err)
	}
}

type acct struct {
	barm.BaseModel `barm:"table:accts"`

	ID    int64  `barm:"id,pk,autoincrement"`
	Email string `barm:"email"`
	Name  string `barm:"name"`
}

// sqlite's last insert id belongs to the connection: an insert that conflicts
// and writes nothing, or updates instead, leaves the previous insert's there.
// Filling it in would hand the value another row's key.
func TestLastInsertIDAfterConflict(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	if _, err := db.Exec(t.Context(), `CREATE TABLE accts (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	first, other := &acct{Email: "a@x", Name: "a"}, &acct{Email: "b@x", Name: "b"}
	for _, a := range []*acct{first, other} {
		if _, err := db.Insert[acct]().Values(a).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if first.ID == 0 || other.ID == 0 {
		t.Fatalf("plain inserts got no keys: %d, %d", first.ID, other.ID)
	}

	dup := &acct{Email: "a@x", Name: "dup"}
	if _, err := db.Insert[acct]().Values(dup).On("CONFLICT (email) DO NOTHING").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if dup.ID != 0 {
		t.Errorf("conflicting insert got id %d, but wrote nothing", dup.ID)
	}
	up := &acct{Email: "a@x", Name: "upserted"}
	if _, err := db.Insert[acct]().Values(up).On("CONFLICT (email) DO UPDATE SET name = excluded.name").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if up.ID != 0 && up.ID != first.ID {
		t.Errorf("upsert got id %d, which is not the row it updated (%d)", up.ID, first.ID)
	}
}

func TestOffsetWithoutLimit(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for i := range 3 {
		if _, err := db.Insert[User]().Values(&User{Age: i, CreatedAt: time.Now()}).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Select[User]().OrderBy("age").Offset(1).Slice(ctx)
	if err != nil || len(got) != 2 || got[0].Age != 1 {
		t.Errorf("offset 1 = %+v, %v", got, err)
	}
}

type ShadowContact struct {
	Email string `barm:"email"`
}

type shadowUser struct {
	barm.BaseModel `barm:"table:users"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
	ShadowContact
}

// The outer field owns a column an embedded one also maps: it is what is
// written, and what is read back.
func TestEmbeddedColumnShadowing(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	u := &shadowUser{Name: "a", Email: "outer@x", CreatedAt: time.Now()}
	u.ShadowContact.Email = "inner@x"
	if _, err := db.Insert[shadowUser]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	u.Email = "updated@x"
	if _, err := db.Update[shadowUser]().Value(u).WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[shadowUser]().One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Email != "updated@x" {
		t.Errorf("email = %q, want the outer field's", got.Email)
	}
}

// Meta scans from a single column on its own, and embedding it hands its Scan
// to the struct around it.
type Meta struct{ S string }

func (m *Meta) Scan(src any) error {
	if s, ok := src.(string); ok {
		m.S = s
	}
	return nil
}

type withMeta struct {
	barm.BaseModel `barm:"table:users"`

	ID   int64  `barm:"id"`
	Name string `barm:"name"`
	Meta
}

// A struct that maps columns of its own is a model, whatever Scan it inherits
// from something it embeds; one that maps none and scans stays a single value.
func TestEmbeddedScannerKeepsTheModel(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	if _, err := db.Insert[User]().Values(&User{Name: "a", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[withMeta]().Slice(ctx)
	if err != nil || len(got) != 1 || got[0].Name != "a" || got[0].ID == 0 {
		t.Fatalf("rows = %+v, %v", got, err)
	}
	one, err := db.Select[withMeta]().One(ctx)
	if err != nil || one.Name != "a" {
		t.Fatalf("one = %+v, %v", one, err)
	}
	meta, err := db.Select[User]().Column("name").SliceAs[Meta](ctx)
	if err != nil || len(meta) != 1 || meta[0].S != "a" {
		t.Errorf("scanner as a value = %+v, %v", meta, err)
	}
	// a Scan of its own is asked for, tags or not
	own, err := db.Select[User]().Column("name").SliceAs[ownScan](ctx)
	if err != nil || len(own) != 1 || own[0].V != "scanned a" {
		t.Errorf("own Scan = %+v, %v", own, err)
	}
}

// ownScan maps a column, and also says how it reads from one.
type ownScan struct {
	V string `barm:"v"`
}

func (o *ownScan) Scan(src any) error {
	if s, ok := src.(string); ok {
		o.V = "scanned " + s
	}
	return nil
}

// An insert that skips values hands back fewer rows than it was given, and
// nothing says which value each belongs to: rather than write ids into the
// wrong structs, Exec writes none and says so. When every row comes back, they
// go back in order as always.
func TestOnReturningWritesBackOnlyWhenComplete(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	if _, err := db.Exec(t.Context(), `CREATE TABLE accts (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert[acct]().Values(&acct{Email: "b@x", Name: "b"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	a, dup := &acct{Email: "a@x", Name: "a"}, &acct{Email: "b@x", Name: "dup"}
	_, err := db.Insert[acct]().Values(dup, a).On("CONFLICT (email) DO NOTHING").Returning("id").Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "1 rows for 2 values") {
		t.Errorf("err = %v, want the counts", err)
	}
	if a.ID != 0 || dup.ID != 0 {
		t.Errorf("structs were written: a %d, dup %d", a.ID, dup.ID)
	}

	up, fresh := &acct{Email: "b@x", Name: "renamed"}, &acct{Email: "c@x", Name: "c"}
	_, err = db.Insert[acct]().Values(up, fresh).
		On("CONFLICT (email) DO UPDATE").Set("name = excluded.name").Returning("id").Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[acct]().Where("email = ?", "b@x").One(ctx)
	if err != nil || got.Name != "renamed" || up.ID != got.ID || fresh.ID == 0 || fresh.ID == up.ID {
		t.Errorf("upsert: row %+v, up %d, fresh %d, %v", got, up.ID, fresh.ID, err)
	}
}

// An unsigned key is as common as a signed one, and takes the id the same way.
func TestLastInsertIDUnsigned(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	type unsignedUser struct {
		barm.BaseModel `barm:"table:users"`

		ID        uint64    `barm:"id,pk,autoincrement"`
		Name      string    `barm:"name"`
		Email     string    `barm:"email"`
		Age       int       `barm:"age"`
		CreatedAt time.Time `barm:"created_at"`
	}
	u := &unsignedUser{Name: "u", CreatedAt: time.Now()}
	if _, err := db.Insert[unsignedUser]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if u.ID == 0 {
		t.Error("a uint64 key was not filled in")
	}
}

func TestUpdateAndDelete(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "dave", Email: "d@x.io", Age: 20, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Returning("id").Exec(ctx); err != nil {
		t.Fatal(err)
	}

	u.Age = 21
	if _, err := db.Update[User]().Value(u).Column("age").WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[User]().Where("id = ?", u.ID).One(ctx)
	if err != nil || got.Age != 21 {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	if _, err := db.Delete[User]().Value(u).WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 0 {
		t.Errorf("count = %d, want 0", n)
	}
}

func TestTxRollback(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	u := &User{Name: "eve", Email: "e@x.io", Age: 1, CreatedAt: time.Now()}
	if _, err := tx.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 0 {
		t.Errorf("count = %d, want 0 after rollback", n)
	}
}

func TestTxCommitThenRollbackIsNoop(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	u := &User{Name: "frank", Email: "f@x.io", Age: 2, CreatedAt: time.Now()}
	if _, err := tx.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != sql.ErrTxDone {
		t.Errorf("rollback after commit = %v, want sql.ErrTxDone", err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 1 {
		t.Errorf("count = %d, want 1", n)
	}
}

func TestOneVariants(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "gina", Email: "g@x.io", Age: 33, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	// default model projection
	got, err := db.Select[User]().Where("age = ?", 33).One(ctx)
	if err != nil || got.Name != "gina" {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	// Column() projection into a struct
	got, err = db.Select[User]().Column("name", "age").One(ctx)
	if err != nil || got.Name != "gina" || got.Age != 33 || got.ID != 0 {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	// single scalar expression
	name, err := db.Select[User]().ColumnExpr("name").OneAs[string](ctx)
	if err != nil || name != "gina" {
		t.Fatalf("name = %q, err = %v", name, err)
	}

	// raw expressions into a struct: mapped by the names the result reports
	got, err = db.Select[User]().ColumnExpr("name").ColumnExpr("age").One(ctx)
	if err != nil || got.Name != "gina" || got.Age != 33 {
		t.Fatalf("got = %+v, err = %v", got, err)
	}
	got, err = db.Select[User]().ColumnExpr("u.*").ColumnExpr("age + 1 AS age").One(ctx)
	if err != nil || got.Name != "gina" || got.Age != 34 {
		t.Fatalf("u.* plus an expression: got = %+v, err = %v", got, err)
	}

	if _, err := db.Select[User]().Where("age = ?", 999).One(ctx); err != barm.ErrNoRows {
		t.Errorf("err = %v, want ErrNoRows", err)
	}
}

func TestOneDoesNotMutateBuilder(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for i := range 3 {
		u := &User{Name: "u", Email: "e", Age: i, CreatedAt: time.Now()}
		if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	q := db.Select[User]().OrderBy("age")
	if _, err := q.One(ctx); err != nil {
		t.Fatal(err)
	}
	all, err := q.Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("len = %d, want 3 — One leaked its LIMIT 1 into the builder", len(all))
	}
}

// An *As terminal settles the projection on a copy: the query it was called on
// keeps its own type and columns.
func TestAsTerminalsDoNotMutate(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "lena", Email: "l@x.io", Age: 31, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	type nameOnly struct {
		Name string `barm:"name"`
	}
	q := db.Select[User]().Where("age = ?", 31)

	for range 2 {
		narrow, err := q.SliceAs[nameOnly](ctx)
		if err != nil || len(narrow) != 1 || narrow[0].Name != "lena" {
			t.Fatalf("narrow = %+v, err = %v", narrow, err)
		}

		// the query is still a User query, with every column
		full, err := q.Slice(ctx)
		if err != nil || len(full) != 1 || full[0].Email != "l@x.io" || full[0].Age != 31 {
			t.Fatalf("full = %+v, err = %v", full, err)
		}

		// and the same holds for the streaming and single-row terminals
		for row, err := range q.SeqAs[nameOnly](ctx) {
			if err != nil || row.Name != "lena" {
				t.Fatalf("row = %+v, err = %v", row, err)
			}
		}
		one, err := q.One(ctx)
		if err != nil || one.Email != "l@x.io" {
			t.Fatalf("one = %+v, err = %v", one, err)
		}
	}

	// A table-name query has no model of its own, so it adopts the result's.
	// That adoption must not stick: the next terminal picks its own type.
	raw := db.Select[any]().Table("users")
	if narrow, err := raw.SliceAs[nameOnly](ctx); err != nil || narrow[0].Name != "lena" {
		t.Fatalf("narrow = %+v, err = %v", narrow, err)
	}
	full, err := raw.SliceAs[User](ctx)
	if err != nil {
		t.Fatalf("the narrow result type stuck to the query: %v", err)
	}
	if len(full) != 1 || full[0].Email != "l@x.io" {
		t.Fatalf("full = %+v", full)
	}
}

func TestExists(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	ok, err := db.Select[User]().Where("age > ?", 10).Exists(ctx)
	if err != nil || ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}

	u := &User{Name: "hank", Email: "h@x.io", Age: 50, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	ok, err = db.Select[User]().Where("age > ?", 10).Exists(ctx)
	if err != nil || !ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
}

func TestTableNameEntryPoints(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "ivy", Email: "i@x.io", Age: 60, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Table("users").Values(u).Returning("id").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if u.ID == 0 {
		t.Fatal("the insert did not fill in the primary key")
	}

	// no model type anywhere: table by name, row type at the end
	got, err := db.Select[User]().Table("users").Where("age = ?", 60).One(ctx)
	if err != nil || got.Name != "ivy" {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	n, err := db.Select[int64]().Table("users").ColumnExpr("max(age)").One(ctx)
	if err != nil || n != 60 {
		t.Fatalf("n = %d, err = %v", n, err)
	}

	u.Age = 61
	if _, err := db.Update[User]().Table("users").Value(u).Column("age").WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err = db.Select[User]().Table("users").One(ctx); err != nil || got.Age != 61 {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	if _, err := db.Delete[User]().Table("users").Where("age = ?", 61).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	ok, err := db.Select[any]().Table("users").Exists(ctx)
	if err != nil || ok {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
}

func TestAnonymousStruct(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	for i, name := range []string{"a", "b", "c"} {
		u := &User{Name: name, Email: "e", Age: 20 + i, CreatedAt: time.Now()}
		if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// ad-hoc row type, columns taken from its tags
	seen := 0
	for row, err := range db.Select[struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}]().Table("users").OrderBy("age").Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if row.Age != 20+seen {
			t.Errorf("row = %+v", row)
		}
		seen++
	}
	if seen != 3 {
		t.Errorf("seen = %d, want 3", seen)
	}

	// One works too: the anonymous type's fields are a known projection
	row, err := db.Select[struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}]().Table("users").Where("age = ?", 22).One(ctx)
	if err != nil || row.Name != "c" || row.Age != 22 {
		t.Fatalf("row = %+v, err = %v", row, err)
	}

	// every column is named by its tag
	row2, err := db.Select[struct {
		Name      string    `barm:"name"`
		CreatedAt time.Time `barm:"created_at"`
	}]().Table("users").Where("age = ?", 20).One(ctx)
	if err != nil || row2.Name != "a" || row2.CreatedAt.IsZero() {
		t.Fatalf("row2 = %+v, err = %v", row2, err)
	}

	// a join projection over two tables, typed inline
	j, err := db.Select[struct {
		Name  string `barm:"name"`
		Other string `barm:"other"`
	}]().
		Table("users u").
		Join("JOIN users o ON o.age = u.age + ?", 1).
		Column("u.name").
		ColumnExpr("o.name AS other").
		Where("u.age = ?", 20).
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(j) != 1 || j[0].Name != "a" || j[0].Other != "b" {
		t.Fatalf("j = %+v", j)
	}
}

func TestSubsetProjectionScans(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	u := &User{Name: "jen", Email: "j@x.io", Age: 44, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	type nameAge struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}
	got, err := db.Select[User]().OneAs[nameAge](ctx)
	if err != nil || got != (nameAge{Name: "jen", Age: 44}) {
		t.Fatalf("got = %+v, err = %v", got, err)
	}

	rows, err := db.Select[User]().SliceAs[nameAge](ctx)
	if err != nil || len(rows) != 1 || rows[0].Name != "jen" {
		t.Fatalf("rows = %+v, err = %v", rows, err)
	}
}

type recorder struct {
	mu     sync.Mutex
	events []barm.QueryEvent
	ctxKey struct{}
}

func (r *recorder) hook() barm.QueryHook {
	return barm.QueryHook{BeforeQuery: r.BeforeQuery, AfterQuery: r.AfterQuery}
}

func (r *recorder) BeforeQuery(ctx context.Context, ev *barm.QueryEvent) context.Context {
	return context.WithValue(ctx, &r.ctxKey, ev.StartedAt)
}

func (r *recorder) AfterQuery(ctx context.Context, ev *barm.QueryEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Value(&r.ctxKey) == nil {
		panic("context from BeforeQuery did not reach AfterQuery")
	}
	r.events = append(r.events, *ev)
}

func TestQueryHooks(t *testing.T) {
	ctx := t.Context()
	rec := &recorder{}
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/h.db", barm.SQLite, barm.WithHook(rec.hook()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER,
		created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	rec.events = nil // the CREATE above is watched too, and not what this counts
	rec.mu.Unlock()

	u := &User{Name: "kim", Email: "k@x.io", Age: 30, CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Select[User]().Where("age = ?", 30).Prepare("by_age").One(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Select[User]().Slice(ctx); err != nil {
		t.Fatal(err)
	}
	// a failing query still reports
	if _, err := db.Select[User]().Where("nope = ?", 1).One(ctx); err == nil {
		t.Fatal("expected a query error")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.events) != 4 {
		t.Fatalf("events = %d, want 4", len(rec.events))
	}
	for _, want := range []struct {
		op       string
		prepared string
		failed   bool
	}{
		{"INSERT", "", false},
		{"SELECT", "by_age", false},
		{"SELECT", "", false},
		{"SELECT", "", true},
	} {
		ev := rec.events[0]
		rec.events = rec.events[1:]
		if ev.Op != want.op || ev.Prepared != want.prepared || (ev.Err != nil) != want.failed {
			t.Errorf("event = %+v, want op=%s prepared=%q failed=%v", ev, want.op, want.prepared, want.failed)
		}
		if ev.Query == "" || ev.Duration <= 0 {
			t.Errorf("event = %+v, want a query and a duration", ev)
		}
	}
}

// Every BeforeQuery is paired with an AfterQuery, whichever way the query goes:
// a hook that opens a span in one and closes it in the other leaks otherwise.
func TestHooksPairOnEveryPath(t *testing.T) {
	ctx := t.Context()
	var before, after int
	var errs []error
	hook := barm.WithHook(barm.QueryHook{
		BeforeQuery: func(c context.Context, _ *barm.QueryEvent) context.Context { before++; return c },
		AfterQuery:  func(_ context.Context, ev *barm.QueryEvent) { after++; errs = append(errs, ev.Err) },
	})
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/pair.db", barm.SQLite, hook)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER,
		created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	before, after, errs = 0, 0, nil // the CREATE above is watched too, and not what this counts
	u := &User{Name: "kim", CreatedAt: time.Now()}

	// prepared, through Query and through Exec
	if _, err := db.Insert[User]().Prepare("ins").Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Select[User]().Prepare("all").Slice(ctx); err != nil {
		t.Fatal(err)
	}
	// a name bound to another query fails before reaching the database
	if _, err := db.Select[User]().Where("age = 1").Prepare("all").Slice(ctx); err == nil {
		t.Fatal("expected a name collision")
	}
	if _, err := db.Delete[User]().Where("age = 1").Prepare("ins").Exec(ctx); err == nil {
		t.Fatal("expected a name collision")
	}
	// nothing to run on
	nothing := barm.NewBuilder(barm.SQLite, hook)
	if _, err := nothing.Select[User]().Slice(ctx); !errors.Is(err, barm.ErrNoConn) {
		t.Fatalf("err = %v, want ErrNoConn", err)
	}
	if _, err := nothing.Delete[User]().Where("id = 1").Exec(ctx); !errors.Is(err, barm.ErrNoConn) {
		t.Fatalf("err = %v, want ErrNoConn", err)
	}

	if before != 6 || after != 6 {
		t.Fatalf("before = %d, after = %d, want 6 and 6", before, after)
	}
	for i, err := range errs {
		if (err != nil) != (i >= 2) {
			t.Errorf("event %d: err = %v", i, err)
		}
	}
}

func TestHookSeesInsertResult(t *testing.T) {
	ctx := t.Context()
	rec := &recorder{}
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/h2.db", barm.SQLite, barm.WithHook(rec.hook()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER,
		created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Insert[User]().Values(&User{Name: "n", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	last := rec.events[len(rec.events)-1]
	if last.Result == nil {
		t.Fatal("exec event has no sql.Result")
	}
	if n, err := last.Result.RowsAffected(); err != nil || n != 1 {
		t.Errorf("rows affected = %d, err = %v", n, err)
	}
}

// sqlite has no batch protocol, so batching says so rather than pretending.
func TestBatchUnsupportedDriver(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	b := db.Batch()
	users := b.Slice(db.Select[User]())

	err := b.Run(ctx)
	if !errors.Is(err, barm.ErrNoBatcher) {
		t.Fatalf("Run() = %v, want ErrNoBatcher", err)
	}
	if !errors.Is(users.Err(), barm.ErrNoBatcher) {
		t.Errorf("result err = %v, want ErrNoBatcher", users.Err())
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Batch().Exec(tx.Insert[User]().Values(&User{Name: "x"})).Err(); err != nil {
		// queueing succeeds; the driver is only consulted at Run
		_ = err
	}
}

func TestNestedTx(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.Insert[User]().Values(&User{Name: "kept", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	// a nested transaction that gets rolled back
	sp, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Insert[User]().Values(&User{Name: "dropped", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sp.Rollback(); err != nil {
		t.Fatal(err)
	}

	// the enclosing transaction is still alive and still has its first row
	n, err := tx.Select[User]().Count(ctx)
	if err != nil {
		t.Fatalf("the outer transaction did not survive the nested rollback: %v", err)
	}
	if n != 1 {
		t.Errorf("count in tx = %d, want 1", n)
	}

	// a nested transaction that gets committed
	sp2, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sp2.Rollback() // must be a no-op after Commit, not a rewind
	if _, err := tx.Insert[User]().Values(&User{Name: "also kept", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sp2.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := sp2.Rollback(); err != sql.ErrTxDone {
		t.Errorf("rollback after nested commit = %v, want sql.ErrTxDone", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 2 {
		t.Errorf("count after commit = %d, want 2", n)
	}
}

// A nested transaction borrows the connection; only the outermost returns it.
func TestNestedTxDoesNotReleaseConn(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	db.Pool().(*barm.SQLPool).DB().SetMaxOpenConns(1) // one slot, so an early release would deadlock the next Begin

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	inner, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.Commit(); err != nil {
		t.Fatal(err)
	}
	// the outer transaction still holds the only connection, so it still works
	if _, err := tx.Select[User]().Count(ctx); err != nil {
		t.Fatalf("the nested commit took the connection with it: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// and now the pool has it back
	waited, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	tx2, err := db.BeginTx(waited, nil)
	if err != nil {
		t.Fatalf("the connection was never returned: %v", err)
	}
	tx2.Rollback()
}

// Nesting goes as deep as you like, each level independent.
func TestNestedTxDepth(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	a, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Insert[User]().Values(&User{Name: "a", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	b, err := a.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Insert[User]().Values(&User{Name: "b", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Rollback(); err != nil { // drops "b" only
		t.Fatal(err)
	}
	if err := a.Commit(); err != nil { // keeps "a"
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	names, err := db.Select[User]().ColumnExpr("name").SliceAs[string](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "a" {
		t.Errorf("names = %v, want [a]", names)
	}
}

// A nested transaction cannot carry its own isolation level.
func TestNestedTxRejectsOpts(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}); err == nil {
		t.Error("expected an error for opts on a nested transaction")
	}
}

func TestPrepare(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	for i := range 3 {
		u := &User{Name: "u", Email: "e", Age: i, CreatedAt: time.Now()}
		if _, err := db.Insert[User]().Prepare("insert_user").Values(u).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	for i := range 3 {
		got, err := db.Select[User]().Where("age = ?", i).Prepare("user_by_age").One(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got.Age != i {
			t.Errorf("age = %d, want %d", got.Age, i)
		}
	}

	// inside a transaction the same name is reused, scoped to the tx
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	got, err := tx.Select[User]().Where("age = ?", 1).Prepare("user_by_age").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Age != 1 {
		t.Errorf("age = %d, want 1", got.Age)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareNameCollision(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	if _, err := db.Select[User]().Where("age = ?", 1).Prepare("dup").Slice(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := db.Select[User]().Where("name = ?", "x").Prepare("dup").Slice(ctx)
	if err == nil {
		t.Fatal("expected an error when a name is bound to a second query")
	}
}

// A prepare that fails is not remembered: the next query under the name tries
// again rather than getting the first one's error back forever.
func TestPrepareFailureIsRetried(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.Select[User]().Where("age = ?", 1).Prepare("retry").Slice(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := db.Select[User]().Where("age = ?", 1).Prepare("retry").Slice(ctx); err != nil {
		t.Fatalf("after a cancelled first use: %v", err)
	}

	// a table that does not exist yet fails the prepare until it does
	later := db.Select[User]().Table("later").Prepare("later")
	if _, err := later.Slice(ctx); err == nil {
		t.Fatal("expected an error preparing over a missing table")
	}
	if _, err := db.Exec(t.Context(), `CREATE TABLE later (id INTEGER, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := later.Slice(ctx); err != nil {
		t.Fatalf("after the table appeared: %v", err)
	}
}

// txRecorder records the transaction lifecycle, and can veto a commit.
type txRecorder struct {
	mu     sync.Mutex
	steps  []string
	events []barm.TxEvent
	veto   error
	ctxKey struct{}
}

func (r *txRecorder) record(step string, ev *barm.TxEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, step)
	r.events = append(r.events, *ev)
}

func (r *txRecorder) hook() barm.TxHook {
	return barm.TxHook{
		BeforeCommit:   r.BeforeCommit,
		AfterCommit:    r.AfterCommit,
		BeforeRollback: r.BeforeRollback,
		AfterRollback:  r.AfterRollback,
	}
}

func (r *txRecorder) BeforeCommit(ctx context.Context, ev *barm.TxEvent) (context.Context, error) {
	r.record("before-commit", ev)
	return context.WithValue(ctx, &r.ctxKey, ev.StartedAt), r.veto
}

func (r *txRecorder) AfterCommit(ctx context.Context, ev *barm.TxEvent) {
	if ctx.Value(&r.ctxKey) == nil {
		panic("context from BeforeCommit did not reach AfterCommit")
	}
	r.record("after-commit", ev)
}

func (r *txRecorder) BeforeRollback(ctx context.Context, ev *barm.TxEvent) context.Context {
	r.record("before-rollback", ev)
	return context.WithValue(ctx, &r.ctxKey, ev.StartedAt)
}

func (r *txRecorder) AfterRollback(ctx context.Context, ev *barm.TxEvent) {
	if ctx.Value(&r.ctxKey) == nil {
		panic("context from BeforeRollback did not reach AfterRollback")
	}
	r.record("after-rollback", ev)
}

func openTx(t *testing.T, rec *txRecorder) *barm.DB {
	t.Helper()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/tx.db", barm.SQLite, barm.WithTxHook(rec.hook()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER,
		created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func (r *txRecorder) took() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.steps)
}

func TestTxHooksCommit(t *testing.T) {
	ctx := t.Context()
	rec := &txRecorder{}
	db := openTx(t, rec)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() // the documented pattern: must not fire hooks again
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if got := rec.took(); !slices.Equal(got, []string{"before-commit", "after-commit"}) {
		t.Fatalf("steps = %v", got)
	}
	if ev := rec.events[1]; ev.Err != nil || ev.Duration <= 0 {
		t.Errorf("event = %+v", ev)
	}
}

// A deferred Rollback after a Commit is a no-op, and must stay silent.
func TestTxHooksRollbackAfterCommitIsSilent(t *testing.T) {
	ctx := t.Context()
	rec := &txRecorder{}
	db := openTx(t, rec)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("rollback err = %v, want ErrTxDone", err)
	}
	if got := rec.took(); !slices.Equal(got, []string{"before-commit", "after-commit"}) {
		t.Fatalf("steps = %v", got)
	}
}

func TestTxHooksRollback(t *testing.T) {
	ctx := t.Context()
	rec := &txRecorder{}
	db := openTx(t, rec)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := rec.took(); !slices.Equal(got, []string{"before-rollback", "after-rollback"}) {
		t.Fatalf("steps = %v", got)
	}
}

// A nested transaction decides nothing durable, so its endings fire nothing —
// only the outermost commit does.
func TestTxHooksNested(t *testing.T) {
	ctx := t.Context()
	rec := &txRecorder{}
	db := openTx(t, rec)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	want := []string{"before-commit", "after-commit"}
	if got := rec.took(); !slices.Equal(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
}

// BeforeCommit's error stops the commit and leaves the transaction open, so the
// deferred Rollback still undoes the work.
func TestTxHookVetoesCommit(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("outbox flush failed")
	rec := &txRecorder{veto: boom}
	db := openTx(t, rec)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	u := &User{Name: "vetoed", Email: "v@x.io", Age: 1, CreatedAt: time.Now()}
	if _, err := tx.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, boom) {
		t.Fatalf("commit err = %v, want %v", err, boom)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("the transaction should still be open: %v", err)
	}

	n, err := db.Select[User]().Count(ctx)
	if err != nil || n != 0 {
		t.Fatalf("count = %d, err = %v — the vetoed work must not be committed", n, err)
	}
	// The hook that returned the error unwinds itself, so it gets no
	// AfterCommit; the rollback that follows is reported in full.
	want := []string{"before-commit", "before-rollback", "after-rollback"}
	if got := rec.took(); !slices.Equal(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}
}

// A hook may set one field and leave the rest nil, which is the common case.
func TestTxHookPartial(t *testing.T) {
	ctx := t.Context()
	var commits, rollbacks int
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/p.db", barm.SQLite,
		barm.WithTxHook(barm.TxHook{
			AfterCommit: func(context.Context, *barm.TxEvent) { commits++ },
		}),
		barm.WithTxHook(barm.TxHook{
			AfterRollback: func(context.Context, *barm.TxEvent) { rollbacks++ },
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || rollbacks != 1 {
		t.Fatalf("commits = %d, rollbacks = %d, want 1 and 1", commits, rollbacks)
	}
}

// The same for a query hook: AfterQuery alone is a perfectly good hook.
func TestQueryHookPartial(t *testing.T) {
	ctx := t.Context()
	var seen []string
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/q.db", barm.SQLite,
		barm.WithHook(barm.QueryHook{
			AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { seen = append(seen, ev.Op) },
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE users (
		id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER,
		created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Select[User]().Slice(ctx); err != nil {
		t.Fatal(err)
	}
	// SQL run straight on the handle is watched as much as a built query
	if !slices.Equal(seen, []string{"CREATE", "SELECT"}) {
		t.Fatalf("seen = %v", seen)
	}
}

// Hooks can be registered on one transaction rather than on the DB.
func TestTxScopedHooks(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	var ended, queries int
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.WithTxHook(barm.TxHook{
		AfterCommit: func(context.Context, *barm.TxEvent) { ended++ },
	}).WithHook(barm.QueryHook{
		AfterQuery: func(context.Context, *barm.QueryEvent) { queries++ },
	})

	if _, err := tx.Select[User]().Slice(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if ended != 1 || queries != 1 {
		t.Fatalf("ended = %d, queries = %d, want 1 and 1", ended, queries)
	}

	// The DB itself never saw them, so a later transaction is unaffected.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Select[User]().Slice(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if ended != 1 || queries != 1 {
		t.Fatalf("ended = %d, queries = %d — the second transaction should not fire", ended, queries)
	}
}

// A hook registered inside a nested transaction belongs to the transaction, so
// it fires once the whole thing commits — not at the savepoint.
func TestTxHooksNestedRegistration(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	var fired []string
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	inner.WithTxHook(barm.TxHook{
		AfterCommit: func(context.Context, *barm.TxEvent) { fired = append(fired, "inner") },
	})
	if err := inner.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("fired = %v at the savepoint, want nothing yet", fired)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fired, []string{"inner"}) {
		t.Fatalf("fired = %v, want [inner]", fired)
	}
}

// Rolling a savepoint back undoes its work, so the hooks registered alongside it
// go too — a cache must not be invalidated for rows that were never written.
func TestTxHooksNestedRollbackDropsThem(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	var fired []string
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.WithTxHook(barm.TxHook{
		AfterCommit: func(context.Context, *barm.TxEvent) { fired = append(fired, "outer") },
	})

	inner, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	inner.WithTxHook(barm.TxHook{
		AfterCommit: func(context.Context, *barm.TxEvent) { fired = append(fired, "rolled-back") },
	})
	if err := inner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fired, []string{"outer"}) {
		t.Fatalf("fired = %v, want [outer] — the undone work must not report", fired)
	}
}

// A nested rollback drops what was registered within it, and only that: not a
// hook the enclosing transaction registered meanwhile, not one a sibling
// committed before it, and not — by a later rollback — one already dropped.
func TestTxHooksNestedBookkeeping(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	var fired []string
	hook := func(name string) barm.TxHook {
		return barm.TxHook{AfterCommit: func(context.Context, *barm.TxEvent) { fired = append(fired, name) }}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	// the outer registers while a nested one is open
	in, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.WithTxHook(hook("outer"))
	if err := in.Rollback(); err != nil {
		t.Fatal(err)
	}
	// a sibling commits, and a later sibling rolls back
	a, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.WithTxHook(hook("committed sibling"))
	if err := a.Commit(); err != nil {
		t.Fatal(err)
	}
	b, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Rollback(); err != nil {
		t.Fatal(err)
	}
	// a hook dropped by one rollback stays dropped through the next
	x, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	x.WithTxHook(hook("dropped"))
	y, err := x.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Rollback(); err != nil {
		t.Fatal(err)
	}
	_ = y.Rollback() // its savepoint went with x's; what matters is what it does to the list

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fired, []string{"committed sibling", "outer"}) {
		t.Fatalf("fired = %v, want [committed sibling outer]", fired)
	}
}

// Hooks registered on a transaction never reach the DB or another transaction.
func TestTxHooksDoNotLeakToTheDB(t *testing.T) {
	ctx := t.Context()
	var base, scoped int
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/s.db", barm.SQLite,
		barm.WithTxHook(barm.TxHook{
			AfterCommit: func(context.Context, *barm.TxEvent) { base++ },
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for range 3 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		tx.WithTxHook(barm.TxHook{
			AfterCommit: func(context.Context, *barm.TxEvent) { scoped++ },
		})
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// Each transaction registered one of its own: three commits, three of each.
	// A leak into the DB's slice would compound instead.
	if base != 3 || scoped != 3 {
		t.Fatalf("base = %d, scoped = %d, want 3 and 3", base, scoped)
	}
}

type event struct {
	barm.BaseModel `barm:"table:events,alias:e"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	CreatedAt time.Time `barm:"created_at,default:current_timestamp"`
}

func TestInsertAutoKeyMixedRows(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	set, unset := &User{ID: 100, Name: "set", CreatedAt: time.Now()}, &User{Name: "unset", CreatedAt: time.Now()}
	if _, err := db.Insert[User]().Values(set, unset).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[User]().OrderBy("name").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != 100 || got[1].ID == 0 || got[1].ID == 100 {
		t.Fatalf("rows = %+v, want id 100 and one the database picked", got)
	}
}

// The point of leaving the column out: the database fills it in.
func TestInsertDefaultAppliesInTheDatabase(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/d.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT current_timestamp)`); err != nil {
		t.Fatal(err)
	}

	// Nothing set: sqlite supplies both the id and the timestamp.
	if _, err := db.Insert[event]().Values(&event{Name: "auto"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[event]().Where("name = ?", "auto").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == 0 || got.CreatedAt.IsZero() {
		t.Fatalf("row = %+v, want an id and a timestamp from the database", got)
	}

	// Set explicitly: that value is what lands.
	want := time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)
	if _, err := db.Insert[event]().Values(&event{Name: "explicit", CreatedAt: want}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = db.Select[event]().Where("name = ?", "explicit").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(want) {
		t.Errorf("created_at = %s, want %s", got.CreatedAt, want)
	}

	// Mixed rows: the one that set it keeps its value, the other gets the
	// default expression rendered inline.
	if _, err := db.Insert[event]().Values(
		&event{Name: "m1", CreatedAt: want},
		&event{Name: "m2"},
	).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	m1, err := db.Select[event]().Where("name = ?", "m1").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := db.Select[event]().Where("name = ?", "m2").One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !m1.CreatedAt.Equal(want) || m2.CreatedAt.IsZero() || m2.CreatedAt.Equal(want) {
		t.Errorf("m1 = %s, m2 = %s", m1.CreatedAt, m2.CreatedAt)
	}
}

// A Conn is one connection held until Close, so session state set on it is
// still there for the next query — the thing a pooled DB cannot promise.
func TestConnHoldsItsSession(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// A temporary table lives on the connection that made it.
	if _, err := c.Exec(ctx, `CREATE TEMP TABLE tmp_users (name TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `INSERT INTO tmp_users VALUES ('held')`); err != nil {
		t.Fatal(err)
	}
	got, err := c.Select[string]().Table("tmp_users").Column("name").One(ctx)
	if err != nil {
		t.Fatalf("the query did not run on the connection that holds the table: %v", err)
	}
	if got != "held" {
		t.Errorf("name = %q, want held", got)
	}

	// The builders are all there, and all land on the same connection.
	u := &User{Name: "conn", Email: "c@x.io", Age: 40, CreatedAt: time.Now()}
	if _, err := c.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Select[User]().Where("name = ?", "conn").Count(ctx); err != nil || n != 1 {
		t.Fatalf("count = %d, err = %v", n, err)
	}
}

// A transaction begun on a Conn borrows it. Committing must not hand it back to
// the pool: the caller is still holding it.
func TestConnTxLeavesTheConnectionHeld(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Exec(ctx, `CREATE TEMP TABLE tmp_marker (n INTEGER)`); err != nil {
		t.Fatal(err)
	}

	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	u := &User{Name: "in-tx", Email: "t@x.io", Age: 41, CreatedAt: time.Now()}
	if _, err := tx.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Still the same connection: the temp table proves it was never released.
	if _, err := c.Exec(ctx, `INSERT INTO tmp_marker VALUES (1)`); err != nil {
		t.Fatalf("the connection was returned to the pool by Commit: %v", err)
	}
	if n, err := c.Select[User]().Where("name = ?", "in-tx").Count(ctx); err != nil || n != 1 {
		t.Fatalf("count = %d, err = %v", n, err)
	}

	// Rolling back a conn transaction leaves it held too.
	tx2, err := c.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, `INSERT INTO tmp_marker VALUES (2)`); err != nil {
		t.Fatalf("the connection was returned to the pool by Rollback: %v", err)
	}
}

// Prepare on a Conn prepares on that connection, not on the pool.
func TestConnPrepare(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	u := &User{Name: "prep", Email: "p@x.io", Age: 42, CreatedAt: time.Now()}
	if _, err := c.Insert[User]().Values(u).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := c.Select[User]().Where("name = ?", "prep").Prepare("by_name").One(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got.Name != "prep" {
			t.Errorf("name = %q", got.Name)
		}
	}
}

// --- relations ---------------------------------------------------------------

type Author struct {
	barm.BaseModel `barm:"table:authors,alias:a"`

	ID    int64    `barm:"id,pk,autoincrement"`
	Name  string   `barm:"name"`
	Email string   `barm:"email"`
	Books []Book   `barm:"rel:id=author_id"`
	Tags  []tagRow `barm:"rel:id=author_id"`
}

type Book struct {
	barm.BaseModel `barm:"table:books"`

	ID       int64    `barm:"id,pk,autoincrement"`
	Title    string   `barm:"title"`
	AuthorID int64    `barm:"author_id"`
	Reviews  []Review `barm:"rel:id=book_id"`
}

// the query-specific shapes: neither names a table, both take one from the model
type authorLite struct {
	ID    int64      `barm:"id,pk"`
	Name  string     `barm:"name"`
	Books []bookLite `barm:"rel:id=author_id"`
}

type bookLite struct {
	Title string `barm:"title"`
}

func openAuthors(t *testing.T, hook ...barm.Option) *barm.DB {
	t.Helper()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/rel.db", barm.SQLite, hook...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		`CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)`,
		`CREATE TABLE books (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author_id INTEGER)`,
	} {
		if _, err := db.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	lem := &Author{Name: "Lem", Email: "l@x.io"}
	borges := &Author{Name: "Borges", Email: "b@x.io"}
	mute := &Author{Name: "Mute", Email: "m@x.io"} // no books
	// one at a time: LastInsertId fills a single row's key, not a batch of them
	for _, a := range []*Author{lem, borges, mute} {
		if _, err := db.Insert[Author]().Values(a).Exec(ctx); err != nil {
			t.Fatal(err)
		}
		if a.ID == 0 {
			t.Fatalf("no id came back for %s", a.Name)
		}
	}
	if _, err := db.Insert[Book]().Values(
		&Book{Title: "Solaris", AuthorID: lem.ID},
		&Book{Title: "The Go Cyberiad", AuthorID: lem.ID},
		&Book{Title: "Ficciones", AuthorID: borges.ID},
	).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// The whole point: narrow shapes on both sides, and two queries however many
// authors come back.
func TestRelationHasMany(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var queries []string
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, ev.Query)
		},
	}))

	mu.Lock()
	queries = nil
	mu.Unlock()

	authors, err := db.Select[Author]().
		OrderBy("id").
		Relation[bookLite]("Books").
		SliceAs[authorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(authors) != 3 {
		t.Fatalf("authors = %d, want 3", len(authors))
	}
	if got := len(authors[0].Books); got != 2 {
		t.Errorf("Lem has %d books, want 2", got)
	}
	if len(authors[1].Books) != 1 || authors[1].Books[0].Title != "Ficciones" {
		t.Errorf("Borges = %+v", authors[1].Books)
	}
	if len(authors[2].Books) != 0 {
		t.Errorf("an author with no books should have none, got %+v", authors[2].Books)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 2 {
		t.Fatalf("queries = %d, want 2 — one per relation, not one per row:\n%s",
			len(queries), strings.Join(queries, "\n"))
	}
	// the parent selects only the projection's columns, the child only its own
	// plus the key it is grouped by
	if strings.Contains(queries[0], "email") {
		t.Errorf("parent over-fetched: %s", queries[0])
	}
	if !strings.Contains(queries[1], `"books"."author_id", "books"."title"`) {
		t.Errorf("child should select the key then its projection: %s", queries[1])
	}
	if strings.Contains(queries[1], `"books"."id"`) {
		t.Errorf("child over-fetched: %s", queries[1])
	}
}

// The fns filter the child query, and cannot displace the key predicate.
func TestRelationCustomizer(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	authors, err := db.Select[Author]().
		OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[bookLite]) *barm.SelectQuery[bookLite] {
			return q.Where("title LIKE ?", "%Go%")
		}).
		SliceAs[authorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors[0].Books) != 1 || authors[0].Books[0].Title != "The Go Cyberiad" {
		t.Errorf("Lem = %+v, want only the matching book", authors[0].Books)
	}
	if len(authors[1].Books) != 0 {
		t.Errorf("Borges = %+v, want none", authors[1].Books)
	}
}

// One loads relations for the row it found.
func TestRelationOne(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	a, err := db.Select[Author]().Where("name = ?", "Lem").
		Relation[bookLite]("Books").
		OneAs[authorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Books) != 2 {
		t.Errorf("books = %+v, want 2", a.Books)
	}
}

// Relations need the whole result, which a stream does not have.
func TestRelationSeqIsRefused(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	for _, err := range db.Select[Author]().Relation[bookLite]("Books").SeqAs[authorLite](ctx) {
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "Slice or One") {
			t.Errorf("err = %v, want it to name the way out", err)
		}
		break
	}
}

// The mistakes that are worth catching.
func TestRelationErrors(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	if _, err := db.Select[Author]().Relation[bookLite]("Nope").SliceAs[authorLite](ctx); err == nil ||
		!strings.Contains(err.Error(), "no relation") {
		t.Errorf("unknown field: %v", err)
	}

	// the projection has to select the column the relation joins on
	type noKey struct {
		Name  string     `barm:"name"`
		Books []bookLite `barm:"rel:id=author_id"`
	}
	if _, err := db.Select[Author]().Relation[bookLite]("Books").SliceAs[noKey](ctx); err == nil ||
		!strings.Contains(err.Error(), "does not select") {
		t.Errorf("missing key column: %v", err)
	}

	// With's type and the field's must agree
	type otherBook struct {
		Title string `barm:"title"`
	}
	type mismatch struct {
		ID    int64       `barm:"id,pk"`
		Books []otherBook `barm:"rel:id=author_id"`
	}
	if _, err := db.Select[Author]().Relation[bookLite]("Books").SliceAs[mismatch](ctx); err == nil ||
		!strings.Contains(err.Error(), "Relation was given") {
		t.Errorf("type mismatch: %v", err)
	}
}

// A relation of a relation: Relation inside the customizer, one more round trip.
type Review struct {
	barm.BaseModel `barm:"table:reviews"`

	ID     int64  `barm:"id,pk,autoincrement"`
	BookID int64  `barm:"book_id"`
	Body   string `barm:"body"`
}

type bookNested struct {
	ID      int64        `barm:"id,pk"`
	Title   string       `barm:"title"`
	Reviews []reviewLite `barm:"rel:id=book_id"`
}

type reviewLite struct {
	Body string `barm:"body"`
}

type authorNested struct {
	ID    int64        `barm:"id,pk"`
	Name  string       `barm:"name"`
	Books []bookNested `barm:"rel:id=author_id"`
}

func TestRelationNested(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var queries []string
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, ev.Query)
		},
	}))
	if _, err := db.Exec(t.Context(), `CREATE TABLE reviews (
		id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, body TEXT)`); err != nil {
		t.Fatal(err)
	}
	books, err := db.Select[Book]().OrderBy("id").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range books[:2] { // reviews for the first two books only
		if _, err := db.Insert[Review]().Values(
			&Review{BookID: b.ID, Body: "on " + b.Title},
		).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	queries = nil
	mu.Unlock()

	authors, err := db.Select[Author]().OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[bookNested]) *barm.SelectQuery[bookNested] {
			return q.Relation[reviewLite]("Reviews")
		}).
		SliceAs[authorNested](ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(authors[0].Books) != 2 {
		t.Fatalf("Lem's books = %+v", authors[0].Books)
	}
	// the nested rows survive being grouped onto their parents
	if len(authors[0].Books[0].Reviews) != 1 || authors[0].Books[0].Reviews[0].Body != "on Solaris" {
		t.Errorf("Solaris reviews = %+v", authors[0].Books[0].Reviews)
	}
	if len(authors[0].Books[1].Reviews) != 1 {
		t.Errorf("second book's reviews = %+v", authors[0].Books[1].Reviews)
	}
	if len(authors[1].Books[0].Reviews) != 0 {
		t.Errorf("Ficciones should have none, got %+v", authors[1].Books[0].Reviews)
	}

	mu.Lock()
	defer mu.Unlock()
	// one per level: authors, books, reviews
	if len(queries) != 3 {
		t.Errorf("queries = %d, want 3 (one per depth):\n%s", len(queries), strings.Join(queries, "\n"))
	}
}

// Two relations on one query are independent — both wait only on the parent
// keys — so they are sent together where the driver can pipeline. sqlite cannot,
// so here the fallback runs them one apiece, with the same results.
type tagRow struct {
	barm.BaseModel `barm:"table:tags"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Label    string `barm:"label"`
}

type tagLite struct {
	Label string `barm:"label"`
}

type authorTwo struct {
	ID    int64      `barm:"id,pk"`
	Name  string     `barm:"name"`
	Books []bookLite `barm:"rel:id=author_id"`
	Tags  []tagLite  `barm:"rel:id=author_id"`
}

func TestRelationSiblingsFallBackWithoutABatcher(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	if _, err := db.Exec(t.Context(), `CREATE TABLE tags (
		id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, label TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert[tagRow]().Values(&tagRow{AuthorID: 1, Label: "sf"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	authors, err := db.Select[Author]().OrderBy("id").
		Relation[bookLite]("Books").
		Relation[tagLite]("Tags").
		SliceAs[authorTwo](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors[0].Books) != 2 || len(authors[0].Tags) != 1 {
		t.Errorf("Lem = books %+v tags %+v", authors[0].Books, authors[0].Tags)
	}
	if len(authors[1].Books) != 1 || len(authors[1].Tags) != 0 {
		t.Errorf("Borges = books %+v tags %+v", authors[1].Books, authors[1].Tags)
	}
}

// A projection that selects the key itself keeps it, and the key is not sent twice.
type bookWithKey struct {
	AuthorID int64  `barm:"author_id"`
	Title    string `barm:"title"`
}

type authorWithKeyed struct {
	ID    int64         `barm:"id,pk"`
	Books []bookWithKey `barm:"rel:id=author_id"`
}

// The row's key field may be a different integer type from the parent's.
type bookNarrowKey struct {
	AuthorID int32  `barm:"author_id"`
	Title    string `barm:"title"`
}

type authorNarrowKeyed struct {
	ID    int64           `barm:"id,pk"`
	Books []bookNarrowKey `barm:"rel:id=author_id"`
}

func TestRelationKeyInProjection(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var queries []string
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, ev.Query)
		},
	}))
	mu.Lock()
	queries = nil
	mu.Unlock()

	authors, err := db.Select[Author]().OrderBy("id").
		Relation[bookWithKey]("Books").
		SliceAs[authorWithKeyed](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors[0].Books) != 2 || authors[0].Books[0].AuthorID != authors[0].ID {
		t.Errorf("Lem = %+v, want two books carrying his id", authors[0].Books)
	}

	mu.Lock()
	child := queries[len(queries)-1]
	mu.Unlock()
	if n := strings.Count(child, `"author_id"`); n != 2 { // once selected, once in the WHERE
		t.Errorf("the key should be selected once: %s", child)
	}

	narrow, err := db.Select[Author]().OrderBy("id").
		Relation[bookNarrowKey]("Books").
		SliceAs[authorNarrowKeyed](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(narrow[0].Books) != 2 || int64(narrow[0].Books[0].AuthorID) != narrow[0].ID {
		t.Errorf("Lem = %+v, want two books under an int32 key", narrow[0].Books)
	}
}

// SQL run on a Tx goes into the transaction and rolls back with it, rather than
// landing on the connection outside it.
func TestTxQueriesGoToTheTransaction(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO users (name, email, age, created_at) VALUES ('ghost', 'g@x.io', 1, ?)`,
		time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Select[User]().Where("name = ?", "ghost").Count(ctx); err != nil || n != 0 {
		t.Fatalf("count = %d, err = %v — the write escaped the transaction", n, err)
	}
}

// A hook registered on a transaction begun from a Conn must not outlive it.
func TestConnTxHooksDoNotLeakToTheConn(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	fired := 0
	tx1, err := c.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx1.WithTxHook(barm.TxHook{AfterCommit: func(context.Context, *barm.TxEvent) { fired++ }})
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if fired != 1 {
		t.Fatalf("fired = %d after the first commit, want 1", fired)
	}

	tx2, err := c.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	if fired != 1 {
		t.Fatalf("fired = %d — the first transaction's hook leaked onto the connection", fired)
	}
}

// An empty result is an empty slice, never a nil one — otherwise the answer
// depends on whether a LIMIT happened to preallocate, and marshals as null.
func TestEmptySliceIsNotNil(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	for _, tc := range []struct {
		name string
		q    *barm.SelectQuery[Author]
	}{
		{"no limit", db.Select[Author]().Where("name = ?", "nobody")},
		{"limit", db.Select[Author]().Where("name = ?", "nobody").Limit(10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.q.Slice(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Error("nil slice")
			}
			if len(got) != 0 {
				t.Errorf("got %d rows", len(got))
			}
		})
	}
}

// Same for a parent no child row joined to: an empty relation is empty, not nil.
func TestCountAndExistsKeepTheQueryShape(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	now := time.Now()
	if _, err := db.Insert[User]().Values(
		&User{Name: "ann", Age: 20, CreatedAt: now}, &User{Name: "bo", Age: 30, CreatedAt: now},
		&User{Name: "ann", Age: 50, CreatedAt: now},
	).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	old := db.Select[User]().Where("age > 45")
	for _, tc := range []struct {
		what string
		q    *barm.SelectQuery[User]
		want int64
	}{
		{"groups", db.Select[User]().Column("name").GroupBy("name"), 2},
		{"distinct", db.Select[User]().Column("name").Distinct(), 2},
		{"union", db.Select[User]().Where("age < 35").Union(old), 3},
	} {
		n, err := tc.q.Count(ctx)
		if err != nil || n != tc.want {
			t.Errorf("%s: count = %d, %v, want %d", tc.what, n, err, tc.want)
		}
	}
	if ok, err := db.Select[User]().Where("age > 100").Union(old).Exists(ctx); err != nil || !ok {
		t.Errorf("exists over a union = %v, %v", ok, err)
	}
}

// A pointer row type reads into a fresh value per row, through every terminal.
func TestPointerRows(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := db.Insert[User]().Values(
		&User{Name: "a", Age: 1, CreatedAt: now}, &User{Name: "b", Age: 2, CreatedAt: now},
	).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	all, err := db.Select[*User]().OrderBy("age").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0] == all[1] || all[0].Name != "a" || all[1].Name != "b" || all[1].ID == 0 {
		t.Fatalf("Slice = %+v", all)
	}
	as, err := db.Select[User]().OrderBy("age").SliceAs[*User](ctx)
	if err != nil || len(as) != 2 || as[1].Name != "b" {
		t.Fatalf("SliceAs = %+v, %v", as, err)
	}
	one, err := db.Select[*User]().Where("age = ?", 2).One(ctx)
	if err != nil || one == nil || one.Name != "b" {
		t.Fatalf("One = %+v, %v", one, err)
	}
	var seen []*User
	for u, err := range db.Select[*User]().OrderBy("age").Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, u)
	}
	if len(seen) != 2 || seen[0] == seen[1] || seen[0].Name != "a" {
		t.Fatalf("Seq = %+v", seen)
	}
}

func TestPointerRowsWithRelation(t *testing.T) {
	db := openAuthors(t)
	got, err := db.Select[*Author]().OrderBy("id").Relation[Book]("Books").Slice(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || len(got[0].Books) != 2 || len(got[2].Books) != 0 {
		t.Fatalf("authors = %+v", got)
	}
}

// A query knows the handle it was started on, so code that is handed only the
// query can start another on the same one — inside the same transaction.
func TestQueryKnowsItsHandle(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	for _, tc := range []struct {
		what string
		got  barm.Handle
		want barm.IDB
	}{
		{"db select", db.Select[Author]().IDB(), db},
		{"db raw", db.NewRaw("SELECT 1").IDB(), db},
		{"conn insert", c.Insert[Author]().IDB(), c},
		{"tx update", tx.Update[Author]().IDB(), tx},
		{"tx delete", tx.Delete[Author]().IDB(), tx},
		{"handle over tx", barm.Handle{IDB: tx}.Select[Author]().IDB(), tx},
	} {
		if tc.got.IDB != tc.want {
			t.Errorf("%s: IDB() wraps %T %p, want %T %p", tc.what, tc.got.IDB, tc.got.IDB, tc.want, tc.want)
		}
	}

	// a relation's query is on its parent's handle too
	var inRelation barm.Handle
	if _, err := tx.Select[Author]().Relation("Books", func(q *barm.SelectQuery[Book]) *barm.SelectQuery[Book] {
		inRelation = q.IDB()
		return q
	}).Slice(ctx); err != nil {
		t.Fatal(err)
	}
	if inRelation.IDB != tx {
		t.Errorf("relation IDB() wraps %T, want the transaction", inRelation.IDB)
	}

	// and what starts from it runs in the transaction: it sees the uncommitted row
	if _, err := tx.Insert[Author]().Values(&Author{Name: "Uncommitted"}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	q := tx.Select[Author]()
	n, err := q.IDB().Select[Author]().Where("name = ?", "Uncommitted").Count(ctx)
	if err != nil || n != 1 {
		t.Errorf("count through the query's handle = %d, %v, want 1", n, err)
	}
}

type blobParent struct {
	barm.BaseModel `barm:"table:bp"`

	Key    []byte      `barm:"k"`
	Kids   []blobChild `barm:"rel:k=pk"`
	Values []blobValue `barm:"rel:k=pk"`
}

// blobChild reads the key into the row; blobValue leaves it out.
type blobChild struct {
	barm.BaseModel `barm:"table:bc"`

	PK []byte `barm:"pk"`
	V  string `barm:"v"`
}

type blobValue struct {
	barm.BaseModel `barm:"table:bc"`

	V string `barm:"v"`
}

// A byte-slice key cannot be a map key, which is how relations group rows; it
// groups by its bytes instead, as the database compared it.
func TestRelationBytesKey(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`CREATE TABLE bp (k BLOB)`, `CREATE TABLE bc (pk BLOB, v TEXT)`,
		`INSERT INTO bp VALUES (x'01'), (x'02')`,
		`INSERT INTO bc VALUES (x'01', 'a'), (x'01', 'b'), (x'02', 'c')`,
	} {
		if _, err := db.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Select[blobParent]().OrderBy("k").
		Relation[blobChild]("Kids").
		Relation[blobValue]("Values").
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Kids) != 2 || len(got[1].Kids) != 1 ||
		len(got[0].Values) != 2 || got[1].Values[0].V != "c" {
		t.Errorf("parents = %+v", got)
	}
}

// A prepared parent names its own statement only; the relation's query is a
// different one and runs unnamed.
func TestPreparedParentWithRelation(t *testing.T) {
	ctx := t.Context()
	var prepared []string
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { prepared = append(prepared, ev.Prepared) },
	}))
	for range 2 {
		prepared = nil
		got, err := db.Select[Author]().OrderBy("id").Prepare("authors").Relation[bookLite]("Books").SliceAs[authorLite](ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || len(got[0].Books) != 2 {
			t.Fatalf("authors = %+v", got)
		}
		if !slices.Equal(prepared, []string{"authors", ""}) {
			t.Errorf("prepared names = %q, want the parent's and none", prepared)
		}
	}
	one, err := db.Select[Author]().Where("name = ?", "Lem").Prepare("author").Relation[bookLite]("Books").OneAs[authorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(one.Books) != 2 {
		t.Errorf("one = %+v", one)
	}
}

// A customizer may join another table; the relation's own columns and key are
// qualified with its table, so a column of the same name there is no ambiguity.
func TestRelationCustomizerJoin(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	if _, err := db.Exec(t.Context(), `CREATE TABLE awards (id INTEGER PRIMARY KEY, book_id INTEGER, author_id INTEGER, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[Author]().OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[Book]) *barm.SelectQuery[Book] {
			return q.Join("LEFT JOIN awards ON awards.book_id = books.id").Where("awards.id IS NULL")
		}).
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[0].Books) != 2 || len(got[1].Books) != 1 {
		t.Errorf("authors = %+v", got)
	}
}

type authorPtrBooks struct {
	barm.BaseModel `barm:"table:authors"`

	ID    int64   `barm:"id,pk"`
	Name  string  `barm:"name"`
	Books []*Book `barm:"rel:id=author_id"`
}

func TestRelationSliceOfPointers(t *testing.T) {
	db := openAuthors(t)
	got, err := db.Select[authorPtrBooks]().OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[Book]) *barm.SelectQuery[Book] { return q.OrderBy("id") }).
		Slice(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	lem, mute := got[0], got[2]
	if len(lem.Books) != 2 || lem.Books[0] == nil || lem.Books[0].Title != "Solaris" {
		t.Errorf("lem.Books = %v", lem.Books)
	}
	if mute.Books == nil || len(mute.Books) != 0 {
		t.Errorf("mute.Books = %#v, want empty", mute.Books)
	}
}

// A belongs-to over a nullable foreign key: the parent's key is a pointer, nil
// where there is nothing to belong to.
type emp struct {
	barm.BaseModel `barm:"table:emps"`

	ID      int64       `barm:"id,pk"`
	Name    string      `barm:"name"`
	MgrID   *int64      `barm:"mgr_id"`
	Manager *empName    `barm:"rel:mgr_id=id"`
	Boss    *empKey     `barm:"rel:mgr_id=id"`
	Reports []empReport `barm:"rel:id=mgr_id"`
}

// empName leaves the key out of its projection, empKey reads it into the row.
type empName struct {
	barm.BaseModel `barm:"table:emps"`

	Name string `barm:"name"`
}

type empKey struct {
	barm.BaseModel `barm:"table:emps"`

	ID   int64  `barm:"id"`
	Name string `barm:"name"`
}

// empReport reads its nullable key into the row, from the child's side.
type empReport struct {
	barm.BaseModel `barm:"table:emps"`

	Name  string `barm:"name"`
	MgrID *int64 `barm:"mgr_id"`
}

func TestRelationPointerParentKey(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`CREATE TABLE emps (id INTEGER PRIMARY KEY, name TEXT, mgr_id INTEGER)`,
		`INSERT INTO emps VALUES (1, 'boss', NULL), (2, 'dev', 1)`,
	} {
		if _, err := db.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.Select[emp]().OrderBy("id").
		Relation[empName]("Manager").
		Relation[empKey]("Boss").
		Relation[empReport]("Reports").
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	boss, dev := got[0], got[1]
	if boss.Manager != nil || boss.Boss != nil {
		t.Errorf("boss has a manager: %+v", boss)
	}
	if dev.Manager == nil || dev.Manager.Name != "boss" || dev.Boss == nil || dev.Boss.ID != 1 {
		t.Errorf("dev = %+v", dev)
	}
	if len(boss.Reports) != 1 || boss.Reports[0].Name != "dev" || len(dev.Reports) != 0 {
		t.Errorf("reports: boss %+v, dev %+v", boss.Reports, dev.Reports)
	}

	// no parent has a key at all: nothing to fetch, and nothing fails
	got, err = db.Select[emp]().Where("mgr_id IS NULL").Relation[empName]("Manager").Slice(ctx)
	if err != nil || len(got) != 1 || got[0].Manager != nil {
		t.Errorf("got %+v, err %v", got, err)
	}
}

// A projection that leaves the parent's key out reads it as zero, which would
// match nothing and load every relation empty. It is an error instead.
func TestRelationNeedsParentKeySelected(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	if _, err := db.Select[Author]().Column("name").Relation[Book]("Books").Slice(ctx); err == nil {
		t.Error("expected an error for a projection without the key")
	}
	got, err := db.Select[Author]().Column("id", "name").OrderBy("id").Relation[Book]("Books").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[0].Books) != 2 {
		t.Errorf("lem.Books = %v", got[0].Books)
	}
}

func TestEmptyRelationIsNotNil(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	if _, err := db.Exec(t.Context(), `CREATE TABLE reviews (
		id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, body TEXT)`); err != nil {
		t.Fatal(err)
	}

	authors, err := db.Select[Author]().OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[bookNested]) *barm.SelectQuery[bookNested] {
			return q.Relation[reviewLite]("Reviews")
		}).
		SliceAs[authorNested](ctx)
	if err != nil {
		t.Fatal(err)
	}

	mute := authors[len(authors)-1] // the author with no books
	if mute.Books == nil {
		t.Error("Books is nil")
	}
	if len(mute.Books) != 0 {
		t.Errorf("Mute has %d books", len(mute.Books))
	}
	// no reviews were inserted, so every book is the nested empty case
	for _, b := range authors[0].Books {
		if b.Reviews == nil {
			t.Errorf("%s: Reviews is nil", b.Title)
		}
	}
}

func TestSchemaReachesRelations(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var queries []string
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, ev.Query)
		},
	}))

	mu.Lock()
	queries = nil
	mu.Unlock()

	authors, err := db.Select[Author]().
		Schema("main").
		OrderBy("id").
		Relation[bookLite]("Books").
		SliceAs[authorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 3 || len(authors[0].Books) != 2 {
		t.Fatalf("authors = %+v", authors)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 2 {
		t.Fatalf("queries = %q, want 2", queries)
	}
	for _, q := range queries {
		if !strings.Contains(q, `FROM "main".`) {
			t.Errorf("schema missing from %s", q)
		}
	}
}

// CTEs go to the database, not just through the builder. What sqlite can show
// is that the clause is well formed and selects the right rows — it binds `?`
// positionally and treats RECURSIVE as optional, so neither a body numbered
// from $1 again nor a missing keyword is visible here. Those are in
// pgxdriver's TestCTE, on a dialect that can tell.
func TestCTEAgainstDB(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	t.Run("two bodies", func(t *testing.T) {
		// two CTEs bind in text order, so the second must not restart numbering
		names, err := db.Select[Author]().
			With("named", db.Select[Author]().Column("id", "name").Where("name <> ?", "Mute")).
			With("mailed", db.Select[Author]().Column("id").Where("email LIKE ?", "%x.io")).
			Table("named").
			Where("id IN (SELECT id FROM mailed) AND name <> ?", "Borges").
			Column("name").
			OrderBy("name").
			SliceAs[string](ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(names, []string{"Lem"}) {
			t.Errorf("names = %v, want [Lem]", names)
		}
	})

	t.Run("recursive", func(t *testing.T) {
		// counts 1..5 — the anchor plus a term that reads the CTE being defined
		type n struct {
			N int64 `barm:"n"`
		}
		got, err := db.Select[n]().Table("nums").
			WithRecursive("nums", db.Select[n]().Table("(SELECT 1) AS seed").ColumnExpr("1 AS n").
				UnionAll(db.Select[n]().Table("nums").ColumnExpr("n + 1").Where("n < ?", 5))).
			OrderBy("n").
			SliceAs[int64](ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, []int64{1, 2, 3, 4, 5}) {
			t.Errorf("got %v, want 1..5", got)
		}
	})

	t.Run("writer", func(t *testing.T) {
		// a CTE feeding an INSERT ... SELECT, which is the data-modifying shape
		res, err := db.Insert[Book]().
			With("prolific", db.Select[Author]().Column("id").Where("name = ?", "Lem")).
			Table("books").
			Values(&Book{Title: "Solaris II", AuthorID: 1}).
			Column("title", "author_id").
			Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("inserted %d rows", n)
		}
	})
}

// Nullable columns go through database/sql's own Scanner and Valuer, so barm
// needs no helpers of its own — but nothing covered that until this, and the
// generic sql.Null[T] is the shape most likely to be missed.
type nullRow struct {
	barm.BaseModel `barm:"table:nulls"`

	ID   sql.NullInt64   `barm:"id,pk,autoincrement"`
	Bio  sql.NullString  `barm:"bio"`
	Age  sql.NullInt64   `barm:"age"`
	Rate sql.Null[int64] `barm:"rate"`
	Ptr  *string         `barm:"ptr"`
}

func TestNullable(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/n.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(t.Context(), `CREATE TABLE nulls (id INTEGER PRIMARY KEY AUTOINCREMENT,
		bio TEXT, age INTEGER, rate INTEGER, ptr TEXT)`); err != nil {
		t.Fatal(err)
	}

	s := "here"
	unset := &nullRow{}
	set := &nullRow{
		Bio:  sql.NullString{String: "hi", Valid: true},
		Age:  sql.NullInt64{Int64: 7, Valid: true},
		Rate: sql.Null[int64]{V: 9, Valid: true},
		Ptr:  &s,
	}
	if _, err := db.Insert[nullRow]().Values(unset, set).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := db.Select[nullRow]().OrderBy("id").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows", len(got))
	}
	for _, f := range []struct {
		name string
		ok   bool
	}{
		{"bio", got[0].Bio.Valid}, {"age", got[0].Age.Valid}, {"rate", got[0].Rate.Valid},
	} {
		if f.ok {
			t.Errorf("%s came back valid, want NULL", f.name)
		}
	}
	if got[0].Ptr != nil {
		t.Errorf("ptr = %q, want nil", *got[0].Ptr)
	}
	if got[1].Bio.String != "hi" || got[1].Age.Int64 != 7 || got[1].Rate.V != 9 {
		t.Errorf("row = %+v", got[1])
	}
	if got[1].Ptr == nil || *got[1].Ptr != "here" {
		t.Errorf("ptr = %v", got[1].Ptr)
	}

	// a nullable auto key still gets the generated id written back
	one := &nullRow{Bio: sql.NullString{String: "solo", Valid: true}}
	if _, err := db.Insert[nullRow]().Values(one).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if !one.ID.Valid || one.ID.Int64 == 0 {
		t.Errorf("pk after insert = %+v, want a generated id", one.ID)
	}

	// scanned as a result type of its own, and bound as an argument
	bios, err := db.Select[nullRow]().Column("bio").Where("bio IS NOT NULL").
		OrderBy("id").SliceAs[sql.NullString](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(bios) != 2 || !bios[0].Valid || bios[0].String != "hi" {
		t.Errorf("bios = %+v", bios)
	}
	n, err := db.Select[nullRow]().Where("bio = ?", sql.NullString{String: "hi", Valid: true}).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("count by NullString arg = %d, want 1", n)
	}
}

// Every builder built on the DB runs where Via sends it, and the one it was
// built from stays on the DB. A query left on the DB would miss the
// transaction's hook, and its write would survive the rollback.
func TestVia(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	u := &User{Name: "via", Email: "v@x.io", Age: 1, CreatedAt: time.Now()}
	ins := db.Insert[User]().Values(u)
	count := db.Select[User]()
	aged := db.Select[User]().Where("age = ?", 5)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var ran []string
	tx.WithHook(barm.QueryHook{AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
		ran = append(ran, ev.Op)
	}})

	if _, err := ins.Via(tx).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Update[User]().Set("age = ?", 5).Where("id = ?", u.ID).Via(tx).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := aged.Via(tx).Count(ctx); n != 1 || err != nil {
		t.Fatalf("aged = %d, %v, want 1", n, err)
	}
	if _, err := db.NewRaw("UPDATE users SET age = 6").Via(tx).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Delete[User]().Where("age = ?", 6).Via(tx).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := count.Via(tx).Count(ctx); n != 0 || err != nil {
		t.Fatalf("count in tx = %d, %v, want 0", n, err)
	}
	want := []string{"INSERT", "UPDATE", "SELECT", "UPDATE", "DELETE", "SELECT"}
	if !slices.Equal(ran, want) {
		t.Errorf("tx ran %v, want %v", ran, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := ins.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := count.Count(ctx); n != 1 || err != nil {
		t.Errorf("count on db = %d, %v, want the one insert run there", n, err)
	}
}

// One query built once, run from many goroutines on connections of their own,
// relation loads included.
func TestViaConcurrent(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)
	q := db.Select[Author]().OrderBy("id").Relation[bookLite]("Books")

	var wg sync.WaitGroup
	for range 8 {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		var ran int
		c.WithHook(barm.QueryHook{AfterQuery: func(context.Context, *barm.QueryEvent) { ran++ }})
		wg.Go(func() {
			for range 5 {
				authors, err := q.Via(c).SliceAs[authorLite](ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if len(authors) != 3 || len(authors[0].Books) != 2 || len(authors[1].Books) != 1 {
					t.Errorf("authors = %+v", authors)
					return
				}
			}
			if ran != 10 {
				t.Errorf("conn ran %d queries, want 10: a parent and a relation load each time", ran)
			}
		})
	}
	wg.Wait()
}

// Subqueries passed as arguments run as part of the query around them, with
// the arguments on both sides bound where they belong.
func TestSubqueryArgumentRuns(t *testing.T) {
	ctx := t.Context()
	db := openAuthors(t)

	type withCount struct {
		Name  string `barm:"name"`
		Books int    `barm:"books"`
	}
	rows, err := db.Select[Author]().
		Column("name").
		ColumnExpr("(?) AS books", db.NewRaw("SELECT count(*) FROM books WHERE books.author_id = a.id AND title <> ?", "")).
		Where("name <> ? AND id IN (?)", "Nobody", db.Select[Book]().Column("author_id").Where("title LIKE ?", "%i%")).
		OrderBy("name").
		SliceAs[withCount](ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []withCount{{"Borges", 1}, {"Lem", 2}}
	if !slices.Equal(rows, want) {
		t.Errorf("rows = %+v, want %+v", rows, want)
	}
}

// inTx is the helper a service writes once over IDB: it begins on whatever
// it is handed, so the same code opens a transaction on a DB or a connection
// and a savepoint inside a transaction.
func inTx(ctx context.Context, h barm.IDB, fn func(*barm.Tx) error) error {
	tx, err := h.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = fn(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func TestBeginOnIDB(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	insert := func(name string) func(*barm.Tx) error {
		return func(tx *barm.Tx) error {
			_, err := tx.Insert[User]().Values(&User{Name: name, Email: name, CreatedAt: time.Now()}).Exec(ctx)
			return err
		}
	}
	failAfter := func(name string) func(*barm.Tx) error {
		return func(tx *barm.Tx) error {
			if err := insert(name)(tx); err != nil {
				return err
			}
			return errors.New("undo")
		}
	}

	// on the DB: a transaction of its own, committed
	if err := inTx(ctx, db, insert("pool")); err != nil {
		t.Fatal(err)
	}

	// inside a transaction: a savepoint, so a failure undoes only its own work
	err := inTx(ctx, db, func(tx *barm.Tx) error {
		if err := inTx(ctx, tx, failAfter("undone")); err == nil {
			t.Error("the inner failure should come back")
		}
		return inTx(ctx, barm.Handle{IDB: tx}, insert("nested"))
	})
	if err != nil {
		t.Fatal(err)
	}

	// on a held connection: the transaction borrows it and leaves it held
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := inTx(ctx, c, insert("conn")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "SELECT 1"); err != nil {
		t.Errorf("the connection should still be held: %v", err)
	}

	names, err := db.Select[User]().Column("name").OrderBy("id").SliceAs[string](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"pool", "nested", "conn"}; !slices.Equal(names, want) {
		t.Errorf("names = %v, want %v", names, want)
	}
}

type nzRow struct {
	barm.BaseModel `barm:"table:nz"`

	ID    int64     `barm:"id,pk,autoincrement"`
	Name  string    `barm:"name,nullzero"`
	Seen  time.Time `barm:"seen,nullzero"`
	Count int       `barm:"count,nullzero"`
	Rate  float64   `barm:"rate,nullzero"`
	Blob  []byte    `barm:"blob,nullzero"`
}

type nzParent struct {
	barm.BaseModel `barm:"table:nz_parent"`

	ID   int64   `barm:"id,pk"`
	Rows []nzRow `barm:"rel:id=count"`
}

// A nullzero field writes its zero as NULL, and reads NULL back as its zero,
// down every path that scans.
func TestNullZero(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/nz.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		`CREATE TABLE nz (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, seen TIMESTAMP, count INTEGER, rate REAL, blob BLOB)`,
		`CREATE TABLE nz_parent (id INTEGER PRIMARY KEY)`,
		`INSERT INTO nz_parent (id) VALUES (7)`,
	} {
		if _, err := db.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	when := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	empty, full := &nzRow{}, &nzRow{Name: "n", Seen: when, Count: 7, Rate: 1.5, Blob: []byte("b")}
	if _, err := db.Insert[nzRow]().Values(empty).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert[nzRow]().Values(full).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	var nulls int
	err = queryRow(ctx, db, `SELECT count(*) FROM nz WHERE name IS NULL AND seen IS NULL AND count IS NULL AND rate IS NULL AND blob IS NULL`).Scan(&nulls)
	if err != nil || nulls != 1 {
		t.Fatalf("rows written all NULL = %d, %v, want 1", nulls, err)
	}

	check := func(what string, got nzRow, want *nzRow) {
		t.Helper()
		if got.Name != want.Name || !got.Seen.Equal(want.Seen) || got.Count != want.Count || got.Rate != want.Rate || string(got.Blob) != string(want.Blob) {
			t.Errorf("%s: got %+v, want %+v", what, got, *want)
		}
	}
	rows, err := db.Select[nzRow]().OrderBy("id").Slice(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("slice: %v, %v", rows, err)
	}
	check("slice NULL", rows[0], empty)
	check("slice set", rows[1], full)

	one, err := db.Select[nzRow]().Where("id = ?", empty.ID).One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check("one", one, empty)

	for row, err := range db.Select[nzRow]().Where("id = ?", empty.ID).Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		check("seq", row, empty)
	}

	back := &nzRow{}
	if _, err := db.Insert[nzRow]().Values(back).Returning("*").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	check("returning", *back, empty)

	// zeroed by an update, then read as a relation's rows
	full.Name, full.Seen, full.Rate, full.Blob = "", time.Time{}, 0, nil
	if _, err := db.Update[nzRow]().Value(full).WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	parents, err := db.Select[nzParent]().Relation[nzRow]("Rows").Slice(ctx)
	if err != nil || len(parents) != 1 || len(parents[0].Rows) != 1 {
		t.Fatalf("relation: %+v, %v", parents, err)
	}
	check("relation", parents[0].Rows[0], &nzRow{Count: 7})
}

type jsonMeta struct {
	Tags  []string `json:"tags"`
	Score int      `json:"score"`
}

type jsonRow struct {
	barm.BaseModel `barm:"table:js"`

	ID     int64          `barm:"id,pk,autoincrement"`
	Meta   jsonMeta       `barm:"meta,json"`
	Opt    *jsonMeta      `barm:"opt,json"`
	Attrs  map[string]int `barm:"attrs,json"`
	List   []int          `barm:"list,json"`
	Zeroed jsonMeta       `barm:"zeroed,json,nullzero"`
}

// A json field goes in encoded and comes back decoded; nil and nullzero-zero
// go in as NULL, not as the literal null, and NULL comes back as the zero.
func TestJSONColumns(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/js.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE js (id INTEGER PRIMARY KEY AUTOINCREMENT, meta TEXT, opt TEXT, attrs TEXT, list TEXT, zeroed TEXT)`); err != nil {
		t.Fatal(err)
	}

	full := &jsonRow{
		Meta: jsonMeta{Tags: []string{"a"}, Score: 3}, Opt: &jsonMeta{Score: 1},
		Attrs: map[string]int{"x": 1}, List: []int{1, 2}, Zeroed: jsonMeta{Score: 9},
	}
	bare := &jsonRow{}
	if _, err := db.Insert[jsonRow]().Values(full).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert[jsonRow]().Values(bare).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	var meta, opt, attrs, list string
	var zeroed sql.NullString
	err = queryRow(ctx, db, `SELECT meta, opt, attrs, list, zeroed FROM js WHERE id = ?`, full.ID).Scan(&meta, &opt, &attrs, &list, &zeroed)
	if err != nil || meta != `{"tags":["a"],"score":3}` || opt != `{"tags":null,"score":1}` || attrs != `{"x":1}` || list != `[1,2]` || zeroed.String != `{"tags":null,"score":9}` {
		t.Fatalf("stored %s %s %s %s %v, %v", meta, opt, attrs, list, zeroed, err)
	}
	var nulls int
	err = queryRow(ctx, db, `SELECT count(*) FROM js WHERE id = ? AND opt IS NULL AND attrs IS NULL AND list IS NULL AND zeroed IS NULL`, bare.ID).Scan(&nulls)
	if err != nil || nulls != 1 {
		t.Fatalf("nil and nullzero stored as NULL = %d, %v, want 1", nulls, err)
	}

	same := func(what string, got, want jsonRow) {
		t.Helper()
		g, _ := json.Marshal([]any{got.Meta, got.Opt, got.Attrs, got.List, got.Zeroed})
		w, _ := json.Marshal([]any{want.Meta, want.Opt, want.Attrs, want.List, want.Zeroed})
		if string(g) != string(w) {
			t.Errorf("%s: got %s, want %s", what, g, w)
		}
	}
	rows, err := db.Select[jsonRow]().OrderBy("id").Slice(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("slice: %v, %v", rows, err)
	}
	same("slice", rows[0], *full)
	same("slice NULL", rows[1], *bare)
	one, err := db.Select[jsonRow]().Where("id = ?", full.ID).One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	same("one", one, *full)

	// through an update's RETURNING
	full.Attrs = map[string]int{"y": 2}
	back, err := db.Update[jsonRow]().Value(full).WherePK().One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Attrs) != 1 || back.Attrs["y"] != 2 {
		t.Errorf("update returned attrs %v, want only y", back.Attrs)
	}

	type bad struct {
		barm.BaseModel `barm:"table:js"`
		Meta           chan int `barm:"meta,json"`
	}
	if _, _, err := db.Insert[bad]().Values(&bad{Meta: make(chan int)}).Build(); err == nil {
		t.Error("a value json cannot encode should fail the build")
	}
}

type soFolder struct {
	barm.BaseModel `barm:"table:so_folders,alias:f"`

	ID     int64     `barm:"id,pk"`
	Name   string    `barm:"name"`
	Count  int       `barm:"tcase_count,scanonly"`
	Titles []string  `barm:"titles,json,scanonly"`
	Cases  []soTcase `barm:"rel:id=folder_id"`
}

type soTcase struct {
	barm.BaseModel `barm:"table:so_tcases,alias:t"`

	ID       int64  `barm:"id,pk"`
	FolderID int64  `barm:"folder_id"`
	Title    string `barm:"title"`
	Upper    string `barm:"upper,scanonly"`
}

// scanonly fields filled by a raw column, through Slice, One and a relation's
// query, and left alone by a query that does not ask for them.
func TestScanOnlyRuns(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/so.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		`CREATE TABLE so_folders (id INTEGER PRIMARY KEY, name TEXT)`,
		`CREATE TABLE so_tcases (id INTEGER PRIMARY KEY, folder_id INTEGER, title TEXT)`,
		`INSERT INTO so_tcases (id, folder_id, title) VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')`,
	} {
		if _, err := db.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	// inserted through barm, so the scanonly fields must stay out of it
	for _, f := range []*soFolder{{ID: 1, Name: "one", Count: 99}, {ID: 2, Name: "two"}} {
		if _, err := db.Insert[soFolder]().Values(f).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}

	q := db.Select[soFolder]().
		ColumnExpr("f.*").
		ColumnExpr("(?) AS tcase_count", db.NewRaw("SELECT count(*) FROM so_tcases WHERE folder_id = f.id")).
		ColumnExpr("(?) AS titles", db.NewRaw("SELECT json_group_array(title) FROM so_tcases WHERE folder_id = f.id AND title <> ?", "")).
		OrderBy("id")
	rows, err := q.Slice(ctx)
	if err != nil || len(rows) != 2 || rows[0].Count != 2 || rows[1].Count != 1 || !slices.Equal(rows[0].Titles, []string{"a", "b"}) {
		t.Fatalf("slice: %+v, %v", rows, err)
	}
	one, err := q.Clone().Where("id = ?", 2).One(ctx)
	if err != nil || one.Count != 1 || !slices.Equal(one.Titles, []string{"c"}) {
		t.Errorf("one: %+v, %v", one, err)
	}
	plain, err := db.Select[soFolder]().Where("id = ?", 1).One(ctx)
	if err != nil || plain.Count != 0 || plain.Titles != nil {
		t.Errorf("without the column: %+v, %v", plain, err)
	}

	withCases, err := db.Select[soFolder]().OrderBy("id").
		Relation("Cases", func(q *barm.SelectQuery[soTcase]) *barm.SelectQuery[soTcase] {
			return q.ColumnExpr("t.*").ColumnExpr("upper(title) AS upper").OrderBy("id")
		}).Slice(ctx)
	if err != nil || len(withCases) != 2 || len(withCases[0].Cases) != 2 || withCases[0].Cases[1].Upper != "B" {
		t.Errorf("relation: %+v, %v", withCases, err)
	}
}

type suTenant struct {
	barm.BaseModel `barm:"table:su_tenants"`

	ID    int64  `barm:"id,pk"`
	Name  string `barm:"name"`
	Users int    `barm:"total_users,skipupdate"`
}

// Saving a struct whose skipupdate column went stale does not undo what
// something else wrote there.
func TestSkipUpdateRuns(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/su.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(t.Context(), `CREATE TABLE su_tenants (id INTEGER PRIMARY KEY, name TEXT, total_users INTEGER)`); err != nil {
		t.Fatal(err)
	}
	v := &suTenant{ID: 1, Name: "acme", Users: 3}
	if _, err := db.Insert[suTenant]().Values(v).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(t.Context(), `UPDATE su_tenants SET total_users = 10`); err != nil { // the counter moves elsewhere
		t.Fatal(err)
	}
	v.Name = "acme inc" // v.Users is still 3
	if _, err := db.Update[suTenant]().Value(v).WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Select[suTenant]().One(ctx)
	if err != nil || got.Name != "acme inc" || got.Users != 10 {
		t.Errorf("got %+v, %v, want the new name and the counter left at 10", got, err)
	}
}

// openSQL opens a database/sql database and runs barm on it.
func openSQL(driver, dsn string, d barm.Dialect, opts ...barm.Option) (*barm.DB, error) {
	sqldb, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return barm.New(barm.SQL(sqldb), d, opts...), nil
}

// row is the first row of a raw query, scanned positionally.
type row struct {
	rows barm.Rows
	err  error
}

func queryRow(ctx context.Context, h barm.IDB, query string, args ...any) row {
	rows, err := h.Query(ctx, query, args...)
	return row{rows, err}
}

func (r row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

// A hook that masks a value in place changes what it reports, not what is
// written.
func TestHookArgsAreACopy(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("sqlite", "file:"+t.TempDir()+"/mask.db", barm.SQLite, barm.WithHook(barm.QueryHook{
		BeforeQuery: func(c context.Context, ev *barm.QueryEvent) context.Context {
			for i, a := range ev.Args {
				if a == "hunter2" {
					ev.Args[i] = "***"
				}
			}
			return c
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert[User]().Values(&User{Name: "hunter2", CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	u, err := db.Select[User]().One(ctx)
	if err != nil || u.Name != "hunter2" {
		t.Errorf("stored %q, %v, want what was bound, not what the hook showed", u.Name, err)
	}
}

// On a driver that cannot batch, relations run one query at a time, and the
// hooks see each once, as it ran — not a failed batch first.
func TestRelationHooksWithoutBatching(t *testing.T) {
	ctx := t.Context()
	var errs []error
	db := openAuthors(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { errs = append(errs, ev.Err) },
	}))
	if _, err := db.Exec(ctx, `CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, label TEXT)`); err != nil {
		t.Fatal(err)
	}
	errs = nil
	if _, err := db.Select[Author]().Relation[Book]("Books").Relation[tagRow]("Tags").Slice(ctx); err != nil {
		t.Fatal(err)
	}
	if len(errs) != 3 || slices.ContainsFunc(errs, func(e error) bool { return e != nil }) {
		t.Errorf("hooks saw %v, want three queries that ran fine", errs)
	}
}

type blobKey []byte

type namedKeyOwner struct {
	barm.BaseModel `barm:"table:nk_owners"`

	Key   blobKey         `barm:"k,pk"`
	Items []namedKeyChild `barm:"rel:k=owner"`
}

type namedKeyChild struct {
	barm.BaseModel `barm:"table:nk_children"`

	Owner blobKey `barm:"owner"`
	Name  string  `barm:"name"`
}

// A relation keyed by a named byte slice groups its rows like a plain []byte
// one, rather than panicking on a key a map cannot hold.
func TestRelationNamedByteKey(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`CREATE TABLE nk_owners (k BLOB PRIMARY KEY)`,
		`CREATE TABLE nk_children (owner BLOB, name TEXT)`,
		`INSERT INTO nk_owners VALUES (x'01'), (x'02')`,
		`INSERT INTO nk_children VALUES (x'01', 'a'), (x'01', 'b'), (x'02', 'c')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	owners, err := db.Select[namedKeyOwner]().OrderBy("k").Relation[namedKeyChild]("Items").Slice(ctx)
	if err != nil || len(owners) != 2 || len(owners[0].Items) != 2 || len(owners[1].Items) != 1 {
		t.Fatalf("owners = %+v, %v", owners, err)
	}
}

// Two DBs on one pool share its prepared statements, so one name meaning two
// queries is an error rather than one DB running the other's SQL.
func TestPreparedNameAcrossDBsOnOnePool(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, age := range []int{10, 90} {
		if _, err := db.Insert[User]().Values(&User{Name: "u", Age: age, CreatedAt: time.Now()}).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	other := barm.New(db.Pool(), barm.SQLite)
	if _, err := db.Select[User]().Where("age < ?", 50).Prepare("shared").Slice(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := other.Select[User]().Where("age > ?", 50).Prepare("shared").Slice(ctx)
	if err == nil {
		t.Errorf("ran the other DB's statement: %+v", rows)
	}
}

// A sequential batch on the pool that fails inside a transaction it began does
// not hand the next caller a connection still in it.
func TestFailedPoolBatchDropsItsConnection(t *testing.T) {
	ctx := t.Context()
	sqldb, err := sql.Open("sqlite", "file:"+t.TempDir()+"/fail.db")
	if err != nil {
		t.Fatal(err)
	}
	sqldb.SetMaxOpenConns(1)
	db := barm.New(barm.SQL(sqldb, barm.SequentialBatches()), barm.SQLite)
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	b := db.Batch()
	b.Begin()
	b.Exec(db.Insert[User]().Values(&User{Name: "half", CreatedAt: time.Now()}))
	b.Exec(db.NewRaw("INSERT INTO nope VALUES (1)"))
	b.Commit()
	if err := b.Run(ctx); err == nil {
		t.Fatal("the batch should fail")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("the next transaction: %v", err)
	}
	defer tx.Rollback()
	if n, err := tx.Select[User]().Count(ctx); err != nil || n != 0 {
		t.Errorf("count = %d, %v, want the failed batch's insert gone", n, err)
	}
}

// A rolled-back nested transaction releases its savepoint, so failing ones in
// a loop do not pile up, and the deferred Rollback after it is a no-op.
func TestNestedRollbackReleases(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	inner, err := tx.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := inner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := inner.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("second rollback = %v, want ErrTxDone", err)
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT barm_sp_1"); err == nil {
		t.Error("the savepoint is still open")
	}
}

// countingDriver wraps sqlite's driver and counts the statements it prepares.
type countingDriver struct {
	inner    driver.Driver
	prepares *atomic.Int64
}

func (d countingDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return countingConn{c, d.prepares}, nil
}

type countingConn struct {
	driver.Conn
	prepares *atomic.Int64
}

func (c countingConn) Prepare(query string) (driver.Stmt, error) {
	c.prepares.Add(1)
	return c.Conn.Prepare(query)
}

func (c countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.prepares.Add(1)
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c countingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c countingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

var countingPrepares atomic.Int64

func init() {
	sqldb, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	sql.Register("countingsqlite", countingDriver{sqldb.Driver(), &countingPrepares})
	sqldb.Close()
}

// A transaction begun on a held connection binds each named statement once,
// rather than having database/sql prepare it again on every run.
func TestSQLTxPreparesOncePerName(t *testing.T) {
	ctx := t.Context()
	db, err := openSQL("countingsqlite", "file:"+t.TempDir()+"/count.db", barm.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	before := countingPrepares.Load()
	for range 20 {
		if _, err := tx.Select[User]().Where("age > ?", 1).Prepare("per_tx").Slice(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := countingPrepares.Load() - before; n > 2 {
		t.Errorf("%d prepares for one name in one transaction, want at most 2", n)
	}
}

// A sequential batch runs a named query prepared: once per batch on the pool,
// whose connection it only borrows, and once for good on a held connection.
func TestSQLBatchPreparesNamed(t *testing.T) {
	ctx := t.Context()
	sqldb, err := sql.Open("countingsqlite", "file:"+t.TempDir()+"/batch.db")
	if err != nil {
		t.Fatal(err)
	}
	db := barm.New(barm.SQL(sqldb, barm.SequentialBatches()), barm.SQLite)
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users (name, email, age, created_at) VALUES ('ann', 'a', 20, CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, tc := range []struct {
		name     string
		batch    func() *barm.Batch
		sel      func() *barm.SelectQuery[User]
		prepares [2]int64
	}{
		{"pool", db.Batch, db.Select[User], [2]int64{1, 1}},
		{"held connection", c.Batch, c.Select[User], [2]int64{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for run, want := range tc.prepares {
				before := countingPrepares.Load()
				b := tc.batch()
				rs := make([]*barm.BatchResult[User], 5)
				for i := range rs {
					rs[i] = b.One(tc.sel().Where("age >= ?", 10+i).Prepare("seq_" + tc.name))
				}
				if err := b.Run(ctx); err != nil {
					t.Fatal(err)
				}
				if rs[4].Value().Name != "ann" {
					t.Fatalf("run %d: got %+v", run, rs[4].Value())
				}
				if got := countingPrepares.Load() - before; got != want {
					t.Errorf("run %d: %d prepares, want %d", run, got, want)
				}
			}
		})
	}
}
