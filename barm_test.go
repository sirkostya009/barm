package barm_test

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

type User struct {
	barm.BaseModel `barm:"table:users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

func TestBuildSelect(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, args, err := db.Select[User]().
		Where("u.age >= ?", 18).
		WhereOr("u.name = ?", "root").
		OrderBy("u.id DESC").
		Limit(10).
		Build()
	if err != nil {
		t.Fatal(err)
	}

	want := `SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" ` +
		`FROM "users" AS "u" WHERE (u.age >= $1) OR (u.name = $2) ORDER BY u.id DESC LIMIT 10`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 2 || args[0] != 18 || args[1] != "root" {
		t.Errorf("args = %v", args)
	}
}

// Among several conditions each is wrapped, a multi-line one included, so the
// AND joining them cannot bind tighter than an OR inside one.
func TestMultilineConditionIsWrapped(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, _, err := db.Delete[User]().Where("name = ?", "x").Where(`email = ?
		OR email = ?`, "a", "b").Build()
	if err != nil {
		t.Fatal(err)
	}
	want := "DELETE FROM \"users\" WHERE (name = $1) AND (email = $2\n\t\tOR email = $3)"
	if q != want {
		t.Errorf("got  %q\nwant %q", q, want)
	}
}

func TestBuildInsertMySQLPlaceholders(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.MySQL)

	q, args, err := db.Insert[User]().
		Values(&User{Name: "a"}, &User{Name: "b"}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	want := "INSERT INTO `users` (`name`, `email`, `age`, `created_at`) VALUES (?, ?, ?, ?), (?, ?, ?, ?)"
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 8 {
		t.Errorf("args = %v", args)
	}
}

func TestBuildUpdateUsesPK(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, args, err := db.Update[User]().Value(&User{ID: 7, Name: "x"}).Column("name").WherePK().Build()
	if err != nil {
		t.Fatal(err)
	}
	want := `UPDATE "users" SET "name" = $1 WHERE "id" = $2`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if args[1] != int64(7) {
		t.Errorf("args = %v", args)
	}
}

func TestBuildByCompositePK(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	type member struct {
		barm.BaseModel `barm:"table:members"`

		Org  int64  `barm:"org,pk"`
		User int64  `barm:"user,pk"`
		Role string `barm:"role"`
	}
	m := &member{Org: 1, User: 2, Role: "admin"}

	q, args, err := db.Update[member]().Value(m).WherePK().Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := `UPDATE "members" SET "role" = $1 WHERE "org" = $2 AND "user" = $3`; q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if !slices.Equal(args, []any{"admin", int64(1), int64(2)}) {
		t.Errorf("args = %v", args)
	}

	q, args, err = db.Delete[member]().Value(m).WherePK().Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := `DELETE FROM "members" WHERE "org" = $1 AND "user" = $2`; q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if !slices.Equal(args, []any{int64(1), int64(2)}) {
		t.Errorf("args = %v", args)
	}
}

// The primary key is a condition only when asked for, and sits among the others
// where it was asked for: a Value on its own picks no rows.
func TestWherePK(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	u := &User{ID: 7, Name: "x", Age: 3}

	q, args, err := db.Delete[User]().Value(u).WherePK().Where("age = ?", 3).Build()
	if err != nil || q != `DELETE FROM "users" WHERE "id" = $1 AND (age = $2)` || !slices.Equal(args, []any{int64(7), 3}) {
		t.Errorf("delete: %s %v, %v", q, args, err)
	}
	// asked for before the value is bound: the key is read when the query is built
	q, _, err = db.Update[User]().WherePK().Column("name").Value(u).Build()
	if err != nil || q != `UPDATE "users" SET "name" = $1 WHERE "id" = $2` {
		t.Errorf("update: %s, %v", q, err)
	}

	type member struct {
		barm.BaseModel `barm:"table:members"`

		Org  int64 `barm:"org,pk"`
		User int64 `barm:"user,pk"`
	}
	q, _, err = db.Delete[member]().Value(&member{Org: 1, User: 2}).WherePK().WhereOr("expired").Build()
	if err != nil || q != `DELETE FROM "members" WHERE ("org" = $1 AND "user" = $2) OR (expired)` {
		t.Errorf("composite: %s, %v", q, err)
	}

	for what, q := range map[string]barm.Query{
		"value alone":       db.Delete[User]().Value(u),
		"update value only": db.Update[User]().Value(u),
		"no value":          db.Delete[User]().WherePK(),
		"no key": db.Delete[struct {
			barm.BaseModel `barm:"table:t"`
			A              int `barm:"a"`
		}]().Value(&struct {
			barm.BaseModel `barm:"table:t"`
			A              int `barm:"a"`
		}{}).WherePK(),
	} {
		_, _, err := q.Build()
		if err == nil {
			t.Errorf("%s: expected an error", what)
		}
	}
}

type acctRow struct {
	barm.BaseModel `barm:"table:accts"`

	ID    int64  `barm:"id,pk,autoincrement"`
	Email string `barm:"email"`
	Name  string `barm:"name"`
}

// Set adds assignments to an ON clause that updates, bun's way: after SET where
// the clause ends in DO UPDATE, straight after it on MySQL's ON DUPLICATE KEY
// UPDATE.
func TestOnSet(t *testing.T) {
	t.Parallel()
	a := &acctRow{Email: "a@x", Name: "a"}

	q, args, err := barm.NewBuilder(barm.Postgres).Insert[acctRow]().Values(a).
		On("CONFLICT (email) DO UPDATE").Set("name = excluded.name").Set("name = name || ?", "!").Build()
	want := `INSERT INTO "accts" ("email", "name") VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = excluded.name, name = name || $3`
	if err != nil || q != want || len(args) != 3 {
		t.Errorf("postgres: %s %v, %v", q, args, err)
	}
	q, _, err = barm.NewBuilder(barm.MySQL).Insert[acctRow]().Values(a).
		On("DUPLICATE KEY UPDATE").Set("name = VALUES(name)").Build()
	if err != nil || !strings.HasSuffix(q, " ON DUPLICATE KEY UPDATE name = VALUES(name)") {
		t.Errorf("mysql: %s, %v", q, err)
	}
	_, _, err = barm.NewBuilder(barm.Postgres).Insert[acctRow]().Values(a).Set("name = 1").Build()
	if err == nil {
		t.Error("expected an error for Set without On")
	}

	// a clone sets its own
	base := barm.NewBuilder(barm.Postgres).Insert[acctRow]().Values(a).On("CONFLICT (email) DO UPDATE").Set("name = excluded.name")
	x, y := base.Clone().Set("x = 1"), base.Clone().Set("y = 1")
	qx, _, _ := x.Build()
	qy, _, _ := y.Build()
	if !strings.HasSuffix(qx, "SET name = excluded.name, x = 1") || !strings.HasSuffix(qy, "SET name = excluded.name, y = 1") {
		t.Errorf("clones: %s / %s", qx, qy)
	}
}

func TestUpdateRequiresWhere(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	_, _, err := db.Update[User]().Set("age = 1").Build()
	if err == nil {
		t.Fatal("expected an error for an unconditional UPDATE")
	}
}

func TestExistsSQL(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, args, err := db.Select[User]().Where("age > ?", 10).OrderBy("id").ExistsQuery()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT EXISTS (SELECT 1 FROM "users" AS "u" WHERE age > $1 LIMIT 1)`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 1 || args[0] != 10 {
		t.Errorf("args = %v", args)
	}
}

// Builders change in place, so branching one takes Clone. Each base carries
// three clauses, which leaves its slice spare capacity: two branches appending
// without a clone of their own would write the same slot.
func TestCloneBranches(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	check := func(what string, q barm.Query, suffix string) {
		t.Helper()
		sql, _, err := q.Build()
		if err != nil || !strings.HasSuffix(sql, suffix) {
			t.Errorf("%s: %s, %v — want suffix %q", what, sql, err, suffix)
		}
	}

	sel := db.Select[User]().Where("a").Where("b").Where("c")
	sx, sy := sel.Clone().Where("x"), sel.Clone().Where("y")
	check("select x", sx, "WHERE (a) AND (b) AND (c) AND (x)")
	check("select y", sy, "WHERE (a) AND (b) AND (c) AND (y)")
	check("select base", sel, "WHERE (a) AND (b) AND (c)")

	upd := db.Update[User]().Set("a = 1").Set("b = 2").Set("c = 3").Where("id = 1")
	ux, uy := upd.Clone().Set("x = 1"), upd.Clone().Set("y = 1")
	check("update x", ux, "c = 3, x = 1 WHERE id = 1")
	check("update y", uy, "c = 3, y = 1 WHERE id = 1")

	del := db.Delete[User]().Where("a").Where("b").Where("c")
	dx, dy := del.Clone().Where("x"), del.Clone().Where("y")
	check("delete x", dx, "WHERE (a) AND (b) AND (c) AND (x)")
	check("delete y", dy, "WHERE (a) AND (b) AND (c) AND (y)")

	ins := db.Insert[User]().Values(&User{Name: "a"}, &User{Name: "b"}, &User{Name: "c"})
	ix, iy := ins.Clone().Values(&User{Name: "x"}), ins.Clone().Values(&User{Name: "y"})
	_, xargs, _ := ix.Build()
	_, yargs, _ := iy.Build()
	if len(xargs) != 16 || xargs[12] != "x" || len(yargs) != 16 || yargs[12] != "y" {
		t.Errorf("insert branches: %v / %v", xargs, yargs)
	}
}

// Queuing a query in a batch reads it; it does not rewrite it.
func TestBatchOneLeavesQueryAlone(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	q := db.Select[User]()
	db.Batch().One(q)
	sql, _, err := q.Build()
	if err != nil || strings.Contains(sql, "LIMIT") {
		t.Errorf("after Batch.One: %s, %v", sql, err)
	}
}

// sqlite and MySQL take an OFFSET only as part of a LIMIT clause, so an offset
// on its own comes with the LIMIT that means none there.
func TestOffsetWithoutLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		d    barm.Dialect
		want string
	}{
		{barm.Postgres, ` FROM "users" AS "u" OFFSET 10`},
		{barm.SQLite, ` FROM "users" AS "u" LIMIT -1 OFFSET 10`},
		{barm.MySQL, " FROM `users` AS `u` LIMIT 18446744073709551615 OFFSET 10"},
	} {
		q, _, err := barm.NewBuilder(tc.d).Select[User]().Offset(10).Build()
		if err != nil || !strings.HasSuffix(q, tc.want) {
			t.Errorf("%s: %s, %v — want suffix %q", tc.d.Name(), q, err, tc.want)
		}
		// with a limit of its own nothing changes
		q, _, _ = barm.NewBuilder(tc.d).Select[User]().Limit(5).Offset(10).Build()
		if !strings.HasSuffix(q, " LIMIT 5 OFFSET 10") {
			t.Errorf("%s: %s", tc.d.Name(), q)
		}
	}
}

// A column reached through a nil embedded pointer has nothing behind it to read,
// so it binds its own zero — not the zero of whatever the pointer leads to.
func TestNilEmbeddedPointerBindsZero(t *testing.T) {
	t.Parallel()
	type Inner struct {
		Age int `barm:"age"`
	}
	type Mid struct{ Inner }
	type person struct {
		barm.BaseModel `barm:"table:people"`

		ID   int64  `barm:"id,pk"`
		Name string `barm:"name"`
		*Mid
	}
	db := barm.NewBuilder(barm.Postgres)
	p := &person{ID: 7, Name: "a"}

	_, args, err := db.Insert[person]().Values(p).Build()
	if err != nil || !slices.Equal(args, []any{int64(7), "a", 0}) {
		t.Errorf("insert args = %#v, %v", args, err)
	}
	_, args, err = db.Update[person]().Value(p).WherePK().Build()
	if err != nil || !slices.Equal(args, []any{"a", 0, int64(7)}) {
		t.Errorf("update args = %#v, %v", args, err)
	}
}

// A column mapped by an outer field and by an embedded one belongs to the outer
// one, as Go promotes fields; two at the same depth are ambiguous.
func TestEmbeddedColumnShadowing(t *testing.T) {
	t.Parallel()
	type Contact struct {
		Email string `barm:"email"`
		Phone string `barm:"phone"`
	}
	type acct struct {
		barm.BaseModel `barm:"table:accts"`

		ID    int64  `barm:"id,pk"`
		Email string `barm:"email"`
		Contact
	}
	db := barm.NewBuilder(barm.Postgres)
	a := &acct{ID: 1, Email: "outer", Contact: Contact{Email: "inner", Phone: "p"}}

	q, args, err := db.Insert[acct]().Values(a).Build()
	if err != nil || q != `INSERT INTO "accts" ("id", "email", "phone") VALUES ($1, $2, $3)` ||
		!slices.Equal(args, []any{int64(1), "outer", "p"}) {
		t.Errorf("insert = %s %v, %v", q, args, err)
	}
	q, args, err = db.Update[acct]().Value(a).WherePK().Build()
	if err != nil || q != `UPDATE "accts" SET "email" = $1, "phone" = $2 WHERE "id" = $3` ||
		!slices.Equal(args, []any{"outer", "p", int64(1)}) {
		t.Errorf("update = %s %v, %v", q, args, err)
	}

	type A struct {
		X string `barm:"x"`
	}
	type B struct {
		X string `barm:"x"`
	}
	type ambiguous struct {
		barm.BaseModel `barm:"table:t"`
		A
		B
	}
	// every time: a model that fails is not cached, but it is not cached as a
	// success either
	for range 2 {
		_, _, err = db.Select[ambiguous]().Build()
		if err == nil {
			t.Error("expected an error for a column mapped twice at the same depth")
		}
	}
}

// `-` says a field is not mapped, and an embedded struct is a field like any
// other: its columns stay out of every statement.
func TestEmbeddedStructDashIsNotMapped(t *testing.T) {
	t.Parallel()
	type Secret struct {
		Token string `barm:"token"`
	}
	type acct struct {
		barm.BaseModel `barm:"table:accts"`

		ID     int64 `barm:"id,pk"`
		Secret `barm:"-"`
	}
	db := barm.NewBuilder(barm.Postgres)
	a := &acct{ID: 1, Token: "hunter2"}

	for _, q := range []barm.Query{db.Select[acct](), db.Insert[acct]().Values(a)} {
		sql, args, err := q.Build()
		if err != nil || strings.Contains(sql, "token") || slices.Contains(args, any("hunter2")) {
			t.Errorf("%s %v, %v", sql, args, err)
		}
	}
}

// A slice is one argument, whatever it holds: the driver sends it as an array,
// and `= ANY(?)` is how the SQL takes it. Nothing is spelled out as a list, so
// the text is the same for any number of values, none included.
func TestSliceBindsAsOneArgument(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	for _, ids := range []any{[]int64{1, 2, 3}, []int64{}, []string{"a"}, []any{1, "x"}} {
		q, args, err := db.Select[User]().Where("id = ANY(?)", ids).Build()
		if err != nil || !strings.HasSuffix(q, "WHERE id = ANY($1)") || len(args) != 1 {
			t.Errorf("%T: %s %v, %v", ids, q, args, err)
		}
	}
}

// A count that has to go around the query, rather than into it, wraps it whole.
func TestCountQueryWrapsShapedQueries(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, _, err := db.Select[User]().Where("age > ?", 1).GroupBy("name").OrderBy("name").CountQuery()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT count(*) FROM (SELECT 1 FROM "users" AS "u" WHERE age > $1 GROUP BY name) AS "barm_count"`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}

	// an ordinary query still takes count(*) in its projection's place
	q, _, err = db.Select[User]().Where("age > ?", 1).OrderBy("name").Limit(5).CountQuery()
	if err != nil {
		t.Fatal(err)
	}
	if want := `SELECT count(*) FROM "users" AS "u" WHERE age > $1`; q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
}

func TestSubsetProjection(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	// a narrower row type selects only its own columns
	q, _, err := db.Select[User]().Where("age > ?", 5).BuildAs[struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}]()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "u"."name", "u"."age" FROM "users" AS "u" WHERE age > $1`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}

	// a column the model does not have is caught at build time
	_, _, err = db.Select[User]().BuildAs[struct {
		Nope string `barm:"nope"`
	}]()
	if err == nil {
		t.Fatal("expected an error for a column outside the model")
	}

	// an explicit projection still wins over the row type
	q, _, err = db.Select[User]().ColumnExpr("count(*)").BuildAs[int64]()
	if err != nil {
		t.Fatal(err)
	}
	if q != `SELECT count(*) FROM "users" AS "u"` {
		t.Errorf("got %s", q)
	}
}

// The *As terminals skip settling the projection when the result type is the
// query's own — the SQL must come out identical either way.
func TestResultTypeSkipIsInvisible(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q := db.Select[User]().Where("age > ?", 5).OrderBy("id").Limit(3)

	plain, args, err := q.Build()
	if err != nil {
		t.Fatal(err)
	}
	explicit, args2, err := q.BuildAs[User]()
	if err != nil {
		t.Fatal(err)
	}
	if plain != explicit {
		t.Errorf("Build() and BuildAs[User]() disagree:\n%s\n%s", plain, explicit)
	}
	if len(args) != len(args2) {
		t.Errorf("args %v vs %v", args, args2)
	}

	// and a different result type still narrows
	narrowed, _, err := q.BuildAs[struct {
		Name string `barm:"name"`
	}]()
	if err != nil {
		t.Fatal(err)
	}
	if narrowed == plain {
		t.Error("a narrower result type produced the same SQL")
	}

	// the query itself is untouched by either
	again, _, err := q.Build()
	if err != nil || again != plain {
		t.Errorf("query mutated: %s", again)
	}
}

func TestTableOverrideSQL(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	// As adopts the model's columns, but the table expression stays as written
	q, _, err := db.Select[User]().Table("users u").Where("u.age > ?", 5).Build()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "id", "name", "email", "age", "created_at" FROM users u WHERE u.age > $1`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}

	// without As there is no model, so the projection stays a star
	q, _, err = db.Select[any]().Table("audit_log").Build()
	if err != nil {
		t.Fatal(err)
	}
	if q != `SELECT * FROM audit_log` {
		t.Errorf("got %s", q)
	}
}

// A type with no table names none: every builder has to say so rather than
// rendering an empty identifier into the SQL.
func TestWriteWithoutTable(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	type untabled struct {
		A int `barm:"a"`
	}
	var u untabled

	for _, tc := range []struct {
		name string
		q    barm.Query
	}{
		{"insert", db.Insert[untabled]().Values(&u)},
		{"update", db.Update[untabled]().Set("a = ?", 1).Where("b = ?", 2)},
		{"delete", db.Delete[untabled]().Where("b = ?", 2)},
		{"insert with schema", db.Insert[untabled]().Schema("tenant").Values(&u)},
	} {
		q, _, err := tc.q.Build()
		if err == nil {
			t.Errorf("%s: no error, built %s", tc.name, q)
		}
	}

	// naming one is what makes them buildable
	_, _, err := db.Insert[untabled]().Table("things").Values(&u).Build()
	if err != nil {
		t.Errorf("insert with Table: %v", err)
	}
}

// A nested query renders into its parent's builder, so both share one argument
// numbering — the thing that splicing separately built SQL together cannot do.
func TestCTESQL(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	recent := db.Select[User]().Column("id").Where("age > ?", 30)

	t.Run("select", func(t *testing.T) {
		t.Parallel()
		q, args, err := db.Select[User]().
			With("recent", recent).
			Table("recent").
			Where("id < ?", 100).
			Column("id").
			Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `WITH "recent" AS (SELECT "id" FROM "users" AS "u" WHERE age > $1) ` +
			`SELECT "id" FROM recent WHERE id < $2`
		if q != want {
			t.Errorf("got  %s\nwant %s", q, want)
		}
		if len(args) != 2 || args[0] != 30 || args[1] != 100 {
			t.Errorf("args = %v", args)
		}
	})

	// Two bodies are what tells shared numbering from separate: built on their
	// own each would start at $1 again and collide with the first.
	t.Run("two", func(t *testing.T) {
		t.Parallel()
		q, args, err := db.Select[User]().
			With("young", db.Select[User]().Column("id").Where("age < ?", 10)).
			With("old", db.Select[User]().Column("id").Where("age > ?", 90)).
			Table("young").
			Where("id NOT IN (SELECT id FROM old) AND id < ?", 100).
			Column("id").
			Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `WITH "young" AS (SELECT "id" FROM "users" AS "u" WHERE age < $1), ` +
			`"old" AS (SELECT "id" FROM "users" AS "u" WHERE age > $2) ` +
			`SELECT "id" FROM young WHERE id NOT IN (SELECT id FROM old) AND id < $3`
		if q != want {
			t.Errorf("got  %s\nwant %s", q, want)
		}
		if len(args) != 3 || args[0] != 10 || args[1] != 90 || args[2] != 100 {
			t.Errorf("args = %v", args)
		}
	})

	t.Run("writers", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			q    barm.Query
			want string
		}{
			{"insert", db.Insert[User]().With("recent", recent).Values(&User{Name: "a"}).Column("name"),
				`WITH "recent" AS (SELECT "id" FROM "users" AS "u" WHERE age > $1) ` +
					`INSERT INTO "users" ("name") VALUES ($2)`},
			{"update", db.Update[User]().With("recent", recent).Set("age = ?", 1).Where("id = ?", 2),
				`WITH "recent" AS (SELECT "id" FROM "users" AS "u" WHERE age > $1) ` +
					`UPDATE "users" SET age = $2 WHERE id = $3`},
			{"delete", db.Delete[User]().With("recent", recent).Where("id = ?", 2),
				`WITH "recent" AS (SELECT "id" FROM "users" AS "u" WHERE age > $1) ` +
					`DELETE FROM "users" WHERE id = $2`},
		} {
			got, _, err := tc.q.Build()
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Errorf("%s:\ngot  %s\nwant %s", tc.name, got, tc.want)
			}
		}
	})

	t.Run("union", func(t *testing.T) {
		t.Parallel()
		q, args, err := db.Select[User]().Column("id").Where("age < ?", 10).
			UnionAll(db.Select[User]().Column("id").Where("age > ?", 90)).
			OrderBy("id").
			Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `SELECT "id" FROM "users" AS "u" WHERE age < $1 UNION ALL ` +
			`SELECT "id" FROM "users" AS "u" WHERE age > $2 ORDER BY id`
		if q != want {
			t.Errorf("got  %s\nwant %s", q, want)
		}
		if len(args) != 2 || args[0] != 10 || args[1] != 90 {
			t.Errorf("args = %v", args)
		}
	})

	t.Run("recursive", func(t *testing.T) {
		t.Parallel()
		q, _, err := db.Select[User]().Table("tree").Column("id").
			WithRecursive("tree", db.Select[User]().Column("id").Where("age IS NULL").
				UnionAll(db.Select[User]().Table("tree t").Column("id"))).
			Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `WITH RECURSIVE "tree" AS (SELECT "id" FROM "users" AS "u" WHERE age IS NULL ` +
			`UNION ALL SELECT "id" FROM tree t) SELECT "id" FROM tree`
		if q != want {
			t.Errorf("got  %s\nwant %s", q, want)
		}
	})

	t.Run("expr", func(t *testing.T) {
		t.Parallel()
		q, args, err := db.Select[User]().Table("recent").Column("id").
			WithExpr(`recent(id) AS MATERIALIZED (SELECT id FROM hits WHERE seen > ?)`, 5).
			Build()
		if err != nil {
			t.Fatal(err)
		}
		want := `WITH recent(id) AS MATERIALIZED (SELECT id FROM hits WHERE seen > $1) ` +
			`SELECT "id" FROM recent`
		if q != want {
			t.Errorf("got  %s\nwant %s", q, want)
		}
		if len(args) != 1 || args[0] != 5 {
			t.Errorf("args = %v", args)
		}
	})

	// a body that cannot build carries its error out through the parent
	t.Run("error", func(t *testing.T) {
		t.Parallel()
		type untabled struct {
			A int `barm:"a"`
		}
		_, _, err := db.Select[User]().With("bad", db.Select[untabled]()).Build()
		if err == nil {
			t.Error("no error from a CTE body that names no table")
		}
	})
}

func TestSchemaSQL(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	for _, tc := range []struct {
		name string
		q    barm.Query
		want string
	}{
		{"select", db.Select[User]().Schema("tenant").Column("id"),
			`SELECT "id" FROM "tenant"."users" AS "u"`},
		{"select table expr wins", db.Select[User]().Schema("tenant").Table("users u").Column("id"),
			`SELECT "id" FROM users u`},
		{"insert", db.Insert[User]().Schema("tenant").Values(&User{Name: "a"}).Column("name"),
			`INSERT INTO "tenant"."users" ("name") VALUES ($1)`},
		{"insert table name", db.Insert[User]().Schema("tenant").Table("people").Values(&User{Name: "a"}).Column("name"),
			`INSERT INTO "tenant"."people" ("name") VALUES ($1)`},
		{"update", db.Update[User]().Schema("tenant").Set("age = ?", 1).Where("id = ?", 1),
			`UPDATE "tenant"."users" SET age = $1 WHERE id = $2`},
		{"delete", db.Delete[User]().Schema("tenant").Where("id = ?", 1),
			`DELETE FROM "tenant"."users" WHERE id = $1`},
	} {
		got, _, err := tc.q.Build()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s:\ngot  %s\nwant %s", tc.name, got, tc.want)
		}
	}
}

func TestSelectWithoutTable(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	type Untagged struct {
		N int `barm:"n"`
	}

	_, _, err := db.Select[int]().Build()
	if err == nil {
		t.Error("expected an error selecting with neither a model nor a table")
	}
	// columns but no BaseModel: still no table to select from
	_, _, err = db.Select[Untagged]().Build()
	if err == nil {
		t.Error("expected an error for a struct with no BaseModel")
	}
	// ... unless the table is named explicitly
	_, _, err = db.Select[Untagged]().Table("things").Build()
	if err != nil {
		t.Errorf("an explicit table should work: %v", err)
	}
}

func TestReturningNeedsDialectSupport(t *testing.T) {
	t.Parallel()
	my := barm.New(nil, barm.MySQL)

	_, err := my.Insert[User]().Values(&User{Name: "a"}).OneAs[User](t.Context())
	if err == nil {
		t.Error("expected an error: MySQL has no RETURNING")
	}
}

// In spells a slice out as placeholders inside the parentheses the SQL wrote,
// A query passed as an argument renders in place, so its placeholders continue
// the enclosing query's numbering, and the arguments on either side of it keep
// theirs.
func TestSubqueryArgument(t *testing.T) {
	t.Parallel()
	pg := barm.NewBuilder(barm.Postgres)
	ids := func() *barm.SelectQuery[User] {
		return pg.Select[User]().Column("id").Where("age > ? AND age < ? AND name <> ?", 1, 2, "z")
	}
	sub := `SELECT "id" FROM "users" AS "u" WHERE age > $2 AND age < $3 AND name <> $4`
	for _, tc := range []struct {
		what string
		q    barm.Query
		want string
		args []any
	}{
		{
			// the enclosing query counts the subquery as one argument, and has bound
			// one already when the subquery's three arrive
			"after an argument", pg.Delete[User]().Where("a = ? AND id IN (?)", 7, ids()),
			`DELETE FROM "users" WHERE a = $1 AND id IN (` + sub + `)`, []any{7, 1, 2, "z"},
		},
		{
			// ?1 after the subquery still points at the first bind
			"?N across it", pg.Delete[User]().Where("a = ?1 AND id IN (?2) AND b = ?1", 7, ids()),
			`DELETE FROM "users" WHERE a = $1 AND id IN (` + sub + `) AND b = $1`, []any{7, 1, 2, "z"},
		},
		{
			"column", pg.Select[User]().Column("id").ColumnExpr("(?) AS n", pg.Select[User]().ColumnExpr("count(*)").Where("age > ?", 5)).Where("name = ?", "x"),
			`SELECT "id", (SELECT count(*) FROM "users" AS "u" WHERE age > $1) AS n FROM "users" AS "u" WHERE name = $2`, []any{5, "x"},
		},
		{
			"raw", pg.NewRaw("SELECT EXISTS(?), ?", pg.NewRaw("SELECT 1 WHERE ? > 0", 3), "y"),
			`SELECT EXISTS(SELECT 1 WHERE $1 > 0), $2`, []any{3, "y"},
		},
	} {
		q, args, err := tc.q.Build()
		if err != nil || q != tc.want || !slices.Equal(args, tc.args) {
			t.Errorf("%s:\n got %s %v, %v\nwant %s %v", tc.what, q, args, err, tc.want, tc.args)
		}
	}

	type untabled struct {
		ID int64 `barm:"id"`
	}
	_, _, err := pg.Select[User]().Where("id IN (?)", pg.Select[untabled]()).Build()
	if err == nil {
		t.Error("a subquery that fails to build should fail the query around it")
	}
}

type counted struct {
	barm.BaseModel `barm:"table:authors,alias:a"`

	ID    int64  `barm:"id,pk"`
	Name  string `barm:"name"`
	Books int    `barm:"book_count,scanonly"`
}

// A scanonly field is in no statement barm writes, selects by default or
// returns by default; a raw column in the projection is what brings it back.
func TestScanOnly(t *testing.T) {
	t.Parallel()
	pg := barm.NewBuilder(barm.Postgres)
	v := &counted{ID: 1, Name: "a", Books: 5}
	sub := pg.NewRaw("SELECT count(*) FROM books WHERE author_id = a.id AND pages > ?", 3)
	for _, tc := range []struct {
		what string
		q    barm.Query
		want string
		args []any
	}{
		{"select", pg.Select[counted](), `SELECT "a"."id", "a"."name" FROM "authors" AS "a"`, nil},
		{
			"computed", pg.Select[counted]().ColumnExpr("a.*").ColumnExpr("(?) AS book_count", sub).Where("name = ?", "x"),
			`SELECT a.*, (SELECT count(*) FROM books WHERE author_id = a.id AND pages > $1) AS book_count FROM "authors" AS "a" WHERE name = $2`,
			[]any{3, "x"},
		},
		{"insert", pg.Insert[counted]().Values(v), `INSERT INTO "authors" ("id", "name") VALUES ($1, $2)`, []any{int64(1), "a"}},
		{"update", pg.Update[counted]().Value(v).WherePK(), `UPDATE "authors" SET "name" = $1 WHERE "id" = $2`, []any{"a", int64(1)}},
	} {
		q, args, err := tc.q.Build()
		if err != nil || q != tc.want || !slices.Equal(args, tc.args) {
			t.Errorf("%s:\n got %s %v, %v\nwant %s %v", tc.what, q, args, err, tc.want, tc.args)
		}
	}

	q, _, _ := pg.Insert[counted]().Values(v).BuildAs[counted]()
	if strings.Contains(q, "book_count") {
		t.Errorf("default RETURNING names the scanonly column: %s", q)
	}
	withCount := pg.Select[counted]().ColumnExpr("a.*").ColumnExpr("(?) AS book_count", sub).Where("name = ?", "x")
	for what, build := range map[string]func() (string, []any, error){
		"count": withCount.CountQuery, "exists": withCount.ExistsQuery,
	} {
		q, args, err := build()
		if err != nil || strings.Contains(q, "book_count") || !slices.Equal(args, []any{"x"}) {
			t.Errorf("%s keeps the added column: %s %v, %v", what, q, args, err)
		}
	}
	for what, q := range map[string]barm.Query{
		"insert": pg.Insert[counted]().Values(v).Column("book_count"),
		"update": pg.Update[counted]().Value(v).Column("book_count").WherePK(),
	} {
		_, _, err := q.Build()
		if err == nil {
			t.Errorf("%s: naming a scanonly column should fail", what)
		}
	}
}

type tenant struct {
	barm.BaseModel `barm:"table:tenants"`

	ID    int64  `barm:"id,pk"`
	Name  string `barm:"name"`
	Users int    `barm:"total_users,skipupdate"`
}

// A skipupdate column is left out of an update built from a value, and nothing
// else: it is inserted, selected, and written when Column names it.
func TestSkipUpdate(t *testing.T) {
	t.Parallel()
	pg := barm.NewBuilder(barm.Postgres)
	v := &tenant{ID: 1, Name: "a", Users: 5}
	for _, tc := range []struct {
		what string
		q    barm.Query
		want string
	}{
		{"update", pg.Update[tenant]().Value(v).WherePK(), `UPDATE "tenants" SET "name" = $1 WHERE "id" = $2`},
		{"named", pg.Update[tenant]().Value(v).Column("total_users").WherePK(), `UPDATE "tenants" SET "total_users" = $1 WHERE "id" = $2`},
		{"insert", pg.Insert[tenant]().Values(v), `INSERT INTO "tenants" ("id", "name", "total_users") VALUES ($1, $2, $3)`},
		{"select", pg.Select[tenant](), `SELECT "tenants"."id", "tenants"."name", "tenants"."total_users" FROM "tenants"`},
	} {
		q, _, err := tc.q.Build()
		if err != nil || q != tc.want {
			t.Errorf("%s:\n got %s, %v\nwant %s", tc.what, q, err, tc.want)
		}
	}
}

type untaggedKey struct {
	barm.BaseModel `barm:"table:things"`

	ID   int64  `barm:"id"`
	Name string `barm:"name"`
}

// A column named id is a column like any other: without pk it is no key, so
// WherePK has nothing to match on, and an insert writes it.
func TestNoInferredKey(t *testing.T) {
	t.Parallel()
	pg := barm.NewBuilder(barm.Postgres)
	v := &untaggedKey{Name: "a"}
	_, _, err := pg.Update[untaggedKey]().Value(v).WherePK().Build()
	if err == nil {
		t.Error("WherePK on a model with no pk field should fail")
	}
	q, args, err := pg.Insert[untaggedKey]().Values(v).Build()
	if err != nil || q != `INSERT INTO "things" ("id", "name") VALUES ($1, $2)` || !slices.Equal(args, []any{int64(0), "a"}) {
		t.Errorf("insert: %s %v, %v", q, args, err)
	}
}

// again at every reference, and refuses what is not a list.
func TestIn(t *testing.T) {
	t.Parallel()
	pg, my := barm.NewBuilder(barm.Postgres), barm.NewBuilder(barm.MySQL)
	for _, tc := range []struct {
		db   *barm.DB
		expr string
		arg  any
		want string
		args []any
	}{
		{pg, "id IN (?)", barm.In([]int64{1, 2, 3}), "id IN ($1, $2, $3)", []any{int64(1), int64(2), int64(3)}},
		{pg, "id IN (?)", barm.In([]any{1, "x"}), "id IN ($1, $2)", []any{1, "x"}},
		{my, "id IN (?)", barm.In([]string{"a", "b"}), "id IN (?, ?)", []any{"a", "b"}},
		{pg, "a IN (?1) OR b IN (?1)", barm.In([]int{5, 6}), "a IN ($1, $2) OR b IN ($3, $4)", []any{5, 6, 5, 6}},
	} {
		q, args, err := tc.db.Select[User]().Where(tc.expr, tc.arg).Build()
		if err != nil || !strings.HasSuffix(q, "WHERE "+tc.want) || !slices.Equal(args, tc.args) {
			t.Errorf("%s: %s %v, %v — want %s %v", tc.expr, q, args, err, tc.want, tc.args)
		}
	}
	for what, arg := range map[string]any{"empty": barm.In([]int64{}), "empty any": barm.In([]any{}), "not a slice": barm.In(7)} {
		for _, q := range []barm.Query{
			pg.Select[User]().Where("id IN (?)", arg),
			pg.NewRaw("DELETE FROM users WHERE id IN (?)", arg),
			pg.Select[User]().With("x", pg.Select[User]().Where("id IN (?)", arg)).Table("x"),
		} {
			_, _, err := q.Build()
			if err == nil {
				t.Errorf("%s: expected a build error", what)
			}
		}
	}
}

// With no Returning, the clause is the result type's own tagged columns.
func TestReturningDefaultsToResultColumns(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, _, err := db.Insert[User]().Values(&User{Name: "a"}).BuildAs[struct {
		ID   int64  `barm:"id"`
		Name string `barm:"name"`
	}]()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, `RETURNING "id", "name"`) {
		t.Errorf("got %s", q)
	}

	// the full model asks for every column it maps
	q, _, err = db.Update[User]().Set("age = 1").Where("id = ?", 1).BuildAs[User]()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, `RETURNING "id", "name", "email", "age", "created_at"`) {
		t.Errorf("got %s", q)
	}

	// an explicit Returning wins
	q, _, err = db.Delete[User]().Where("id = ?", 1).Returning("id").BuildAs[int64]()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "RETURNING id") {
		t.Errorf("got %s", q)
	}

	// a scalar with no Returning names nothing to return
	_, _, err = db.Delete[User]().Where("id = ?", 1).BuildAs[int64]()
	if err == nil {
		t.Error("expected an error: int64 names no columns")
	}
}

func TestApply(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	adult := func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
		return q.Where("age >= ?", 18)
	}
	newest := func(n int64) func(*barm.SelectQuery[User]) *barm.SelectQuery[User] {
		return func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.OrderBy("created_at DESC").Limit(n)
		}
	}

	// a nil fn is skipped, so an optional clause needs no branch
	var optional func(*barm.SelectQuery[User]) *barm.SelectQuery[User]

	q, args, err := db.Select[User]().Apply(adult, optional, newest(5)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, `WHERE age >= $1 ORDER BY created_at DESC LIMIT 5`) {
		t.Errorf("got %s", q)
	}
	if len(args) != 1 || args[0] != 18 {
		t.Errorf("args = %v", args)
	}

	iq, _, err := db.Insert[User]().Values(&User{Name: "a"}).
		Apply(func(q *barm.InsertQuery[User]) *barm.InsertQuery[User] {
			return q.On("CONFLICT DO NOTHING")
		}).Build()

	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(iq, "ON CONFLICT DO NOTHING") {
		t.Errorf("got %s", iq)
	}
}

func TestIndexedPlaceholders(t *testing.T) {
	t.Parallel()
	pg := barm.New(nil, barm.Postgres)

	// ?1 binds once and is referenced twice
	q, args, err := pg.Select[User]().
		Where("name = ?1 OR email = ?1", "root").
		Where("age > ?", 18).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" ` +
		`FROM "users" AS "u" WHERE (name = $1 OR email = $1) AND (age > $2)`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 2 || args[0] != "root" || args[1] != 18 {
		t.Errorf("args = %v", args)
	}

	// Out-of-order references are fine. A bare ? keeps its own counter, so here
	// it takes argument 1 — already bound by ?1, hence the reuse.
	q, args, err = pg.Select[User]().Where("a = ?2 AND b = ?1 AND c = ?", 1, 2).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, `WHERE a = $1 AND b = $2 AND c = $2`) {
		t.Errorf("got %s", q)
	}
	if len(args) != 2 || args[0] != 2 || args[1] != 1 {
		t.Errorf("args = %v", args)
	}

	// a positional dialect cannot reference a placeholder twice, so it re-binds
	my := barm.New(nil, barm.MySQL)
	q, args, err = my.Select[User]().Where("name = ?1 OR email = ?1", "root").Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE name = ? OR email = ?") {
		t.Errorf("got %s", q)
	}
	if len(args) != 2 || args[0] != "root" || args[1] != "root" {
		t.Errorf("args = %v", args)
	}

	// an index past the end is written through, not silently misbound
	q, _, err = pg.Select[User]().Where("x = ?9", 1).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE x = ?") {
		t.Errorf("got %s", q)
	}

	// a slice is one bind like any other, so it is referenced again too
	q, args, err = pg.Select[User]().Where("a = ANY(?1) OR b = ANY(?1)", []int64{5}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE a = ANY($1) OR b = ANY($1)") || len(args) != 1 {
		t.Errorf("got %s %v", q, args)
	}
}

func TestArgDedup(t *testing.T) {
	t.Parallel()
	blob := strings.Repeat("x", 200)
	small := "y"

	db := barm.New(nil, barm.Postgres, barm.WithArgDedup())

	// the same backing array reaches two placeholders: bound once
	q, args, err := db.Select[User]().Where("a = ?", blob).Where("b = ?", blob).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $1)") {
		t.Errorf("got %s", q)
	}
	if len(args) != 1 {
		t.Errorf("args = %v", len(args))
	}

	// equal contents but a distinct backing array is a distinct argument
	other := strings.Clone(blob)
	q, args, err = db.Select[User]().Where("a = ?", blob).Where("b = ?", other).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $2)") || len(args) != 2 {
		t.Errorf("got %s / %d args", q, len(args))
	}

	// size does not matter: identity does
	q, args, err = db.Select[User]().Where("a = ?", small).Where("b = ?", small).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $1)") || len(args) != 1 {
		t.Errorf("got %s / %d args", q, len(args))
	}

	// a nil []byte is NULL and an empty one is an empty blob: never merged
	q, args, err = db.Select[User]().
		Where("a = ?", []byte(nil)).
		Where("b = ?", []byte{}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $2)") || len(args) != 2 {
		t.Errorf("got %s / %d args", q, len(args))
	}

	// the same []byte reaching two markers is bound once
	buf := []byte("payload")
	q, args, err = db.Select[User]().Where("a = ?", buf).Where("b = ?", buf).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $1)") || len(args) != 1 {
		t.Errorf("got %s / %d args", q, len(args))
	}

	// a prepared query must render the same text regardless of its values
	q, args, err = db.Select[User]().Prepare("p").Where("a = ?", blob).Where("b = ?", blob).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $2)") || len(args) != 2 {
		t.Errorf("prepared query deduped: %s / %d args", q, len(args))
	}

	// off by default
	plain := barm.New(nil, barm.Postgres)
	q, _, err = plain.Select[User]().Where("a = ?", blob).Where("b = ?", blob).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(q, "WHERE (a = $1) AND (b = $2)") {
		t.Errorf("got %s", q)
	}
}

// Past a few dozen distinct arguments the bound ones are indexed rather than
// scanned; a value seen before the switch must still be found after it.
func TestArgDedupIndexed(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres, barm.WithArgDedup())

	vals := make([]string, 40)
	q := db.Select[User]()
	for i := range vals {
		vals[i] = "v" + strconv.Itoa(i*1000)
		q = q.Where("name = ?", vals[i])
	}
	query, args, err := q.Where("email = ?", vals[0]).Where("email = ?", vals[39]).Build()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(query, "(email = $1) AND (email = $40)") {
		t.Errorf("got %s", query[max(0, len(query)-60):])
	}
	if len(args) != len(vals) {
		t.Errorf("len(args) = %d, want %d", len(args), len(vals))
	}
}

// The builder is pooled, so a query must not see another's buffer or args.
func TestPooledBuilderIsolation(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			for range 100 {
				q, args, err := db.Select[User]().
					Where("age = ?", i).
					Where("name = ?", "n").
					Build()
				if err != nil {
					t.Error(err)
					return
				}
				want := `SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" ` +
					`FROM "users" AS "u" WHERE (age = $1) AND (name = $2)`
				if q != want {
					t.Errorf("got %s", q)
					return
				}
				if len(args) != 2 || args[0] != i || args[1] != "n" {
					t.Errorf("args = %v, want [%d n]", args, i)
					return
				}
			}
		})
	}
	wg.Wait()
}

// An outsized query must not leave a huge buffer pinned, nor corrupt the next.
func TestPooledBuilderAfterLargeQuery(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	big := db.Select[User]()
	for range 4000 {
		big.Where("name = ?", strings.Repeat("x", 64))
	}
	_, _, err := big.Build()
	if err != nil {
		t.Fatal(err)
	}

	q, args, err := db.Select[User]().Where("age = ?", 1).Build()
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" ` +
		`FROM "users" AS "u" WHERE age = $1`
	if q != want || len(args) != 1 {
		t.Errorf("got %s / %v", q, args)
	}
}

// Arguments are bound by value: a row mutated after Build cannot change a query
// already rendered. This is what lets the columns of one row be bound with a
// single copy rather than one allocation per column.
func TestBuildSnapshotsValues(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)

	u := &User{ID: 1, Name: "before", Age: 30}
	_, args, err := db.Insert[User]().Values(u).Build()
	if err != nil {
		t.Fatal(err)
	}
	u.Name = "after"
	if !slices.Contains(args, any("before")) {
		t.Errorf("args = %v, want the value as it was at Build", args)
	}

	up := &User{ID: 7, Name: "before"}
	_, args, err = db.Update[User]().Value(up).Column("name").WherePK().Build()
	if err != nil {
		t.Fatal(err)
	}
	up.Name = "after"
	if !slices.Contains(args, any("before")) {
		t.Errorf("args = %v, want the value as it was at Build", args)
	}
}

type defaulted struct {
	barm.BaseModel `barm:"table:events,alias:e"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	CreatedAt time.Time `barm:"created_at,default:current_timestamp"`
}

// A zero value in a column that has a default means "let the database decide",
// so the column is left out and its own DEFAULT applies.
func TestInsertDefaultOmitsColumn(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)

	q, args, err := db.Insert[defaulted]().Values(&defaulted{Name: "a"}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := `INSERT INTO "events" ("name") VALUES ($1)`; q != want {
		t.Errorf("query = %s, want %s", q, want)
	}
	if len(args) != 1 {
		t.Errorf("args = %v, want just the name", args)
	}

	// Set it, and it is written like any other column.
	now := time.Now()
	q, args, err = db.Insert[defaulted]().Values(&defaulted{Name: "a", CreatedAt: now}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := `INSERT INTO "events" ("name", "created_at") VALUES ($1, $2)`; q != want {
		t.Errorf("query = %s, want %s", q, want)
	}
	if len(args) != 2 {
		t.Errorf("args = %v, want the name and the time", args)
	}
}

// When one row sets the column and another does not, it cannot be left out — the
// rows that did not set it say DEFAULT, which is the column's real default and
// not barm's idea of it. A dialect that will not take the keyword falls back to
// the expression from the tag.
func TestInsertDefaultMixedRows(t *testing.T) {
	t.Parallel()
	now := time.Now()
	for _, tc := range []struct {
		d    barm.Dialect
		want string
	}{
		{barm.Postgres, `INSERT INTO "events" ("name", "created_at") VALUES ($1, $2), ($3, DEFAULT)`},
		{barm.MySQL, "INSERT INTO `events` (`name`, `created_at`) VALUES (?, ?), (?, DEFAULT)"},
		{barm.SQLite, `INSERT INTO "events" ("name", "created_at") VALUES (?, ?), (?, current_timestamp)`},
	} {
		t.Run(tc.d.Name(), func(t *testing.T) {
			t.Parallel()
			q, args, err := barm.NewBuilder(tc.d).Insert[defaulted]().Values(
				&defaulted{Name: "a", CreatedAt: now},
				&defaulted{Name: "b"},
			).Build()
			if err != nil {
				t.Fatal(err)
			}
			if q != tc.want {
				t.Errorf("query = %s\nwant  = %s", q, tc.want)
			}
			if len(args) != 3 {
				t.Errorf("args = %v, want three", args)
			}
		})
	}
}

// An auto key set on one row but not another is the same case with no
// expression to fall back on: sqlite fills an integer primary key in for NULL.
func TestInsertAutoKeyMixedRows(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		d    barm.Dialect
		want string
	}{
		{barm.Postgres, `INSERT INTO "users" ("id", "name", "email", "age", "created_at") VALUES ($1, $2, $3, $4, $5), (DEFAULT, $6, $7, $8, $9)`},
		{barm.MySQL, "INSERT INTO `users` (`id`, `name`, `email`, `age`, `created_at`) VALUES (?, ?, ?, ?, ?), (DEFAULT, ?, ?, ?, ?)"},
		{barm.SQLite, `INSERT INTO "users" ("id", "name", "email", "age", "created_at") VALUES (?, ?, ?, ?, ?), (NULL, ?, ?, ?, ?)`},
	} {
		t.Run(tc.d.Name(), func(t *testing.T) {
			t.Parallel()
			q, args, err := barm.NewBuilder(tc.d).Insert[User]().Values(
				&User{ID: 100, Name: "a"},
				&User{Name: "b"},
			).Build()
			if err != nil {
				t.Fatal(err)
			}
			if q != tc.want {
				t.Errorf("query = %s\nwant  = %s", q, tc.want)
			}
			if len(args) != 9 {
				t.Errorf("args = %v, want nine", args)
			}
		})
	}
}

// The embedded database/sql types must not answer for anything barm has its own
// version of: a *sql.Tx builds no queries, and nothing at the call site would
// say so. These assignments fail to compile if one of them starts leaking.
func TestEmbeddedTypesDoNotLeak(t *testing.T) {
	t.Parallel()
	var (
		db *barm.DB
		c  *barm.Conn
		tx *barm.Tx
	)
	_ = func(ctx context.Context) {
		tx, _ = db.BeginTx(ctx, nil)
		tx, _ = db.BeginTx(ctx, nil)
		tx, _ = c.BeginTx(ctx, nil)
		tx, _ = c.BeginTx(ctx, nil)
		tx, _ = tx.BeginTx(ctx, nil)
		c, _ = db.Conn(ctx)
	}
	_, _, _ = db, c, tx
}

// A builder-only DB has nothing to run on, and says so rather than panicking:
// a nil *sql.DB inside the Querier interface is not a nil interface.
func TestBuilderOnlyReportsNoConn(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	ctx := t.Context()

	_, err := db.Select[User]().Slice(ctx)
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("Slice = %v, want ErrNoConn", err)
	}
	_, err = db.Select[User]().One(ctx)
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("One = %v, want ErrNoConn", err)
	}
	_, err = db.Select[User]().Count(ctx)
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("Count = %v, want ErrNoConn", err)
	}
	for _, err := range db.Select[User]().Seq(ctx) {
		if !errors.Is(err, barm.ErrNoConn) {
			t.Errorf("Seq = %v, want ErrNoConn", err)
		}
		break
	}
	_, err = db.Insert[User]().Values(&User{Name: "x"}).Exec(ctx)
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("Exec = %v, want ErrNoConn", err)
	}
	err = db.Ping(ctx)
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("Ping = %v, want ErrNoConn", err)
	}
}

// Handle is what lets one function serve a DB, a transaction and a connection.
func TestHandleAcceptsEveryKind(t *testing.T) {
	t.Parallel()
	var (
		db *barm.DB
		tx *barm.Tx
		c  *barm.Conn
	)
	_ = func() {
		var _ barm.IDB = db
		var _ barm.IDB = tx
		idb := barm.IDB(c)

		h := barm.Handle{idb}
		_ = h.Select[User]()
		_ = h.Insert[User]()
		_ = h.Update[User]()
		_ = h.Delete[User]()
	}
	// Every builder returning a User satisfies TypedQuery[User].
	_ = func(dbh *barm.DB) {
		var _ barm.TypedQuery[User] = dbh.Select[User]()
		var _ barm.TypedQuery[User] = dbh.Insert[User]()
		var _ barm.TypedQuery[User] = dbh.Update[User]()
		q := dbh.Delete[User]()
		var plain barm.Query = q
		_, _ = q, plain
	}
	// And the database/sql adapter is a Pool.
	_ = func() {
		var _ = barm.SQL((*sql.DB)(nil))
	}
	_, _, _ = db, tx, c
}

// Begin mirrors database/sql: no arguments, default isolation, background
// context. BeginTx is the one that takes them.
func TestBeginSignatures(t *testing.T) {
	t.Parallel()
	var (
		db *barm.DB
		tx *barm.Tx
		c  *barm.Conn
	)
	_ = func(ctx context.Context, opts *sql.TxOptions) {
		_, _ = db.Begin()
		_, _ = db.BeginTx(ctx, opts)
		_, _ = c.Begin()
		_, _ = c.BeginTx(ctx, opts)
		_, _ = tx.Begin()
		_, _ = tx.BeginTx(ctx, nil)
	}
	_, _, _ = db, tx, c
}

// Query is closed to barm's own types, so hand-written SQL goes through SQL —
// which renders into the enclosing builder like any other query, and so shares
// its argument numbering.
func TestRawQuery(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)

	q, args, err := db.NewRaw("DELETE FROM sessions WHERE seen < ? AND kind = ?", 5, "web").Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := "DELETE FROM sessions WHERE seen < $1 AND kind = $2"; q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 2 || args[0] != 5 || args[1] != "web" {
		t.Errorf("args = %v", args)
	}

	// as a CTE body, sharing the outer query's numbering
	q, args, err = db.Select[User]().
		With("stale", db.NewRaw("SELECT id FROM sessions WHERE seen < ?", 5)).
		Table("stale").
		Where("id > ?", 7).
		Column("id").
		Build()
	if err != nil {
		t.Fatal(err)
	}
	want := `WITH "stale" AS (SELECT id FROM sessions WHERE seen < $1) ` +
		`SELECT "id" FROM stale WHERE id > $2`
	if q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}
	if len(args) != 2 || args[0] != 5 || args[1] != 7 {
		t.Errorf("args = %v", args)
	}

	// and as a union branch
	q, _, err = db.Select[User]().Column("id").
		UnionAll(db.NewRaw("SELECT id FROM archived_users")).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	if want := `SELECT "id" FROM "users" AS "u" UNION ALL SELECT id FROM archived_users`; q != want {
		t.Errorf("got  %s\nwant %s", q, want)
	}

	// it is a Query, so it queues in a batch like any other
	var _ barm.Query = db.NewRaw("SELECT 1")
}

// Among several conditions each is wrapped, whatever spells its OR: barm does
// not parse the SQL, so a MySQL || or an XOR could not otherwise be told apart
// from an AND, and would bind across a tenant filter.
func TestConditionsAreWrapped(t *testing.T) {
	t.Parallel()
	my := barm.NewBuilder(barm.MySQL)
	q, _, err := my.Select[User]().Where("tenant_id = ?", 1).Where("a = ? || b = ?", 2, 3).Build()
	if err != nil || !strings.HasSuffix(q, "WHERE (tenant_id = ?) AND (a = ? || b = ?)") {
		t.Errorf("got %s, %v", q, err)
	}
	q, _, err = my.Select[User]().Where("a = ? || b = ?", 2, 3).Build()
	if err != nil || !strings.HasSuffix(q, "WHERE a = ? || b = ?") {
		t.Errorf("a lone condition has nothing to bind across: %s, %v", q, err)
	}
}

// A negative Limit or Offset is an error rather than no limit, so a page size
// taken from input cannot ask for the whole table.
func TestNegativeLimit(t *testing.T) {
	t.Parallel()
	pg := barm.NewBuilder(barm.Postgres)
	for what, q := range map[string]barm.Query{
		"limit":  pg.Select[User]().Limit(-1),
		"offset": pg.Select[User]().Offset(-5),
	} {
		_, _, err := q.Build()
		if err == nil {
			t.Errorf("%s: negative value built", what)
		}
	}
}
