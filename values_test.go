package barm_test

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

type change struct {
	ID   int64          `barm:"id"`
	Name string         `barm:"name"`
	Meta map[string]any `barm:"meta,json"`
	Note string         `barm:"note,nullzero"`
}

func check(t *testing.T, q string, args []any, err error, wantQ string, wantArgs ...any) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if q != wantQ {
		t.Errorf("got  %s\nwant %s", q, wantQ)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Errorf("args = %#v\nwant %#v", args, wantArgs)
	}
}

// A VALUES list as a CTE names its columns, and an update reads it through
// From. Its arguments come first, as the CTE does in the text, and each field
// binds as it would in an insert: json encoded, nullzero as NULL.
func TestUpdateFromValues(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	rows := []change{{ID: 1, Name: "a", Meta: map[string]any{"k": 1}}, {ID: 2, Name: "b", Note: "n"}}

	q, args, err := db.Update[User]().
		With("data", db.Values(rows)).
		From("data").
		Set("name = data.name").
		Set("age = age + ?", 1).
		Where("users.id = data.id::bigint").
		Where("users.age < ?", 99).
		Build()
	check(t, q, args, err,
		`WITH "data" ("id", "name", "meta", "note") AS (VALUES ($1::bigint, $2::text, $3::jsonb, $4::text), ($5, $6, $7, $8)) `+
			`UPDATE "users" SET name = data.name, age = age + $9 FROM data `+
			`WHERE (users.id = data.id::bigint) AND (users.age < $10)`,
		int64(1), "a", `{"k":1}`, nil, int64(2), "b", nil, "n", 1, 99)
}

// From and Set interleave freely: the tables are listed in order, after every
// assignment, and their arguments take their place in the text.
func TestUpdateFromOrder(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	q, args, err := db.Update[User]().
		From("teams AS t").
		Set("name = t.name").
		From("(SELECT id FROM orgs WHERE plan = ?) AS o", "pro").
		Set("email = ?", "e").
		Where("u.team_id = t.id AND t.org_id = o.id").
		Build()
	check(t, q, args, err,
		`UPDATE "users" SET name = t.name, email = $1 FROM teams AS t, (SELECT id FROM orgs WHERE plan = $2) AS o `+
			`WHERE u.team_id = t.id AND t.org_id = o.id`,
		"e", "pro")
}

func TestDeleteUsingValues(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	q, args, err := db.Delete[User]().
		With("data", db.Values([]change{{ID: 7, Name: "x"}})).
		Using("data").
		Where("users.id = data.id::bigint").
		Where("users.name = data.name").
		Build()
	check(t, q, args, err,
		`WITH "data" ("id", "name", "meta", "note") AS (VALUES ($1::bigint, $2::text, $3::jsonb, $4::text)) `+
			`DELETE FROM "users" USING data WHERE (users.id = data.id::bigint) AND (users.name = data.name)`,
		int64(7), "x", nil, nil)
}

// A table to match against is no condition: a delete with Using alone still
// has no WHERE, and one condition beside it is not wrapped as if among several.
func TestUsingIsNotACondition(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	_, _, err := db.Delete[User]().Using("data").Build()
	if err == nil || !strings.Contains(err.Error(), "no WHERE") {
		t.Errorf("Using alone: %v, want the missing WHERE reported", err)
	}
	q, _, err := db.Delete[User]().Using("data").Where("users.id = data.id").Build()
	check(t, q, nil, err, `DELETE FROM "users" USING data WHERE users.id = data.id`)

	_, _, err = db.Update[User]().From("data").Where("users.id = data.id").Build()
	if err == nil || !strings.Contains(err.Error(), "nothing to set") {
		t.Errorf("From alone: %v, want nothing to set reported", err)
	}
}

// A VALUES list goes wherever a query does, and reports what it cannot render.
func TestValuesElsewhere(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	type pair struct {
		A int64  `barm:"a"`
		B string `barm:"b"`
	}
	q, args, err := db.Select[User]().
		Where("(u.id, u.name) IN (?)", db.Values([]pair{{1, "x"}, {2, "y"}})).
		Build()
	check(t, q, args, err,
		`SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" FROM "users" AS "u" `+
			`WHERE (u.id, u.name) IN (VALUES ($1::bigint, $2::text), ($3, $4))`,
		int64(1), "x", int64(2), "y")

	q, args, err = db.Values([]*pair{{3, "z"}}).Build()
	check(t, q, args, err, `VALUES ($1::bigint, $2::text)`, int64(3), "z")

	_, _, err = db.Values([]pair{}).Build()
	if err == nil {
		t.Error("an empty VALUES list should not render")
	}
	_, _, err = db.Values([]int{1}).Build()
	if err == nil {
		t.Error("a VALUES list of non-structs should not render")
	}
}

// From and Using do not grow the builders: they ride in slices already there.
func TestFromKeepsWhereShape(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	u := &User{ID: 3, Name: "n"}
	q, args, err := db.Update[User]().Value(u).Column("name").From("x").WherePK().Build()
	check(t, q, args, err, `UPDATE "users" SET "name" = $1 FROM x WHERE "id" = $2`, "n", int64(3))
}

type casts struct {
	I8    int8            `barm:"i8"`
	U8    uint8           `barm:"u8"`
	I32   int32           `barm:"i32"`
	U16   uint16          `barm:"u16"`
	I     int             `barm:"i"`
	U32   uint32          `barm:"u32"`
	U64   uint64          `barm:"u64"`
	F32   float32         `barm:"f32"`
	F64   float64         `barm:"f64"`
	B     bool            `barm:"b"`
	S     *string         `barm:"s"`
	Bytes []byte          `barm:"bytes"`
	At    time.Time       `barm:"at"`
	Ints  []int64         `barm:"ints,array"`
	Grid  [][]int32       `barm:"grid"`
	Null  sql.NullInt64   `barm:"null"`
	UUID  [16]byte        `barm:"uuid,type:uuid"`
	Own   ownValuer       `barm:"own"`
	Obj   struct{ A int } `barm:"obj"`
	Kind  kind            `barm:"kind"`
}

type ownValuer struct{}

func (ownValuer) Value() (driver.Value, error) { return "x", nil }

type kind string

// The first row is cast to the type each field maps to, or to its type: tag;
// a field barm cannot map stays uncast. Only Postgres casts.
func TestValuesCasts(t *testing.T) {
	t.Parallel()
	rows := []casts{{}, {}}
	q, _, err := barm.New(nil, barm.Postgres).Values(rows).Build()
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := strings.Cut(q, "), (")
	want := `VALUES ($1::smallint, $2::smallint, $3::integer, $4::integer, $5::bigint, $6::bigint, $7::numeric, ` +
		`$8::real, $9::double precision, $10::boolean, $11::text, $12::bytea, $13::timestamptz, $14::bigint[], ` +
		`$15::integer[], $16::bigint, $17::uuid, $18, $19, $20::text`
	if first != want {
		t.Errorf("got  %s\nwant %s", first, want)
	}
	if strings.Count(q, "::") != strings.Count(want, "::") {
		t.Errorf("rows after the first are cast too: %s", q)
	}
	for _, d := range []barm.Dialect{barm.SQLite, barm.MySQL} {
		q, _, err := barm.New(nil, d).Values(rows[:1]).Build()
		if err != nil || strings.Contains(q, "::") {
			t.Errorf("%s: %s, %v", d.Name(), q, err)
		}
	}
}
