package barm_test

import (
	"testing"

	"github.com/sirkostya009/barm"
)

// An identifier and raw SQL take a marker's place without binding anything,
// so the value after them is still the first argument.
func TestIdentAndSafe(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	q, args, err := db.Select[User]().
		Column("name").
		Join("JOIN ? AS tv ON tv.id = u.id", barm.Ident("tmp_versions")).
		Where("? = ?", barm.Ident("tv.version"), 3).
		Where("? IS NULL", barm.Safe("coalesce(u.email, u.name)")).
		Build()
	check(t, q, args, err,
		`SELECT "name" FROM "users" AS "u" JOIN "tmp_versions" AS tv ON tv.id = u.id `+
			`WHERE ("tv"."version" = $1) AND (coalesce(u.email, u.name) IS NULL)`,
		3)
}

func TestIdentQuoting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		d    barm.Dialect
		name string
		want string
	}{
		{barm.Postgres, "public.users", `"public"."users"`},
		{barm.Postgres, `bad"name`, `"bad""name"`},
		{barm.Postgres, `x"; DROP TABLE users; --`, `"x""; DROP TABLE users; --"`},
		{barm.MySQL, "db.t", "`db`.`t`"},
		{barm.MySQL, "a`b", "`a``b`"},
	} {
		q, args, err := barm.NewBuilder(tc.d).NewRaw("SELECT id FROM ?", barm.Ident(tc.name)).Build()
		check(t, q, nil, err, "SELECT id FROM "+tc.want)
		if len(args) != 0 {
			t.Errorf("an identifier bound %v", args)
		}
	}
}

// Raw SQL is written as it is: a ? inside it is not a marker, and ?N
// references write an identifier or raw SQL again rather than reusing a
// placeholder.
func TestSafeIsVerbatim(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	q, args, err := db.NewRaw("SELECT ?1, ?1, ?2, ?3", barm.Safe("data ? 'k'"), barm.Ident("c"), 5).Build()
	check(t, q, args, err, `SELECT data ? 'k', data ? 'k', "c", $1`, 5)

	q, args, err = barm.NewBuilder(barm.SQLite).NewRaw("SELECT ? FROM t WHERE x = ?", barm.Ident("c"), 1).Build()
	check(t, q, args, err, `SELECT "c" FROM t WHERE x = ?`, 1)
}
