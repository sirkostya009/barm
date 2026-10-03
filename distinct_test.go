package barm_test

import (
	"strings"
	"testing"

	"github.com/sirkostya009/barm"
)

// DISTINCT ON goes ahead of the columns, so its arguments take the first
// numbers, and a second call adds to the list.
func TestDistinctOn(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	q, args, err := db.Select[User]().
		DistinctOn("u.team_id").
		DistinctOn("u.age > ?", 30).
		ColumnExpr("u.name, ? AS tag", "x").
		OrderBy("u.team_id, u.age > 30, u.created_at DESC").
		Build()
	check(t, q, args, err,
		`SELECT DISTINCT ON (u.team_id, u.age > $1) u.name, $2 AS tag FROM "users" AS "u" `+
			`ORDER BY u.team_id, u.age > 30, u.created_at DESC`,
		30, "x")

	q, _, err = db.Select[User]().Column("name").Distinct().Build()
	check(t, q, nil, err, `SELECT DISTINCT "name" FROM "users" AS "u"`)
}

// A Clone keeps the ON list it had: adding to one does not reach the other.
func TestDistinctOnClone(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	base := db.Select[User]().Column("name").DistinctOn("a")
	more := base.Clone().DistinctOn("b")
	plain := base.Clone().Distinct()
	for _, tc := range []struct {
		q    *barm.SelectQuery[User]
		want string
	}{
		{base, `SELECT DISTINCT ON (a) "name" FROM "users" AS "u"`},
		{more, `SELECT DISTINCT ON (a, b) "name" FROM "users" AS "u"`},
		{plain, `SELECT DISTINCT "name" FROM "users" AS "u"`},
	} {
		q, _, err := tc.q.Build()
		check(t, q, nil, err, tc.want)
	}
}

// A DISTINCT ON query decides what a row is, so it is counted from outside.
func TestDistinctOnCount(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	q, args, err := db.Select[User]().Column("name").DistinctOn("u.age % ?", 10).CountQuery()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(q, `SELECT count(*) FROM (SELECT DISTINCT ON (u.age % $1) "name"`) || len(args) != 1 {
		t.Errorf("got %s %v", q, args)
	}
}
