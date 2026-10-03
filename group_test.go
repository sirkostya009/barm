package barm_test

import (
	"strings"
	"testing"

	"github.com/sirkostya009/barm"
)

const usersFrom = `SELECT "u"."id", "u"."name", "u"."email", "u"."age", "u"."created_at" FROM "users" AS "u"`

// A group's OR stays inside its parentheses, and its arguments keep their
// place in the numbering.
func TestWhereGroup(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	q, args, err := db.Select[User]().
		Where("u.team_id = ?", 1).
		WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("u.age > ?", 2).WhereOr("u.name = ?", "x")
		}).
		Where("u.email <> ?", "y").
		Build()
	check(t, q, args, err,
		usersFrom+` WHERE (u.team_id = $1) AND ((u.age > $2) OR (u.name = $3)) AND (u.email <> $4)`,
		1, 2, "x", "y")
}

func TestWhereGroupShapes(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	for _, tc := range []struct {
		name  string
		build func(*barm.SelectQuery[User]) *barm.SelectQuery[User]
		where string
	}{
		{"alone, so unwrapped", func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
				return q.Where("a").WhereOr("b")
			})
		}, ` WHERE (a) OR (b)`},
		{"one condition inside", func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("a").WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] { return q.Where("b") })
		}, ` WHERE (a) AND (b)`},
		{"empty, left out", func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("a").WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] { return q })
		}, ` WHERE a`},
		{"or group", func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("a").WhereOrGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
				return q.Where("b").Where("c")
			})
		}, ` WHERE (a) OR ((b) AND (c))`},
		{"nested", func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("a").WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
				return q.Where("b").WhereOrGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
					return q.Where("c").Where("d")
				})
			})
		}, ` WHERE (a) AND ((b) OR ((c) AND (d)))`},
	} {
		q, _, err := tc.build(db.Select[User]()).Build()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if want := usersFrom + tc.where; q != want {
			t.Errorf("%s:\ngot  %s\nwant %s", tc.name, q, want)
		}
	}
}

// Update and delete group the same way, WherePK beside a group but not in one.
func TestWhereGroupOnWrites(t *testing.T) {
	t.Parallel()
	db := barm.New(nil, barm.Postgres)
	u := &User{ID: 9}
	q, args, err := db.Delete[User]().Value(u).WherePK().
		WhereGroup(func(q *barm.DeleteQuery[User]) *barm.DeleteQuery[User] {
			return q.Where("age < ?", 1).WhereOr("name = ?", "z")
		}).Build()
	check(t, q, args, err, `DELETE FROM "users" WHERE "id" = $1 AND ((age < $2) OR (name = $3))`, int64(9), 1, "z")

	q, args, err = db.Update[User]().Set("age = 0").
		WhereGroup(func(q *barm.UpdateQuery[User]) *barm.UpdateQuery[User] {
			return q.Where("a = ?", 1).WhereOr("b = ?", 2)
		}).Build()
	check(t, q, args, err, `UPDATE "users" SET age = 0 WHERE (a = $1) OR (b = $2)`, 1, 2)

	_, _, err = db.Delete[User]().Value(u).WhereGroup(func(q *barm.DeleteQuery[User]) *barm.DeleteQuery[User] {
		return q.Where("a").WherePK()
	}).Build()
	if err == nil || !strings.Contains(err.Error(), "WherePK") {
		t.Errorf("WherePK in a group: %v", err)
	}
	_, _, err = db.Delete[User]().WhereGroup(func(q *barm.DeleteQuery[User]) *barm.DeleteQuery[User] { return q }).Build()
	if err == nil || !strings.Contains(err.Error(), "no WHERE") {
		t.Errorf("an empty group is no WHERE: %v", err)
	}
	_, _, err = db.Select[User]().WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
		return q.Clone().Where("lost")
	}).Build()
	if err == nil || !strings.Contains(err.Error(), "return the query") {
		t.Errorf("a function returning another query: %v", err)
	}
}
