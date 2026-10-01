// Package bench compares barm with bun on the same queries and the same
// database. It lives in its own module so bun stays out of barm's go.mod.
package bench

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

// The same table, described to each library in its own tags. Two structs rather
// than one: both libraries name their marker struct BaseModel, which cannot be
// embedded twice.
type barmUser struct {
	barm.BaseModel `barm:"table:users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

type bunUser struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID        int64     `bun:"id,pk,autoincrement"`
	Name      string    `bun:"name"`
	Email     string    `bun:"email"`
	Age       int       `bun:"age"`
	CreatedAt time.Time `bun:"created_at"`
}

const schema = `CREATE TABLE users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL,
	created_at TIMESTAMP NOT NULL)`

// open seeds one database and hands both libraries a handle on it.
func open(tb testing.TB, rows int) (*barm.DB, *bun.DB) {
	tb.Helper()
	sqldb, err := sql.Open("sqlite", "file:"+tb.TempDir()+"/b.db")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { sqldb.Close() })
	if _, err := sqldb.Exec(schema); err != nil {
		tb.Fatal(err)
	}

	bd := barm.New(sqldb, barm.SQLite)
	if rows > 0 {
		users := make([]*barmUser, rows)
		now := time.Now()
		for i := range users {
			users[i] = &barmUser{Name: "user", Email: "u@x.io", Age: i % 90, CreatedAt: now}
		}
		if _, err := bd.Insert[barmUser]().Values(users...).Exec(context.Background()); err != nil {
			tb.Fatal(err)
		}
	}
	return bd, bun.NewDB(sqldb, sqlitedialect.New())
}

func builders(tb testing.TB) (*barm.DB, *bun.DB) {
	tb.Helper()
	sqldb, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { sqldb.Close() })
	return barm.New(sqldb, barm.SQLite), bun.NewDB(sqldb, sqlitedialect.New())
}

var (
	sinkQuery string
	sinkBytes []byte
	sinkArgs  []any
)

// --- building ---------------------------------------------------------------

func BenchmarkBuildSelect(b *testing.B) {
	bd, bn := builders(b)

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkQuery, sinkArgs, _ = bd.Select[barmUser]().Where("age >= ?", 18).Build()
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			q := bn.NewSelect().Model((*bunUser)(nil)).Where("age >= ?", 18)
			sinkBytes, _ = q.AppendQuery(bn.QueryGen(), nil)
		}
	})
}

func BenchmarkBuildSelectComplex(b *testing.B) {
	bd, bn := builders(b)

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkQuery, sinkArgs, _ = bd.Select[barmUser]().
				Where("age >= ?", 18).
				Where("email LIKE ?", "%@x.io").
				Where("created_at > ?", time.Time{}).
				OrderBy("name").
				Limit(20).
				Offset(40).
				Build()
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			q := bn.NewSelect().Model((*bunUser)(nil)).
				Where("age >= ?", 18).
				Where("email LIKE ?", "%@x.io").
				Where("created_at > ?", time.Time{}).
				Order("name").
				Limit(20).
				Offset(40)
			sinkBytes, _ = q.AppendQuery(bn.QueryGen(), nil)
		}
	})
}

func BenchmarkBuildInsert(b *testing.B) {
	bd, bn := builders(b)
	now := time.Now()

	for _, n := range []int{1, 100} {
		barmRows := make([]*barmUser, n)
		bunRows := make([]*bunUser, n)
		for i := range n {
			barmRows[i] = &barmUser{Name: "user", Email: "u@x.io", Age: i, CreatedAt: now}
			bunRows[i] = &bunUser{Name: "user", Email: "u@x.io", Age: i, CreatedAt: now}
		}
		name := strconv.Itoa(n)
		b.Run("barm/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkQuery, sinkArgs, _ = bd.Insert[barmUser]().Values(barmRows...).Build()
			}
		})
		b.Run("bun/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q := bn.NewInsert().Model(&bunRows)
				sinkBytes, _ = q.AppendQuery(bn.QueryGen(), nil)
			}
		})
	}
}

func BenchmarkBuildUpdate(b *testing.B) {
	bd, bn := builders(b)
	bu := &barmUser{ID: 7, Name: "x", Email: "e", Age: 3, CreatedAt: time.Now()}
	nu := &bunUser{ID: 7, Name: "x", Email: "e", Age: 3, CreatedAt: time.Now()}

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkQuery, sinkArgs, _ = bd.Update[barmUser]().Value(bu).WherePK().Build()
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			q := bn.NewUpdate().Model(nu).WherePK()
			sinkBytes, _ = q.AppendQuery(bn.QueryGen(), nil)
		}
	})
}

// --- executing --------------------------------------------------------------

var (
	sinkBarm []barmUser
	sinkBun  []bunUser
	sinkOne  barmUser
)

func BenchmarkSelectSlice(b *testing.B) {
	for _, rows := range []int{1, 100, 1000} {
		name := map[int]string{1: "1", 100: "100", 1000: "1000"}[rows]
		bd, bn := open(b, rows)
		ctx := b.Context()

		b.Run("barm/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sinkBarm, _ = bd.Select[barmUser]().Slice(ctx)
			}
		})
		b.Run("bun/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// A fresh slice each time, like barm's Slice — reusing capacity
				// here would be measuring a different thing.
				var users []bunUser
				_ = bn.NewSelect().Model(&users).Scan(ctx)
				sinkBun = users
			}
		})
	}
}

func BenchmarkSelectOne(b *testing.B) {
	bd, bn := open(b, 1000)
	ctx := b.Context()

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkOne, _ = bd.Select[barmUser]().Where("id = ?", 500).One(ctx)
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var u bunUser
			_ = bn.NewSelect().Model(&u).Where("id = ?", 500).Scan(ctx)
		}
	})
}

func BenchmarkSelectSeq(b *testing.B) {
	bd, bn := open(b, 1000)
	ctx := b.Context()

	// barm streams; bun has no iterator, so the fair comparison is the slice it
	// would have to collect instead.
	b.Run("barm/seq", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n := 0
			for u, err := range bd.Select[barmUser]().Seq(ctx) {
				if err != nil {
					b.Fatal(err)
				}
				n += len(u.Name)
			}
		}
	})
	b.Run("bun/slice", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var users []bunUser
			if err := bn.NewSelect().Model(&users).Scan(ctx); err != nil {
				b.Fatal(err)
			}
			n := 0
			for _, u := range users {
				n += len(u.Name)
			}
		}
	})
}

func BenchmarkBuildDeleteByPK(b *testing.B) {
	bd, bn := builders(b)
	bu := &barmUser{ID: 7, Name: "x", Email: "e", Age: 3, CreatedAt: time.Now()}
	nu := &bunUser{ID: 7, Name: "x", Email: "e", Age: 3, CreatedAt: time.Now()}

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sinkQuery, sinkArgs, _ = bd.Delete[barmUser]().Value(bu).WherePK().Build()
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			q := bn.NewDelete().Model(nu).WherePK()
			sinkBytes, _ = q.AppendQuery(bn.QueryGen(), nil)
		}
	})
}
