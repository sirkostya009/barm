package barm_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

func BenchmarkBuildSelect(b *testing.B) {
	db := barm.New(nil, barm.Postgres)
	b.ReportAllocs()
	for b.Loop() {
		q, args, err := db.Select[User]().
			Where("u.age >= ?", 18).
			WhereOr("u.name = ?", "root").
			OrderBy("u.id DESC").
			Limit(10).
			Build()
		if err != nil || q == "" || args == nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildInsert(b *testing.B) {
	db := barm.New(nil, barm.Postgres)
	rows := make([]*User, 10)
	for i := range rows {
		rows[i] = &User{Name: "n", Email: "e", Age: i, CreatedAt: time.Now()}
	}
	b.ReportAllocs()
	for b.Loop() {
		q, args, err := db.Insert[User]().Values(rows...).Build()
		if err != nil || q == "" || args == nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExistsQuery(b *testing.B) {
	db := barm.New(nil, barm.Postgres)
	b.ReportAllocs()
	for b.Loop() {
		q, _, err := db.Select[User]().Where("age > ?", 10).ExistsQuery()
		if err != nil || q == "" {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildUpdateByValue(b *testing.B) {
	db := barm.New(nil, barm.Postgres)
	u := &User{ID: 7, Name: "n", Email: "e", Age: 30, CreatedAt: time.Now()}
	b.ReportAllocs()
	for b.Loop() {
		q, args, err := db.Update[User]().Value(u).WherePK().Build()
		if err != nil || q == "" || args == nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildDeleteByValue(b *testing.B) {
	db := barm.New(nil, barm.Postgres)
	u := &User{ID: 7}
	b.ReportAllocs()
	for b.Loop() {
		q, args, err := db.Delete[User]().Value(u).WherePK().Build()
		if err != nil || q == "" || args == nil {
			b.Fatal(err)
		}
	}
}

// A wide insert with argument dedup on: every row carries distinct strings, so
// nothing merges and the cost is all in looking.
func BenchmarkBuildInsertDedup(b *testing.B) {
	db := barm.New(nil, barm.Postgres, barm.WithArgDedup())
	rows := make([]*User, 1000)
	for i := range rows {
		rows[i] = &User{Name: strconv.Itoa(i), Email: strconv.Itoa(i) + "@x", Age: i, CreatedAt: time.Now()}
	}
	b.ReportAllocs()
	for b.Loop() {
		q, args, err := db.Insert[User]().Values(rows...).Build()
		if err != nil || q == "" || args == nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOne(b *testing.B) {
	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1})), barm.Postgres)
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		_, err := db.Select[User]().Where("id = ?", 7).One(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSlice(b *testing.B) {
	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 10})), barm.Postgres)
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		_, err := db.Select[User]().Where("age > ?", 7).Limit(10).Slice(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Via on a query built once should cost what building on the handle does.
func BenchmarkVia(b *testing.B) {
	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1})), barm.Postgres)
	c, err := db.Conn(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	ctx := b.Context()
	b.Run("on-conn", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := c.Select[User]().Where("id = ?", 7).One(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("via", func(b *testing.B) {
		q := db.Select[User]().Where("id = ?", 7)
		b.ReportAllocs()
		for b.Loop() {
			_, err := q.Via(c).One(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// fakeConnector is a database/sql driver that answers every query with the
// same rows of User's shape, so a benchmark measures barm and database/sql
// rather than a database.
type fakeConnector struct{ rows int }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn(c), nil }
func (c fakeConnector) Driver() driver.Driver                        { return nil }

type fakeConn struct{ rows int }

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeDriverRows{n: c.rows}, nil
}

type fakeDriverRows struct{ n, i int }

var (
	fakeCols = []string{"id", "name", "email", "age", "created_at"}
	fakeAt   = time.Unix(1700000000, 0)
)

func (*fakeDriverRows) Columns() []string { return fakeCols }
func (*fakeDriverRows) Close() error      { return nil }

func (r *fakeDriverRows) Next(dest []driver.Value) error {
	if r.i >= r.n {
		return io.EOF
	}
	r.i++
	dest[0], dest[1], dest[2], dest[3], dest[4] = int64(r.i), "name", "e@x", int64(30), fakeAt
	return nil
}
