package integration_test

import (
	"context"
	"database/sql"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

// usersOf is a caller's own helper, small enough to be inlined into its
// caller, which must not move Caller off it.
func usersOf(ctx context.Context, q *barm.SelectQuery[User]) ([]User, error) {
	return q.Slice(ctx) // usersOf's call
}

type userName struct {
	Name string `barm:"name"`
}

//go:noinline
func usersUnder(ctx context.Context, db *barm.DB, age int) ([]User, error) {
	return db.Select[User]().Where("age < ?", age).Slice(ctx) // usersUnder's call
}

// lineOf is the line of file that ends in the comment marker.
func lineOf(t *testing.T, file, marker string) int {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range strings.Split(string(src), "\n") {
		if strings.HasSuffix(strings.TrimSpace(l), marker) {
			return i + 1
		}
	}
	t.Fatalf("no %q in %s", marker, file)
	return 0
}

// Caller is the line in the caller's code that made the call, on every path a
// call can take through barm, so that a layer added in between, or inlined
// differently, cannot move it: each call below is checked against the line it
// sits on.
func TestCallerEverywhere(t *testing.T) {
	ctx := t.Context()
	_, file, _, _ := runtime.Caller(0)
	self := "github.com/sirkostya009/barm/integration_test.TestCallerEverywhere"

	var got []barm.QueryEvent
	sqldb, err := sql.Open("sqlite", "file:"+t.TempDir()+"/caller.db")
	if err != nil {
		t.Fatal(err)
	}
	sqldb.SetMaxOpenConns(1)
	db := barm.New(barm.SQL(sqldb, barm.SQLite, barm.SequentialBatches()), barm.WithCaller(), barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	}))
	defer db.Close()
	for _, q := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, age INTEGER, created_at TIMESTAMP)`,
		`CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)`,
		`CREATE TABLE books (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author_id INTEGER)`,
		`CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, label TEXT)`,
		`INSERT INTO authors (name, email) VALUES ('lem', 'l@x.io')`,
		`INSERT INTO books (title, author_id) VALUES ('Solaris', 1)`,
	} {
		_, err := db.Exec(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
	}

	// check runs f and expects every event it fired to come from the line
	// that ends in the comment naming what.
	check := func(what string, f func()) {
		t.Helper()
		line := lineOf(t, file, "// "+what)
		got = nil
		f()
		if len(got) == 0 {
			t.Errorf("%s: no event", what)
		}
		for _, ev := range got {
			c := ev.Caller
			if c == nil || c.File != file || c.Line != line || !strings.HasPrefix(c.Function, self) {
				t.Errorf("%s: %s came from %+v, want line %d", what, ev.Query, c, line)
			}
		}
	}
	u := func() *User { return &User{Name: "kim", Email: "k@x.io", Age: 30, CreatedAt: time.Now()} }

	check("insert exec", func() { _, _ = db.Insert[User]().Values(u()).Exec(ctx) })                           // insert exec
	check("insert returning", func() { _, _ = db.Insert[User]().Values(u(), u()).Returning("id").Exec(ctx) }) // insert returning
	check("insert one", func() { _, _ = db.Insert[User]().Values(u()).One(ctx) })                             // insert one
	check("insert slice", func() { _, _ = db.Insert[User]().Values(u()).Slice(ctx) })                         // insert slice
	check("select slice", func() { _, _ = db.Select[User]().Slice(ctx) })                                     // select slice
	check("select one", func() { _, _ = db.Select[User]().One(ctx) })                                         // select one
	check("select count", func() { _, _ = db.Select[User]().Count(ctx) })                                     // select count
	check("select exists", func() { _, _ = db.Select[User]().Exists(ctx) })                                   // select exists
	check("select slice as", func() { _, _ = db.Select[User]().SliceAs[userName](ctx) })                      // select slice as
	check("select one as", func() { _, _ = db.Select[User]().OneAs[userName](ctx) })                          // select one as
	check("select seq", func() {
		for range db.Select[User]().Seq(ctx) { // select seq
		}
	})
	check("select rows", func() { rows, _ := db.Select[User]().Rows(ctx); _ = rows.Close() })                             // select rows
	check("handle", func() { _, _ = barm.Handle{IDB: db}.Select[User]().Slice(ctx) })                                     // handle
	check("via", func() { _, _ = db.Select[User]().Via(db).Slice(ctx) })                                                  // via
	check("update", func() { _, _ = db.Update[User]().Set("age = age + 1").Where("age > ?", 0).Exec(ctx) })               // update
	check("delete returning", func() { _, _ = db.Delete[User]().Where("age > ?", 100).Slice(ctx) })                       // delete returning
	check("raw exec", func() { _, _ = db.Exec(ctx, "UPDATE users SET age = age") })                                       // raw exec
	check("raw query", func() { rows, _ := db.Query(ctx, "SELECT 1"); _ = rows.Close() })                                 // raw query
	check("raw builder", func() { _, _ = db.NewRaw("UPDATE users SET age = age").Exec(ctx) })                             // raw builder
	check("relations", func() { _, _ = db.Select[Author]().Relation[Book]("Books").Relation[tagRow]("Tags").Slice(ctx) }) // relations
	nothing := barm.NewBuilder(barm.SQLite, barm.WithCaller(), barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	}))
	check("unsent", func() { _, _ = nothing.Select[User]().Slice(ctx) }) // unsent

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	check("tx select", func() { _, _ = tx.Select[User]().Slice(ctx) })                            // tx select
	check("tx exec", func() { _, _ = tx.Exec(ctx, "UPDATE users SET age = age") })                // tx exec
	check("savepoint", func() { inner, _ := tx.BeginTx(ctx, nil); _ = inner.Commit() })           // savepoint
	check("savepoint rewound", func() { inner, _ := tx.BeginTx(ctx, nil); _ = inner.Rollback() }) // savepoint rewound
	check("commit", func() { _ = tx.Commit() })                                                   // commit
	check("begin and rollback", func() { tx, _ := db.BeginTx(ctx, nil); _ = tx.Rollback() })      // begin and rollback

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	check("conn select", func() { _, _ = conn.Select[User]().Slice(ctx) })                                // conn select
	check("conn tx", func() { tx, _ := conn.BeginTx(ctx, nil); _ = tx.Commit() })                         // conn tx
	check("batch rollback", func() { b := conn.Batch(); b.Begin(); _ = b.Run(ctx); _ = b.Rollback(ctx) }) // batch rollback
	_ = conn.Close()

	// A batch's queries come from where each was queued, and the relations it
	// loads afterwards from where it was run.
	got = nil
	b := db.Batch()
	queued := []string{"// queued begin", "// queued slice", "// queued one", "// queued count", "// queued returning", "// queued commit", "// run"}
	b.Begin()                                                                                 // queued begin
	b.Slice(db.Select[Author]().Relation[Book]("Books"))                                      // queued slice
	b.One(db.Select[User]())                                                                  // queued one
	b.Count(db.Select[User]())                                                                // queued count
	b.Exec(db.Insert[User]().Values(&User{Name: "b", CreatedAt: time.Now()}).Returning("id")) // queued returning
	b.Commit()                                                                                // queued commit
	err = b.Run(ctx)                                                                          // run
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(queued) {
		t.Fatalf("batch events = %d, want %d", len(got), len(queued))
	}
	for i, ev := range got {
		if want := lineOf(t, file, queued[i]); ev.Caller == nil || ev.Caller.Line != want || ev.Caller.Function != self {
			t.Errorf("batch %d, %s: came from %+v, want line %d", i, ev.Query, ev.Caller, want)
		}
	}

	// The first frame outside barm is the caller's own helper, inlined or not.
	for name, call := range map[string]func() ([]User, error){
		"usersOf":    func() ([]User, error) { return usersOf(ctx, db.Select[User]()) },
		"usersUnder": func() ([]User, error) { return usersUnder(ctx, db, 99) },
	} {
		got = nil
		_, err := call()
		if err != nil {
			t.Fatal(err)
		}
		want := lineOf(t, file, "// "+name+"'s call")
		if len(got) != 1 || got[0].Caller == nil || got[0].Caller.Line != want ||
			got[0].Caller.Function != "github.com/sirkostya009/barm/integration_test."+name {
			t.Errorf("%s: came from %+v, want its line %d", name, got, want)
		}
	}
}
