package pgxdriver_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirkostya009/barm"
	_ "github.com/sirkostya009/barm/pgxdriver"
)

type User struct {
	barm.BaseModel `barm:"table:batch_users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

// open gives a DB on database/sql. The pgx driver underneath is found by the
// registration in pgxdriver's init — nothing here wires it up.
func open(t *testing.T, opts ...barm.Option) *barm.DB {
	t.Helper()
	dsn := os.Getenv("PGDSN")
	if dsn == "" {
		dsn = "postgres://postgres@localhost/postgres"
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	if err := sqldb.Ping(); err != nil {
		t.Skipf("no postgres at %s: %v", dsn, err)
	}
	t.Cleanup(func() { sqldb.Close() })

	for _, q := range []string{
		`DROP TABLE IF EXISTS batch_users`,
		`CREATE TABLE batch_users (
			id bigserial PRIMARY KEY, name text NOT NULL, email text NOT NULL,
			age int NOT NULL, created_at timestamptz NOT NULL)`,
	} {
		if _, err := sqldb.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return barm.New(sqldb, barm.Postgres, opts...)
}

func seed(t *testing.T, db *barm.DB) {
	t.Helper()
	rows := []*User{
		{Name: "ann", Email: "a@x.io", Age: 20, CreatedAt: time.Now()},
		{Name: "bo", Email: "b@x.io", Age: 30, CreatedAt: time.Now()},
		{Name: "cy", Email: "c@x.io", Age: 40, CreatedAt: time.Now()},
	}
	if _, err := db.Insert[User]().Values(rows...).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBatch(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	b := db.Batch()
	adults := b.Slice(db.Select[User]().Where("age >= ?", 30).OrderBy("age"))
	oldest := b.One(db.Select[User]().OrderBy("age DESC"))
	total := b.Count(db.Select[User]())
	has40 := b.Exists(db.Select[User]().Where("age = ?", 40))
	ins := b.Exec(db.Insert[User]().Values(&User{
		Name: "dot", Email: "d@x.io", Age: 50, CreatedAt: time.Now(),
	}))

	if b.Len() != 5 {
		t.Fatalf("len = %d, want 5", b.Len())
	}
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 0 {
		t.Errorf("batch not emptied by Run: %d", b.Len())
	}

	if us := adults.Value(); len(us) != 2 || us[0].Name != "bo" || us[1].Email != "c@x.io" {
		t.Errorf("adults = %+v", us)
	}
	if oldest.Value().Name != "cy" {
		t.Errorf("oldest = %+v", oldest.Value())
	}
	if total.Value() != 3 {
		t.Errorf("total = %d", total.Value())
	}
	if !has40.Value() {
		t.Error("exists = false")
	}
	if ins.Value().RowsAffected != 1 {
		t.Errorf("rows affected = %d", ins.Value().RowsAffected)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 4 {
		t.Errorf("count after batch = %d, want 4", n)
	}
}

// The row type is per query, so one batch mixes as many as it likes.
func TestBatchHeterogeneous(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	type nameAge struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}

	b := db.Batch()
	users := b.Slice(db.Select[User]().OrderBy("age"))
	narrow := b.Slice(db.Select[nameAge]().Table("batch_users").OrderBy("age"))
	total := b.Count(db.Select[User]())

	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(users.Value()) != 3 || users.Value()[0].Email == "" {
		t.Errorf("users = %+v", users.Value())
	}
	if n := narrow.Value(); len(n) != 3 || n[0].Name != "ann" || n[0].Age != 20 {
		t.Errorf("narrow = %+v", n)
	}
	if total.Value() != 3 {
		t.Errorf("total = %d", total.Value())
	}
}

// A loop queues an arbitrary number of queries, each result typed.
func TestBatchLoop(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	ages := []int{20, 30, 40, 99}
	results := make([]*barm.BatchResult[[]User], len(ages))

	b := db.Batch()
	for i, age := range ages {
		results[i] = b.Slice(db.Select[User]().Where("age = ?", age))
	}
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}

	for i, r := range results {
		us := r.Value() // []User, statically
		want := 1
		if ages[i] == 99 {
			want = 0
		}
		if len(us) != want {
			t.Errorf("age %d: got %d rows, want %d", ages[i], len(us), want)
		}
	}
}

func TestBatchError(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	b := db.Batch()
	b.Slice(db.Select[User]())
	bad := b.Slice(db.Select[any]().Table("no_such_table"))
	after := b.Count(db.Select[User]())

	if err := b.Run(ctx); err == nil {
		t.Fatal("expected an error")
	}
	if bad.Err() == nil {
		t.Error("failing query has no error")
	}
	if !errors.Is(after.Err(), barm.ErrNotRun) {
		t.Errorf("after.Err() = %v, want ErrNotRun", after.Err())
	}
}

func TestBatchNoRows(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	b := db.Batch()
	missing := b.One(db.Select[User]().Where("age = ?", 999))

	if err := b.Run(ctx); err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(missing.Err(), barm.ErrNoRows) {
		t.Errorf("err = %v, want ErrNoRows", missing.Err())
	}
}

// A write batch reports per query, and lands.
func TestBatchExec(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	b := db.Batch()
	one := b.Exec(db.Insert[User]().Values(&User{Name: "x", Email: "x@x.io", Age: 1, CreatedAt: time.Now()}))
	two := b.Exec(db.Insert[User]().Values(&User{Name: "y", Email: "y@x.io", Age: 2, CreatedAt: time.Now()}))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if one.Value().RowsAffected != 1 || two.Value().RowsAffected != 1 {
		t.Errorf("rows affected = %d, %d", one.Value().RowsAffected, two.Value().RowsAffected)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 2 {
		t.Errorf("count = %d, want 2", n)
	}
}

// The batch borrows the transaction's own connection, so it is part of it. That
// connection has to be one barm can reach, which means beginning on a Conn: a
// *sql.Tx offers no way down to the driver.
func TestBatchInsideTx(t *testing.T) {
	ctx := t.Context()
	db := open(t)

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

	b := tx.Batch() // no driver handle at the call site
	b.Exec(tx.Insert[User]().Values(&User{Name: "eve", Email: "e@x.io", Age: 1, CreatedAt: time.Now()}))
	b.Exec(tx.Insert[User]().Values(&User{Name: "fay", Email: "f@x.io", Age: 2, CreatedAt: time.Now()}))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// the batched rows are visible inside the transaction ...
	inTx, err := tx.Select[User]().Count(ctx)
	if err != nil || inTx != 2 {
		t.Fatalf("count in tx = %d, err = %v", inTx, err)
	}
	// ... and vanish with it
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 0 {
		t.Errorf("count after rollback = %d, want 0 — the batch escaped the transaction", n)
	}
}

// A bare-bones value goes in; the full row, defaults and all, comes back.
func TestInsertReturningAs(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	type bare struct {
		Name  string `barm:"name"`
		Email string `barm:"email"`
	}

	// created_at and age have defaults in this table, and id is generated
	if _, err := db.Exec(`ALTER TABLE batch_users
		ALTER COLUMN age SET DEFAULT 42,
		ALTER COLUMN created_at SET DEFAULT now()`); err != nil {
		t.Fatal(err)
	}

	full, err := db.Insert[bare]().Table("batch_users").
		Values(&bare{Name: "gil", Email: "g@x.io"}).
		OneAs[User](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if full.ID == 0 || full.Name != "gil" || full.Age != 42 || full.CreatedAt.IsZero() {
		t.Errorf("full = %+v — the generated columns did not come back", full)
	}

	// an explicit Returning narrows it
	id, err := db.Insert[bare]().Table("batch_users").
		Values(&bare{Name: "hal", Email: "h@x.io"}).
		Returning("id").
		OneAs[int64](ctx)
	if err != nil || id == 0 {
		t.Fatalf("id = %d, err = %v", id, err)
	}

	// a multi-row insert reads every generated key back
	ids, err := db.Insert[bare]().Table("batch_users").
		Values(&bare{Name: "i", Email: "i@x.io"}, &bare{Name: "j", Email: "j@x.io"}).
		Returning("id").
		SliceAs[int64](ctx)
	if err != nil || len(ids) != 2 || ids[0] == 0 || ids[1] == 0 {
		t.Fatalf("ids = %v, err = %v", ids, err)
	}
}

// A write knows its own model, so the plain terminals need no type argument.
func TestWriteOwnModel(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	// Insert[User] returns User
	u, err := db.Insert[User]().Values(&User{
		Name: "zed", Email: "z@x.io", Age: 60, CreatedAt: time.Now(),
	}).One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u.ID == 0 || u.Name != "zed" {
		t.Errorf("u = %+v", u)
	}

	// Update[User] returns User
	bumped, err := db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "zed").One(ctx)
	if err != nil || bumped.Age != 61 {
		t.Fatalf("bumped = %+v, err = %v", bumped, err)
	}

	// Delete[User] streams User
	seen := 0
	for row, err := range db.Delete[User]().Where("age >= ?", 30).Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if row.Email == "" {
			t.Errorf("row = %+v", row)
		}
		seen++
	}
	if seen != 3 {
		t.Errorf("deleted %d rows, want 3", seen)
	}

	// Slice too
	left, err := db.Delete[User]().Where("age < ?", 30).Slice(ctx)
	if err != nil || len(left) != 1 || left[0].Name != "ann" {
		t.Fatalf("left = %+v, err = %v", left, err)
	}
}

// SeqAs streams what a write returned, without collecting it.
// A multi-row insert knows exactly how many rows RETURNING will produce, so the
// slice comes back sized rather than grown.
func TestInsertSliceIsPresized(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	rows := make([]*User, 8)
	for i := range rows {
		rows[i] = &User{Name: "n", Email: "e", Age: i, CreatedAt: time.Now()}
	}
	got, err := db.Insert[User]().Values(rows...).SliceAs[User](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 8 {
		t.Fatalf("got %d rows, want 8", len(got))
	}
	if cap(got) != 8 {
		t.Errorf("cap = %d, want 8 — the row count should have sized the slice", cap(got))
	}
}

func TestWriteSeqAs(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	// stream the rows a bulk delete removed
	type nameAge struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}
	seen := 0
	for row, err := range db.Delete[User]().Where("age >= ?", 30).SeqAs[nameAge](ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if row.Name == "" || row.Age < 30 {
			t.Errorf("row = %+v", row)
		}
		seen++
	}
	if seen != 2 {
		t.Errorf("streamed %d rows, want 2", seen)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 1 {
		t.Errorf("count = %d, want 1", n)
	}

	// stopping early stops the reading, not the write
	seed(t, db) // three more rows, ages 20/30/40
	stopped := 0
	for _, err := range db.Update[User]().Set("age = age + ?", 100).
		Where("age < ?", 100).SeqAs[User](ctx) {
		if err != nil {
			t.Fatal(err)
		}
		stopped++
		break
	}
	if stopped != 1 {
		t.Fatalf("read %d rows before breaking", stopped)
	}
	n, err := db.Select[User]().Where("age >= ?", 100).Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("%d rows updated, want 4 — breaking must not undo the write", n)
	}
}

// A write's One maps its RETURNING columns by the names the result reports,
// whoever wrote the clause.
func TestWriteOneMapping(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	// named by the result type: maps fine
	u, err := db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").One(ctx)
	if err != nil || u.Age != 21 || u.Name != "ann" {
		t.Fatalf("u = %+v, err = %v", u, err)
	}

	// a Returning of its own into a scalar
	n, err := db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").
		Returning("age").OneAs[int](ctx)
	if err != nil || n != 22 {
		t.Fatalf("n = %d, err = %v", n, err)
	}

	// and into a struct
	u, err = db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").
		Returning("id, name, age").OneAs[User](ctx)
	if err != nil || u.Name != "ann" || u.Age != 23 {
		t.Fatalf("u = %+v, err = %v", u, err)
	}
	rows, err := db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").
		Returning("id, name").SliceAs[struct {
		ID   int64  `barm:"id"`
		Name string `barm:"name"`
	}](ctx)
	if err != nil || len(rows) != 1 || rows[0].Name != "ann" {
		t.Fatalf("rows = %+v, err = %v", rows, err)
	}
}

func TestUpdateDeleteReturningAs(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	// update hands back the row it wrote
	updated, err := db.Update[User]().
		Set("age = age + ?", 1).
		Where("name = ?", "ann").
		OneAs[User](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Name != "ann" || updated.Age != 21 {
		t.Errorf("updated = %+v", updated)
	}

	// delete hands back what it removed
	type nameAge struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}
	gone, err := db.Delete[User]().Where("age >= ?", 30).SliceAs[nameAge](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gone) != 2 {
		t.Fatalf("gone = %+v, want 2 rows", gone)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 1 {
		t.Errorf("count = %d, want 1", n)
	}
}

func TestBatchEmpty(t *testing.T) {
	db := open(t)
	if err := db.Batch().Run(t.Context()); err != nil {
		t.Errorf("empty batch: %v", err)
	}
}

// A pipelined batch reports each of its queries to the hooks, the same as any
// other query does.
func TestBatchQueryHooks(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var ops []string
	var rows int64
	db := open(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			ops = append(ops, ev.Op)
			if ev.Result != nil {
				n, err := ev.Result.RowsAffected()
				if err != nil {
					t.Errorf("RowsAffected: %v", err)
				}
				rows += n
			}
		},
	}))
	seed(t, db)

	b := db.Batch()
	b.Slice(db.Select[User]())
	b.Count(db.Select[User]())
	b.Exec(db.Update[User]().Set("age = age + 1").Where("TRUE"))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	// The seed runs its own queries first, so only the tail is the batch.
	if len(ops) < 3 {
		t.Fatalf("ops = %v", ops)
	}
	if got := ops[len(ops)-3:]; !slices.Equal(got, []string{"SELECT", "SELECT", "UPDATE"}) {
		t.Errorf("batch ops = %v", got)
	}
	if rows == 0 {
		t.Error("the batch exec reported no rows affected to the hook")
	}
}

// A transaction holds its own connection, so a batch inside one is part of it
// whichever way the transaction was begun.
func TestBatchInTxFromDB(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	b := tx.Batch()
	b.Exec(tx.Insert[User]().Values(&User{Name: "eve", Email: "e@x.io", Age: 1, CreatedAt: time.Now()}))
	b.Exec(tx.Insert[User]().Values(&User{Name: "fay", Email: "f@x.io", Age: 2, CreatedAt: time.Now()}))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := tx.Select[User]().Count(ctx); err != nil || n != 2 {
		t.Fatalf("count in tx = %d, err = %v", n, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.Select[User]().Count(ctx); n != 0 {
		t.Errorf("count after rollback = %d, want 0 — the batch escaped the transaction", n)
	}
}

// --- relations ---------------------------------------------------------------

type RelAuthor struct {
	barm.BaseModel `barm:"table:rel_authors,alias:a"`

	ID    int64     `barm:"id,pk,autoincrement"`
	Name  string    `barm:"name"`
	Books []RelBook `barm:"rel:id=author_id"`
	Tags  []RelTag  `barm:"rel:id=author_id"`
}

type RelBook struct {
	barm.BaseModel `barm:"table:rel_books"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Title    string `barm:"title"`
}

type RelTag struct {
	barm.BaseModel `barm:"table:rel_tags"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Label    string `barm:"label"`
}

type relBookLite struct {
	Title string `barm:"title"`
}

type relTagLite struct {
	Label string `barm:"label"`
}

type relAuthorLite struct {
	ID    int64         `barm:"id,pk"`
	Name  string        `barm:"name"`
	Books []relBookLite `barm:"rel:id=author_id"`
	Tags  []relTagLite  `barm:"rel:id=author_id"`
}

// Two relations wait on the same parent keys and on nothing else, so pgx sends
// them together: two round trips for the lot, not three.
func TestRelationsAreBatched(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var seq []string
	note := func(what, q string) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.Contains(q, "rel_books"):
			seq = append(seq, what+":books")
		case strings.Contains(q, "rel_tags"):
			seq = append(seq, what+":tags")
		}
	}
	db := open(t, barm.WithHook(barm.QueryHook{
		BeforeQuery: func(c context.Context, ev *barm.QueryEvent) context.Context {
			note("before", ev.Query)
			return c
		},
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { note("after", ev.Query) },
	}))
	seedRel(t, db)

	mu.Lock()
	seq = nil
	mu.Unlock()

	authors, err := db.Select[RelAuthor]().OrderBy("id").
		Relation[relBookLite]("Books").
		Relation[relTagLite]("Tags").
		SliceAs[relAuthorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 2 {
		t.Fatalf("authors = %d", len(authors))
	}
	if len(authors[0].Books) != 2 || len(authors[0].Tags) != 1 {
		t.Errorf("lem = books %+v tags %+v", authors[0].Books, authors[0].Tags)
	}
	if len(authors[1].Books) != 1 || len(authors[1].Tags) != 0 {
		t.Errorf("borges = books %+v tags %+v", authors[1].Books, authors[1].Tags)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seq) != 4 {
		t.Fatalf("relation events = %v, want two apiece", seq)
	}
	// Pipelined, both go out before either comes back; run one at a time, the
	// second cannot start until the first has finished.
	if !strings.HasPrefix(seq[1], "before") {
		t.Errorf("relations ran one at a time, not batched: %v", seq)
	}
}

// A transaction begun on a DB cannot reach the driver, so its relations fall
// back to a query apiece — same rows, more round trips.
func TestRelationsFallBackInsideTx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`,
		`CREATE TABLE rel_authors (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE rel_books (id bigserial PRIMARY KEY, author_id bigint, title text)`,
		`CREATE TABLE rel_tags (id bigserial PRIMARY KEY, author_id bigint, label text)`,
		`INSERT INTO rel_authors (id, name) VALUES (1, 'lem')`,
		`INSERT INTO rel_books (author_id, title) VALUES (1, 'Solaris')`,
		`INSERT INTO rel_tags (author_id, label) VALUES (1, 'sf')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`) })

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	authors, err := tx.Select[RelAuthor]().
		Relation[relBookLite]("Books").
		Relation[relTagLite]("Tags").
		SliceAs[relAuthorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 1 || len(authors[0].Books) != 1 || len(authors[0].Tags) != 1 {
		t.Fatalf("authors = %+v", authors)
	}
}

// Keys go as one array parameter, so the SQL stays the same however many there
// are and the driver can reuse the statement.
func TestRelationKeysUseAnyArray(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var child string
	db := open(t, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(ev.Query, "rel_books") {
				child = ev.Query
			}
		},
	}))
	seedRel(t, db)

	if _, err := db.Select[RelAuthor]().Relation[relBookLite]("Books").SliceAs[relAuthorLite](ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(child, "= ANY(") {
		t.Errorf("child query = %s, want an array parameter", child)
	}
	if strings.Contains(child, "IN (") {
		t.Errorf("child query = %s, want no value list", child)
	}
}

// A transaction begun on a Conn holds a connection barm can reach, so its
// relations batch like any other query's.
func TestRelationsBatchInConnTx(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var seq []string
	note := func(what, q string) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.Contains(q, "rel_books"):
			seq = append(seq, what+":books")
		case strings.Contains(q, "rel_tags"):
			seq = append(seq, what+":tags")
		}
	}
	db := open(t, barm.WithHook(barm.QueryHook{
		BeforeQuery: func(c context.Context, ev *barm.QueryEvent) context.Context {
			note("before", ev.Query)
			return c
		},
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { note("after", ev.Query) },
	}))
	seedRel(t, db)

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

	mu.Lock()
	seq = nil
	mu.Unlock()

	authors, err := tx.Select[RelAuthor]().OrderBy("id").
		Relation[relBookLite]("Books").
		Relation[relTagLite]("Tags").
		SliceAs[relAuthorLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 2 || len(authors[0].Books) != 2 || len(authors[0].Tags) != 1 {
		t.Fatalf("authors = %+v", authors)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seq) != 4 {
		t.Fatalf("relation events = %v", seq)
	}
	if !strings.HasPrefix(seq[1], "before") {
		t.Errorf("relations ran one at a time inside the transaction: %v", seq)
	}
}

// seedRel gives two authors, three books and one tag.
func seedRel(t *testing.T, db *barm.DB) {
	t.Helper()
	for _, q := range []string{
		`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`,
		`CREATE TABLE rel_authors (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE rel_books (id bigserial PRIMARY KEY, author_id bigint, title text)`,
		`CREATE TABLE rel_tags (id bigserial PRIMARY KEY, author_id bigint, label text)`,
		`INSERT INTO rel_authors (id, name) VALUES (1, 'lem'), (2, 'borges')`,
		`INSERT INTO rel_books (author_id, title) VALUES (1, 'Solaris'), (1, 'Cyberiad'), (2, 'Ficciones')`,
		`INSERT INTO rel_tags (author_id, label) VALUES (1, 'sf')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`) })
}

// A statement that fails inside a savepoint aborts the whole transaction until
// something rewinds it. The nested Commit then fails too, and the deferred
// Rollback is what rewinds — so the failed Commit must leave it something to do.
func TestNestedCommitFailureLeavesRollback(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	func() {
		in, err := tx.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer in.Rollback()
		if _, err := in.ExecContext(ctx, "SELECT 1/0"); err == nil {
			t.Fatal("expected division by zero")
		}
		if err := in.Commit(); err == nil {
			t.Fatal("expected the release to fail in an aborted transaction")
		}
	}()

	var n int
	if err := tx.QueryRowContext(ctx, "SELECT 1").Scan(&n); err != nil {
		t.Fatalf("outer transaction after the nested one failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// A batch has every row before any relation could be asked for, so relations
// load once it is done, rather than silently not at all.
func TestBatchLoadsRelations(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seedRel(t, db)

	b := db.Batch()
	all := b.Slice(db.Select[RelAuthor]().OrderBy("id").Relation[RelBook]("Books"))
	one := b.One(db.Select[RelAuthor]().Where("id = ?", 2).Relation[RelBook]("Books"))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	authors, err := all.Get()
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 2 || len(authors[0].Books) != 2 || len(authors[1].Books) != 1 {
		t.Errorf("authors = %+v", authors)
	}
	borges, err := one.Get()
	if err != nil {
		t.Fatal(err)
	}
	if len(borges.Books) != 1 || borges.Books[0].Title != "Ficciones" {
		t.Errorf("borges = %+v", borges)
	}
}

// Count and Exists stand in their own projection for the query's. Where the
// query's shape decides what a row is — groups, distinct rows, union branches,
// locked rows — that is not enough, and the query is counted as a whole.
func TestCountAndExistsKeepTheQueryShape(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	if _, err := db.Insert[User]().Values(&User{Name: "ann", Email: "a2@x.io", Age: 50, CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	type name struct {
		Name string `barm:"name"`
	}
	young := func() *barm.SelectQuery[User] { return db.Select[User]().Where("age < 35") }
	old := db.Select[User]().Where("age > 45")

	for _, tc := range []struct {
		what string
		q    *barm.SelectQuery[User]
		want int64
	}{
		{"groups", db.Select[User]().Column("name").GroupBy("name"), 3},
		{"groups with having", db.Select[User]().Column("name").GroupBy("name").Having("count(*) > 1"), 1},
		{"distinct", db.Select[User]().Column("name").Distinct(), 3},
		{"union", young().Union(old), 3},
		{"union all", young().UnionAll(old).OrderBy("age").Limit(1), 3},
	} {
		n, err := tc.q.Count(ctx)
		if err != nil || n != tc.want {
			t.Errorf("%s: count = %d, %v, want %d", tc.what, n, err, tc.want)
		}
	}

	ok, err := db.Select[User]().Where("age > 100").Union(old).Exists(ctx)
	if err != nil || !ok {
		t.Errorf("exists over a union = %v, %v", ok, err)
	}
	names, err := young().UnionAll(old).OrderBy("age").SliceAs[name](ctx)
	if err != nil || len(names) != 3 || names[2].Name != "ann" {
		t.Errorf("narrowed union = %+v, %v", names, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if n, err := tx.Select[User]().For("UPDATE").Count(ctx); err != nil || n != 4 {
		t.Errorf("locked count = %d, %v", n, err)
	}
	if ok, err := tx.Select[User]().For("UPDATE").Exists(ctx); err != nil || !ok {
		t.Errorf("locked exists = %v, %v", ok, err)
	}
}

type byteaParent struct {
	barm.BaseModel `barm:"table:bp"`

	Key  []byte       `barm:"k"`
	Kids []byteaChild `barm:"rel:k=pk"`
}

type byteaChild struct {
	barm.BaseModel `barm:"table:bc"`

	V string `barm:"v"`
}

// On Postgres the keys go out as one bytea[] parameter.
func TestRelationByteaKey(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS bp, bc`,
		`CREATE TABLE bp (k bytea)`, `CREATE TABLE bc (pk bytea, v text)`,
		`INSERT INTO bp VALUES ('\x01'), ('\x02')`,
		`INSERT INTO bc VALUES ('\x01', 'a'), ('\x01', 'b'), ('\x02', 'c')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS bp, bc`) })
	got, err := db.Select[byteaParent]().OrderBy("k").Relation[byteaChild]("Kids").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Kids) != 2 || len(got[1].Kids) != 1 {
		t.Errorf("parents = %+v", got)
	}
}

type treeNode struct {
	barm.BaseModel `barm:"table:nodes"`

	ID       int64  `barm:"id"`
	ParentID *int64 `barm:"parent_id"`
}

// The README's recursive example, as written there: a doc that does not run
// is worse than none.
func TestReadmeRecursiveExample(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS nodes`,
		`CREATE TABLE nodes (id bigint PRIMARY KEY, parent_id bigint)`,
		`INSERT INTO nodes VALUES (1, NULL), (2, 1), (3, 2), (4, NULL)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS nodes`) })
	type node = treeNode

	got, err := db.Select[node]().Table("tree").
		WithRecursive("tree", db.Select[node]().Where("parent_id IS NULL").
			UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id"))).
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Errorf("tree = %+v, want all four nodes", got)
	}
}

// Exec on an insert with RETURNING scans the returned columns back into the
// values it was given, batched or not.
func TestBatchExecReturningWritesBack(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	a := &User{Name: "a", Email: "a@x", Age: 1, CreatedAt: time.Now()}
	b1 := &User{Name: "b", Email: "b@x", Age: 2, CreatedAt: time.Now()}
	c := &User{Name: "c", Email: "c@x", Age: 3, CreatedAt: time.Now()}

	b := db.Batch()
	one := b.Exec(db.Insert[User]().Values(a).Returning("id"))
	two := b.Exec(db.Insert[User]().Values(b1, c).Returning("id"))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if a.ID == 0 || b1.ID == 0 || c.ID == 0 || b1.ID == c.ID {
		t.Errorf("ids = %d, %d, %d", a.ID, b1.ID, c.ID)
	}
	if one.Value().RowsAffected != 1 || two.Value().RowsAffected != 2 {
		t.Errorf("rows affected = %d, %d", one.Value().RowsAffected, two.Value().RowsAffected)
	}
}

type bfsAuthor struct {
	barm.BaseModel `barm:"table:rel_authors"`

	ID    int64      `barm:"id,pk"`
	Books []nestBook `barm:"rel:id=author_id"`
	Tags  []bfsTag   `barm:"rel:id=author_id"`
}

type bfsTag struct {
	barm.BaseModel `barm:"table:rel_tags"`

	ID       int64     `barm:"id,pk"`
	AuthorID int64     `barm:"author_id"`
	Label    string    `barm:"label"`
	Notes    []tagNote `barm:"rel:id=tag_id"`
}

type tagNote struct {
	barm.BaseModel `barm:"table:rel_tag_notes"`

	TagID int64  `barm:"tag_id"`
	Body  string `barm:"body"`
}

// relationEvents records when each relation query starts and finishes, by the
// table it reads.
type relationEvents struct {
	mu  sync.Mutex
	seq []string
}

func (e *relationEvents) hook() barm.QueryHook {
	note := func(what, q string) {
		for _, table := range []string{"rel_books", "rel_tags", "rel_reviews", "rel_tag_notes"} {
			if strings.Contains(q, `FROM "`+table+`"`) {
				e.mu.Lock()
				e.seq = append(e.seq, what+":"+table)
				e.mu.Unlock()
			}
		}
	}
	return barm.QueryHook{
		BeforeQuery: func(c context.Context, ev *barm.QueryEvent) context.Context { note("before", ev.Query); return c },
		AfterQuery:  func(_ context.Context, ev *barm.QueryEvent) { note("after", ev.Query) },
	}
}

// together reports whether every query of the pair started before either one
// finished, which is what going out in one batch looks like.
func (e *relationEvents) together(a, b string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	first := slices.IndexFunc(e.seq, func(s string) bool { return s == "after:"+a || s == "after:"+b })
	return first >= 0 && slices.Contains(e.seq[:first], "before:"+a) && slices.Contains(e.seq[:first], "before:"+b)
}

func seedDeepRel(t *testing.T, db *barm.DB) {
	t.Helper()
	seedRel(t, db)
	for _, q := range []string{
		`DROP TABLE IF EXISTS rel_reviews, rel_tag_notes`,
		`CREATE TABLE rel_reviews (book_id bigint, body text)`,
		`CREATE TABLE rel_tag_notes (tag_id bigint, body text)`,
		`INSERT INTO rel_reviews VALUES (1, 'great'), (1, 'dense'), (3, 'short')`,
		`INSERT INTO rel_tag_notes VALUES (1, 'classic')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS rel_reviews, rel_tag_notes`) })
}

// Relations load a depth at a time: everything at one depth goes out together,
// whichever branch of the tree it hangs off — one round trip per depth, not
// one per relation that has relations of its own.
func TestRelationsLoadByDepth(t *testing.T) {
	ctx := t.Context()
	var ev relationEvents
	db := open(t, barm.WithHook(ev.hook()))
	seedDeepRel(t, db)

	authors, err := db.Select[bfsAuthor]().OrderBy("id").
		Relation("Books", func(q *barm.SelectQuery[nestBook]) *barm.SelectQuery[nestBook] {
			return q.OrderBy("id").Relation[nestReview]("Reviews")
		}).
		Relation("Tags", func(q *barm.SelectQuery[bfsTag]) *barm.SelectQuery[bfsTag] {
			return q.Relation[tagNote]("Notes")
		}).
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lem := authors[0]
	if len(lem.Books) != 2 || len(lem.Books[0].Reviews) != 2 || len(lem.Tags) != 1 || len(lem.Tags[0].Notes) != 1 {
		t.Fatalf("lem = %+v", lem)
	}
	if !ev.together("rel_books", "rel_tags") {
		t.Errorf("depth 1 went out separately: %v", ev.seq)
	}
	if !ev.together("rel_reviews", "rel_tag_notes") {
		t.Errorf("depth 2 went out separately: %v", ev.seq)
	}
}

// Relations of different queries in one batch share their round trips too.
func TestBatchRelationsLoadTogether(t *testing.T) {
	ctx := t.Context()
	var ev relationEvents
	db := open(t, barm.WithHook(ev.hook()))
	seedDeepRel(t, db)

	b := db.Batch()
	books := b.Slice(db.Select[RelAuthor]().OrderBy("id").Relation[RelBook]("Books"))
	tags := b.One(db.Select[bfsAuthor]().Where("id = ?", 1).Relation[bfsTag]("Tags"))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(books.Value()) != 2 || len(books.Value()[0].Books) != 2 || len(tags.Value().Tags) != 1 {
		t.Fatalf("books %+v, tags %+v", books.Value(), tags.Value())
	}
	if !ev.together("rel_books", "rel_tags") {
		t.Errorf("the queries' relations went out separately: %v", ev.seq)
	}
}

// syncCounter proxies a Postgres connection and counts the Sync messages the
// client sends, one per extended-protocol round trip, and the Parse messages,
// one per statement prepared.
type syncCounter struct {
	n      atomic.Int64
	parses atomic.Int64
	ln     net.Listener
}

func newSyncCounter(t *testing.T, upstream string) *syncCounter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sc := &syncCounter{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				client.Close()
				return
			}
			go func() { io.Copy(client, server); client.Close() }()
			go func() { sc.pump(server, client); server.Close() }()
		}
	}()
	return sc
}

// pump forwards the client's messages, counting the Syncs among them. The
// startup message comes first and carries no type byte.
func (sc *syncCounter) pump(dst io.Writer, src io.Reader) {
	r := bufio.NewReader(src)
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:4]); err != nil {
		return
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	startup := make([]byte, n)
	copy(startup, hdr[:4])
	if _, err := io.ReadFull(r, startup[4:]); err != nil {
		return
	}
	dst.Write(startup)
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[1:])-4)
		if _, err := io.ReadFull(r, body); err != nil {
			return
		}
		switch hdr[0] {
		case 'S':
			sc.n.Add(1)
		case 'P':
			sc.parses.Add(1)
		}
		dst.Write(hdr[:])
		dst.Write(body)
	}
}

// openCounted opens a DB whose connections go through a Sync counter.
func openCounted(t *testing.T) (*barm.DB, *syncCounter) {
	t.Helper()
	db := open(t) // creates the table
	u, err := url.Parse(os.Getenv("PGDSN"))
	if err != nil || u.Host == "" {
		t.Skip("needs a PGDSN URL to proxy")
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":5432"
	}
	sc := newSyncCounter(t, host)
	u.Host = sc.ln.Addr().String()
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	sqldb, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqldb.Close() })
	_ = db
	return barm.New(sqldb, barm.Postgres), sc
}

// A named statement is prepared on the connection that runs it, in the same
// round trip as its first execution; every later one is a round trip too.
func TestPreparedIsOneRoundTrip(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t)
	seed(t, db)
	db.SetMaxOpenConns(1)

	for i, parses := range []int64{1, 0} { // prepared once, run by name after
		before, parsedBefore := sc.n.Load(), sc.parses.Load()
		u, err := db.Select[User]().Where("age = ?", 20).Prepare("by_age").One(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := sc.n.Load() - before; got != 1 || u.Name != "ann" {
			t.Errorf("call %d: %d round trips, name %q", i, got, u.Name)
		}
		if got := sc.parses.Load() - parsedBefore; got != parses {
			t.Errorf("call %d: %d parses, want %d", i, got, parses)
		}
	}
	before := sc.n.Load()
	us, err := db.Select[User]().Where("age >= ?", 30).OrderBy("age").Prepare("adults").Slice(ctx)
	if err != nil || len(us) != 2 {
		t.Fatalf("slice = %+v, %v", us, err)
	}
	res, err := db.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").Prepare("bump").Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("rows affected = %d", n)
	}
	if got := sc.n.Load() - before; got != 2 {
		t.Errorf("slice and exec took %d round trips, want 2", got)
	}
}

// Inside a transaction the statement is prepared on the transaction's own
// connection, so a pool with nothing else free is no obstacle.
func TestPreparedInTxOnFullPool(t *testing.T) {
	db := open(t)
	seed(t, db)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	u, err := tx.Select[User]().Where("age = ?", 30).Prepare("tx_by_age").One(ctx)
	if err != nil || u.Name != "bo" {
		t.Fatalf("one = %+v, %v", u, err)
	}
	if _, err := tx.Insert[User]().Values(&User{Name: "dot", Email: "d@x", Age: 9, CreatedAt: time.Now()}).Prepare("tx_ins").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	us, err := tx.Select[User]().Where("age < ?", 25).OrderBy("age").Prepare("tx_young").Slice(ctx)
	if err != nil || len(us) != 2 {
		t.Fatalf("slice = %+v, %v", us, err)
	}
}

// A statement the server has forgotten — DEALLOCATE, or a pooler resetting the
// session — is prepared again rather than failing every call after.
func TestPreparedSurvivesDeallocate(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for range 2 {
		u, err := c.Select[User]().Where("age = ?", 40).Prepare("forgotten").One(ctx)
		if err != nil || u.Name != "cy" {
			t.Fatalf("one = %+v, %v", u, err)
		}
		if _, err := c.ExecContext(ctx, "DEALLOCATE ALL"); err != nil {
			t.Fatal(err)
		}
	}
}

// The hooks see a statement run through the driver exactly as any other query.
func TestPreparedHooksThroughDriver(t *testing.T) {
	ctx := t.Context()
	var before, after int
	var names []string
	db := open(t, barm.WithHook(barm.QueryHook{
		BeforeQuery: func(c context.Context, _ *barm.QueryEvent) context.Context { before++; return c },
		AfterQuery:  func(_ context.Context, ev *barm.QueryEvent) { after++; names = append(names, ev.Prepared) },
	}))
	seed(t, db)
	before, after, names = 0, 0, nil

	if _, err := db.Select[User]().Where("age = ?", 999).Prepare("none").One(ctx); !errors.Is(err, barm.ErrNoRows) {
		t.Fatalf("err = %v, want ErrNoRows", err)
	}
	if _, err := db.Select[User]().Prepare("all").Slice(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Delete[User]().Where("age = ?", 999).Prepare("del").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if before != 3 || after != 3 || !slices.Equal(names, []string{"none", "all", "del"}) {
		t.Errorf("before %d after %d names %v", before, after, names)
	}
}

type pgAcct struct {
	barm.BaseModel `barm:"table:pg_accts"`

	ID    int64  `barm:"id,pk,autoincrement"`
	Email string `barm:"email"`
	Name  string `barm:"name"`
}

// DO UPDATE with Set hands back every row, which go back in order; DO NOTHING
// that skipped a value writes none rather than the wrong ones.
func TestOnSetReturning(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS pg_accts`,
		`CREATE TABLE pg_accts (id bigserial PRIMARY KEY, email text UNIQUE, name text)`,
		`INSERT INTO pg_accts (email, name) VALUES ('b@x', 'b')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS pg_accts`) })
	idOf := func(email string) int64 {
		var id int64
		if err := db.QueryRowContext(ctx, `SELECT id FROM pg_accts WHERE email = $1`, email).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}

	up, fresh := &pgAcct{Email: "b@x", Name: "renamed"}, &pgAcct{Email: "d@x", Name: "d"}
	if _, err := db.Insert[pgAcct]().Values(up, fresh).
		On("CONFLICT (email) DO UPDATE").Set("name = excluded.name").Returning("id").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if up.ID != idOf("b@x") || fresh.ID != idOf("d@x") {
		t.Errorf("do update: up %d, fresh %d", up.ID, fresh.ID)
	}

	dup, a := &pgAcct{Email: "b@x", Name: "dup"}, &pgAcct{Email: "a@x", Name: "a"}
	_, err := db.Insert[pgAcct]().Values(dup, a).On("CONFLICT (email) DO NOTHING").Returning("id").Exec(ctx)
	if err == nil || dup.ID != 0 || a.ID != 0 {
		t.Errorf("do nothing: err %v, dup %d, a %d", err, dup.ID, a.ID)
	}
}

type nestAuthor struct {
	barm.BaseModel `barm:"table:rel_authors"`

	ID    int64      `barm:"id,pk"`
	Books []nestBook `barm:"rel:id=author_id"`
	Tags  []RelTag   `barm:"rel:id=author_id"`
}

type nestBook struct {
	barm.BaseModel `barm:"table:rel_books"`

	ID       int64        `barm:"id,pk"`
	AuthorID int64        `barm:"author_id"`
	Title    string       `barm:"title"`
	Reviews  []nestReview `barm:"rel:id=book_id"`
}

type nestReview struct {
	barm.BaseModel `barm:"table:rel_reviews"`

	BookID int64  `barm:"book_id"`
	Body   string `barm:"body"`
}

// A relation of a relation, loaded beside a sibling, goes out as a batch. The
// batch holds its connection while it is read, so the nested load cannot run
// on that connection until the batch is done with it — on a held connection or
// a transaction that is the only one there is.
func TestNestedRelationBesideSibling(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seedRel(t, db)
	for _, q := range []string{
		`DROP TABLE IF EXISTS rel_reviews`,
		`CREATE TABLE rel_reviews (book_id bigint, body text)`,
		`INSERT INTO rel_reviews (book_id, body) VALUES (1, 'great'), (1, 'dense'), (3, 'short')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS rel_reviews`) })

	load := func(t *testing.T, h barm.IDB) {
		t.Helper()
		done := make(chan struct{})
		var authors []nestAuthor
		var err error
		go func() {
			defer close(done)
			authors, err = barm.Handle{IDB: h}.Select[nestAuthor]().OrderBy("id").
				Relation("Books", func(q *barm.SelectQuery[nestBook]) *barm.SelectQuery[nestBook] {
					return q.OrderBy("id").Relation[nestReview]("Reviews")
				}).
				Relation[RelTag]("Tags").
				Slice(ctx)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("deadlocked")
		}
		if err != nil {
			t.Fatal(err)
		}
		lem := authors[0]
		if len(lem.Books) != 2 || len(lem.Tags) != 1 ||
			len(lem.Books[0].Reviews) != 2 || len(lem.Books[1].Reviews) != 0 ||
			len(authors[1].Books[0].Reviews) != 1 {
			t.Errorf("authors = %+v", authors)
		}
	}

	t.Run("conn", func(t *testing.T) {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		load(t, c)
	})
	t.Run("tx on conn", func(t *testing.T) {
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
		load(t, tx)
	})
	t.Run("tx on db", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		load(t, tx)
	})
	t.Run("pool of one", func(t *testing.T) {
		db.SetMaxOpenConns(1)
		defer db.SetMaxOpenConns(0)
		load(t, db)
	})
}

// Postgres is where CTEs are actually load-bearing to test: it numbers
// placeholders, so two bodies built separately would both start at $1 and bind
// the wrong values, and it requires the RECURSIVE keyword that sqlite treats as
// optional. Neither mistake is visible on a `?` dialect.
func TestCTE(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	t.Run("two bodies bind in text order", func(t *testing.T) {
		names, err := db.Select[User]().
			With("adults", db.Select[User]().Column("id").Where("age >= ?", 30)).
			With("named", db.Select[User]().Column("id", "name").Where("name <> ?", "cy")).
			Table("named").
			Where("id IN (SELECT id FROM adults) AND name <> ?", "ann").
			Column("name").
			OrderBy("name").
			SliceAs[string](ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != 1 || names[0] != "bo" {
			t.Errorf("names = %v, want [bo]", names)
		}
	})

	t.Run("recursive", func(t *testing.T) {
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
		if len(got) != 5 || got[0] != 1 || got[4] != 5 {
			t.Errorf("got %v, want 1..5", got)
		}
	})

	t.Run("data-modifying", func(t *testing.T) {
		// the CTE deletes and the statement inserts what it returned: one
		// round trip, and the rows never come back to Go in between
		res, err := db.Insert[User]().
			With("gone", db.Delete[User]().Where("age = ?", 20).Returning("name, email, age, created_at")).
			Table("batch_users").
			Values(&User{Name: "placeholder", Email: "p@x.io", Age: 1, CreatedAt: time.Now()}).
			Column("name", "email", "age", "created_at").
			Exec(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("inserted %d", n)
		}
		left, err := db.Select[User]().Count(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if left != 3 { // ann deleted, placeholder added
			t.Errorf("rows left = %d, want 3", left)
		}
	})
}

// SQL is what carries hand-written statements now that Query is closed to
// barm's own types, so it has to survive the round trip a batch puts it through.
func TestRawInBatch(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	b := db.Batch()
	gone := b.Exec(db.NewRaw("DELETE FROM batch_users WHERE age = ?", 20))
	left := b.Count(db.Select[User]())
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := gone.Value().RowsAffected; n != 1 {
		t.Errorf("deleted %d, want 1", n)
	}
	if left.Value() != 2 {
		t.Errorf("left %d, want 2", left.Value())
	}
}

// Begin, Commit and Rollback work out what to send from what the batch runs on:
// BEGIN on the pool, SAVEPOINT inside a transaction. Nothing on the batch
// remembers which — it is read from the batcher each time.
func TestBatchBeginProbes(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		`DROP TABLE IF EXISTS probe_t`,
		`CREATE TABLE probe_t (id int PRIMARY KEY)`,
		`INSERT INTO probe_t VALUES (1)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	count := func(q barm.Querier) int64 {
		var n int64
		if err := q.QueryRowContext(ctx, `SELECT count(*) FROM probe_t`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	pid := func() int64 {
		n, _ := db.Select[User]().Table("(SELECT pg_backend_pid() AS id) t").
			Column("id").OneAs[int64](ctx)
		return n
	}

	// on the pool: a transaction of the batch's own
	t.Run("pool commits", func(t *testing.T) {
		b := db.Batch()
		b.Begin()
		b.Exec(db.NewRaw("INSERT INTO probe_t VALUES (10)"))
		b.Commit()
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if n := count(db); n != 2 {
			t.Errorf("rows = %d, want 2", n)
		}
	})

	// inside a transaction: a savepoint, so the batch undoes only itself
	t.Run("transaction uses a savepoint", func(t *testing.T) {
		before := pid()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.NewRaw("INSERT INTO probe_t VALUES (100)").Exec(ctx); err != nil {
			t.Fatal(err)
		}

		b := tx.Batch()
		b.Begin()
		b.Exec(tx.NewRaw("INSERT INTO probe_t VALUES (20)"))
		b.Exec(tx.NewRaw("INSERT INTO probe_t VALUES (1)")) // dupe
		commit := b.Commit()
		if err := b.Run(ctx); err == nil {
			t.Fatal("no error from the failing batch")
		}
		if !errors.Is(commit.Err(), barm.ErrNotRun) {
			t.Errorf("RELEASE err = %v, want ErrNotRun", commit.Err())
		}

		// recovery runs on the spot, on the connection the batch used
		if err := b.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		// the transaction survived, and the work before the savepoint with it
		if n := count(tx); n != 3 {
			t.Errorf("rows in tx = %d, want 3 (1, 10, 100)", n)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if after := pid(); after != before {
			t.Errorf("backend went from %d to %d — the connection was discarded", before, after)
		}
	})
}

type pgNZ struct {
	barm.BaseModel `barm:"table:pg_nz"`

	ID   int64     `barm:"id,pk,autoincrement"`
	Name string    `barm:"name,nullzero"`
	Seen time.Time `barm:"seen,nullzero"`
	N    int32     `barm:"n,nullzero"`
}

// nullzero through pgx's own rows: a prepared statement run by the driver, and
// a batch.
func TestNullZeroPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS pg_nz`,
		`CREATE TABLE pg_nz (id bigserial PRIMARY KEY, name text, seen timestamptz, n int)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS pg_nz`) })

	empty := &pgNZ{}
	if _, err := db.Insert[pgNZ]().Values(empty).Returning("id").Prepare("nz_ins").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	var nulls int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_nz WHERE name IS NULL AND seen IS NULL AND n IS NULL`).Scan(&nulls)
	if err != nil || nulls != 1 {
		t.Fatalf("rows written all NULL = %d, %v, want 1", nulls, err)
	}

	got, err := db.Select[pgNZ]().Where("id = ?", empty.ID).Prepare("nz_one").One(ctx)
	if err != nil || got.Name != "" || !got.Seen.IsZero() || got.N != 0 {
		t.Errorf("prepared one: %+v, %v", got, err)
	}
	b := db.Batch()
	rows := b.Slice(db.Select[pgNZ]())
	one := b.One(db.Select[pgNZ]())
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if rs := rows.Value(); len(rs) != 1 || rs[0].Name != "" || one.Value().N != 0 {
		t.Errorf("batch: %+v, %+v", rs, one.Value())
	}
}

type pgJSON struct {
	barm.BaseModel `barm:"table:pg_js"`

	ID    int64          `barm:"id,pk,autoincrement"`
	Meta  map[string]any `barm:"meta,json"`
	Plain []int          `barm:"plain,json"`
}

// json fields against real json and jsonb columns, on every path pgx reads by:
// database/sql, a prepared statement run by the driver, and a batch.
func TestJSONPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS pg_js`,
		`CREATE TABLE pg_js (id bigserial PRIMARY KEY, meta jsonb, plain json)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS pg_js`) })

	for i, prepare := range []string{"", "js_ins"} {
		v := &pgJSON{Meta: map[string]any{"k": "v", "n": float64(i)}, Plain: []int{i}}
		if _, err := db.Insert[pgJSON]().Values(v).Returning("id").Prepare(prepare).Exec(ctx); err != nil {
			t.Fatalf("insert %q: %v", prepare, err)
		}
	}
	var kinds string
	if err := db.QueryRowContext(ctx, `SELECT string_agg(jsonb_typeof(meta), ',') FROM pg_js`).Scan(&kinds); err != nil || kinds != "object,object" {
		t.Fatalf("stored as %q, %v, want objects rather than strings", kinds, err)
	}

	check := func(what string, rows []pgJSON, err error) {
		t.Helper()
		if err != nil || len(rows) != 2 || rows[1].Meta["k"] != "v" || rows[1].Meta["n"] != float64(1) || len(rows[1].Plain) != 1 || rows[1].Plain[0] != 1 {
			t.Errorf("%s: %+v, %v", what, rows, err)
		}
	}
	rows, err := db.Select[pgJSON]().OrderBy("id").Slice(ctx)
	check("database/sql", rows, err)
	rows, err = db.Select[pgJSON]().OrderBy("id").Prepare("js_sel").Slice(ctx)
	check("prepared", rows, err)
	b := db.Batch()
	res := b.Slice(db.Select[pgJSON]().OrderBy("id"))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	check("batch", res.Value(), res.Err())
}
