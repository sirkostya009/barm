package pgxdriver_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirkostya009/barm"
	"github.com/sirkostya009/barm/pgxdriver"
)

type User struct {
	barm.BaseModel `barm:"table:batch_users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

// open gives a DB on a pgx pool.
func open(t *testing.T, opts ...barm.Option) *barm.DB {
	t.Helper()
	return openPool(t, 0, opts...)
}

func dsn() string {
	if d := os.Getenv("PGDSN"); d != "" {
		return d
	}
	return "postgres://postgres@localhost/postgres"
}

// openPool gives a DB on a pgx pool of at most size connections, or pgx's
// default for 0, over dsn.
func openPool(t *testing.T, size int32, opts ...barm.Option) *barm.DB {
	t.Helper()
	return openDSN(t, dsn(), size, nil, opts...)
}

// execMode configures a pool to run plain queries unprepared, as unnamed
// statements.
func execMode(c *pgxpool.Config) { c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec }

func openDSN(t *testing.T, dsn string, size int32, configure func(*pgxpool.Config), opts ...barm.Option) *barm.DB {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	if size > 0 {
		cfg.MaxConns = size
	}
	if configure != nil {
		configure(cfg)
	}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	if err := p.Ping(context.Background()); err != nil {
		p.Close()
		t.Skipf("no postgres at %s: %v", dsn, err)
	}
	t.Cleanup(p.Close)

	for _, q := range []string{
		`DROP TABLE IF EXISTS batch_users`,
		`CREATE TABLE batch_users (
			id bigserial PRIMARY KEY, name text NOT NULL, email text NOT NULL,
			age int NOT NULL, created_at timestamptz NOT NULL)`,
	} {
		if _, err := p.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	return barm.New(pgxdriver.Pool(p), opts...)
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

func TestPingAndUnderlying(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	err := db.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping = %v", err)
	}
	p, ok := db.Pool().Underlying().(*pgxpool.Pool)
	if !ok {
		t.Fatalf("Underlying = %T, want *pgxpool.Pool", db.Pool().Underlying())
	}
	p.Close()
	err = db.Ping(ctx)
	if err == nil {
		t.Error("Ping on a closed pool = nil, want an error")
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
	if _, err := db.Exec(context.Background(), `ALTER TABLE batch_users
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
	db, sc := openCounted(t)
	seedRel(t, db)

	// The first run describes each statement new to the pool, so the second is
	// the one whose round trips are the loading's own.
	for i, want := range []int64{-1, 2} {
		before := sc.n.Load()
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
		if got := sc.n.Load() - before; want >= 0 && got != want {
			t.Errorf("run %d: %d round trips, want %d", i, got, want)
		}
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`) })

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
	db, sc := openCounted(t)
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

	for i, want := range []int64{-1, 2} { // described on the first run
		before := sc.n.Load()
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
		if got := sc.n.Load() - before; want >= 0 && got != want {
			t.Errorf("run %d: %d round trips, want %d", i, got, want)
		}
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags`) })
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
		if _, err := in.Exec(ctx, "SELECT 1/0"); err == nil {
			t.Fatal("expected division by zero")
		}
		if err := in.Commit(); err == nil {
			t.Fatal("expected the release to fail in an aborted transaction")
		}
	}()

	var n int
	if err := queryRow(ctx, tx, "SELECT 1").Scan(&n); err != nil {
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS bp, bc`) })
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS nodes`) })
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS rel_reviews, rel_tag_notes`) })
}

// Relations load a depth at a time: everything at one depth goes out together,
// whichever branch of the tree it hangs off — one round trip per depth, not
// one per relation that has relations of its own.
func TestRelationsLoadByDepth(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t)
	seedDeepRel(t, db)

	// The first run describes each statement new to the pool, so the second is
	// the one whose round trips are the loading's own.
	for i, want := range []int64{-1, 3} { // the authors, then one per depth
		before := sc.n.Load()
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
		if got := sc.n.Load() - before; want >= 0 && got != want {
			t.Errorf("run %d: %d round trips, want %d", i, got, want)
		}
	}
}

// Relations of different queries in one batch share their round trips too.
func TestBatchRelationsLoadTogether(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t)
	seedDeepRel(t, db)

	for i, want := range []int64{-1, 2} { // the batch, then both relations at once
		before := sc.n.Load()
		b := db.Batch()
		books := b.Slice(db.Select[RelAuthor]().OrderBy("id").Relation[RelBook]("Books"))
		tags := b.One(db.Select[bfsAuthor]().Where("id = ?", 1).Relation[bfsTag]("Tags"))
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if len(books.Value()) != 2 || len(books.Value()[0].Books) != 2 || len(tags.Value().Tags) != 1 {
			t.Fatalf("books %+v, tags %+v", books.Value(), tags.Value())
		}
		if got := sc.n.Load() - before; want >= 0 && got != want {
			t.Errorf("run %d: %d round trips, want %d", i, got, want)
		}
	}
}

// syncCounter proxies a Postgres connection and counts the round trips the
// client starts — a Sync ends one in the extended protocol, a Query is one in
// the simple protocol — and the Parse messages, one per statement prepared.
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
		case 'S', 'Q': // a Sync ends an extended-protocol round trip; a simple query is one on its own
			sc.n.Add(1)
		case 'P':
			sc.parses.Add(1)
		}
		dst.Write(hdr[:])
		dst.Write(body)
	}
}

// openCounted opens a DB of one connection that goes through a Sync counter.
func openCounted(t *testing.T, configure ...func(*pgxpool.Config)) (*barm.DB, *syncCounter) {
	t.Helper()
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
	var c func(*pgxpool.Config)
	if len(configure) > 0 {
		c = configure[0]
	}
	return openDSN(t, u.String(), 1, c), sc
}

// A named statement new to the pool is described first, a round trip of its
// own, so its arguments are encoded for their types from the first run. After
// that each run is one round trip, by name, with no Parse.
func TestPreparedRoundTrips(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t)
	seed(t, db)

	for i, want := range []struct{ trips, parses int64 }{{2, 1}, {1, 0}} { // prepared once, run by name after
		parses := want.parses
		before, parsedBefore := sc.n.Load(), sc.parses.Load()
		u, err := db.Select[User]().Where("age = ?", 20).Prepare("by_age").One(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := sc.n.Load() - before; got != want.trips || u.Name != "ann" {
			t.Errorf("call %d: %d round trips, want %d, name %q", i, got, want.trips, u.Name)
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
	if got := sc.n.Load() - before; got != 4 {
		t.Errorf("slice and exec, each new to the pool, took %d round trips, want 4", got)
	}
}

// Inside a transaction the statement is prepared on the transaction's own
// connection, so a pool with nothing else free is no obstacle.
func TestPreparedInTxOnFullPool(t *testing.T) {
	db := openPool(t, 1)
	seed(t, db)
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
// session — fails the call that finds out, and pgx drops it, so the next call
// prepares it again rather than every call after failing.
func TestPreparedSurvivesDeallocate(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	q := func() (User, error) { return c.Select[User]().Where("age = ?", 40).Prepare("forgotten").One(ctx) }
	u, err := q()
	if err != nil || u.Name != "cy" {
		t.Fatalf("one = %+v, %v", u, err)
	}
	if _, err := c.Exec(ctx, "DEALLOCATE ALL"); err != nil {
		t.Fatal(err)
	}
	_, _ = q() // finds the statement gone
	u, err = q()
	if err != nil || u.Name != "cy" {
		t.Fatalf("after DEALLOCATE: %+v, %v", u, err)
	}
}

// A batch forgets a named statement it could not run, as a single query does:
// one the server has dropped, and one whose Parse never ran because an earlier
// query in the batch failed first. Either way the next batch parses it again.
func TestBatchForgetsWhatFailed(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	run := func(fail bool) error {
		b := c.Batch()
		if fail {
			b.Exec(c.NewRaw("SELECT 1/0"))
		}
		r := b.One(c.Select[User]().Where("age = ?", 40).Prepare("bf_cy"))
		if err := b.Run(ctx); err != nil {
			return err
		}
		if r.Value().Name != "cy" {
			return errors.New("wrong row: " + r.Value().Name)
		}
		return nil
	}

	if err := run(true); err == nil {
		t.Fatal("the batch should fail")
	}
	if err := run(false); err != nil {
		t.Fatalf("after a batch that never parsed it: %v", err)
	}
	if _, err := c.Exec(ctx, "DEALLOCATE ALL"); err != nil {
		t.Fatal(err)
	}
	_ = run(false) // finds the statement gone
	if err := run(false); err != nil {
		t.Fatalf("after DEALLOCATE: %v", err)
	}
}

// A schema change that alters a named statement's result fails the call that
// finds out, and the next one runs against the new schema rather than every
// call after failing with "cached plan must not change result type".
func TestPreparedSurvivesSchemaChange(t *testing.T) {
	ctx := t.Context()
	db := openPool(t, 1)
	seed(t, db)
	q := func() ([]User, error) { return db.Select[User]().Where("age > ?", 0).Prepare("sk_stale").Slice(ctx) }
	if _, err := q(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE batch_users ALTER COLUMN age TYPE bigint"); err != nil {
		t.Fatal(err)
	}
	_, _ = q()
	us, err := q()
	if err != nil || len(us) != 3 {
		t.Fatalf("after ALTER: %d rows, %v", len(us), err)
	}
}

// A named query that runs on its first run, arguments as untyped text, runs on
// every run after: an argument pgx cannot encode for its parameter's type goes
// as text again.
func TestPreparedEncodesTheSameEveryRun(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	var first error
	for i := range 3 {
		_, err := db.Select[User]().Where("name = ?", 5).Prepare("sk_textint").Slice(ctx)
		if i == 0 {
			first = err
			continue
		}
		if (err == nil) != (first == nil) {
			t.Errorf("run %d: %v, but run 0: %v", i, err, first)
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS pg_accts`) })
	idOf := func(email string) int64 {
		var id int64
		if err := queryRow(ctx, db, `SELECT id FROM pg_accts WHERE email = $1`, email).Scan(&id); err != nil {
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS rel_reviews`) })

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
		load(t, openPool(t, 1))
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

type newUser struct {
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

type userName struct {
	Name string `barm:"name"`
}

// The As calls read any query's rows into a type of the caller's choosing: an
// insert of a sub-model returns the whole row, a select reads what it narrows to.
func TestBatchAs(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)

	b := db.Batch()
	dee := b.OneAs[User](db.Insert[newUser]().
		Table("batch_users").
		Values(&newUser{Name: "dee", Email: "d@x.io", Age: 50, CreatedAt: time.Now()}))
	names := b.SliceAs[userName](db.Select[User]().OrderBy("age"))
	oldest := b.OneAs[int](db.NewRaw("SELECT max(age) FROM batch_users"))
	gone := b.SliceAs[User](db.Delete[User]().Where("age < ?", 25))
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if u := dee.Value(); u.ID == 0 || u.Name != "dee" || u.Age != 50 {
		t.Errorf("inserted = %+v", u)
	}
	if got := names.Value(); len(got) != 4 || got[0].Name != "ann" || got[3].Name != "dee" {
		t.Errorf("names = %+v", got)
	}
	if oldest.Value() != 50 {
		t.Errorf("oldest = %d, want 50", oldest.Value())
	}
	if got := gone.Value(); len(got) != 1 || got[0].ID == 0 || got[0].Name != "ann" {
		t.Errorf("deleted = %+v", got)
	}

	b = db.Batch()
	none := b.OneAs[User](db.Delete[User]().Where("age > ?", 100))
	if err := b.Run(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("Run = %v, want ErrNoRows", err)
	}
	if !errors.Is(none.Err(), sql.ErrNoRows) {
		t.Errorf("result = %v, want ErrNoRows", none.Err())
	}
}

// A Handle batches on whatever it wraps, so a batch on a transaction's handle
// sees what the transaction wrote.
func TestHandleBatchRunsWhereItIs(t *testing.T) {
	ctx := t.Context()
	db := open(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Insert[User]().Values(&User{Name: "eve", Email: "e@x.io", Age: 1, CreatedAt: time.Now()}).Exec(ctx); err != nil {
		t.Fatal(err)
	}

	h := barm.Handle{IDB: tx}
	b := h.Batch()
	n := b.Count(h.Select[User]())
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n.Value() != 1 {
		t.Errorf("count = %d, want the transaction's own row", n.Value())
	}
}

// Begin, Commit and Rollback work out what to send from what the batch runs on:
// BEGIN on the pool, SAVEPOINT inside a transaction. Nothing on the batch
// remembers which — it is read from the batcher each time.
func TestBatchBeginProbes(t *testing.T) {
	ctx := t.Context()
	db := openPool(t, 1)
	for _, q := range []string{
		`DROP TABLE IF EXISTS probe_t`,
		`CREATE TABLE probe_t (id int PRIMARY KEY)`,
		`INSERT INTO probe_t VALUES (1)`,
	} {
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	count := func(q barm.IDB) int64 {
		var n int64
		if err := queryRow(ctx, q, `SELECT count(*) FROM probe_t`).Scan(&n); err != nil {
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS pg_nz`) })

	empty := &pgNZ{}
	if _, err := db.Insert[pgNZ]().Values(empty).Returning("id").Prepare("nz_ins").Exec(ctx); err != nil {
		t.Fatal(err)
	}
	var nulls int
	err := queryRow(ctx, db, `SELECT count(*) FROM pg_nz WHERE name IS NULL AND seen IS NULL AND n IS NULL`).Scan(&nulls)
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
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS pg_js`) })

	for i, prepare := range []string{"", "js_ins"} {
		v := &pgJSON{Meta: map[string]any{"k": "v", "n": float64(i)}, Plain: []int{i}}
		if _, err := db.Insert[pgJSON]().Values(v).Returning("id").Prepare(prepare).Exec(ctx); err != nil {
			t.Fatalf("insert %q: %v", prepare, err)
		}
	}
	var kinds string
	if err := queryRow(ctx, db, `SELECT string_agg(jsonb_typeof(meta), ',') FROM pg_js`).Scan(&kinds); err != nil || kinds != "object,object" {
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

type nativeRow struct {
	barm.BaseModel `barm:"table:native_t"`

	ID    int64          `barm:"id,pk,autoincrement"`
	Ints  []int64        `barm:"ints"`
	Texts []string       `barm:"texts"`
	Meta  map[string]any `barm:"meta"`
}

// On pgx's own pool every column type pgx knows reads straight into its field,
// with no tag: arrays and JSON through a plain Slice and One, which went
// through database/sql's conversions before and could not take them.
func TestNativeTypes(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`DROP TABLE IF EXISTS native_t`,
		`CREATE TABLE native_t (id bigserial PRIMARY KEY, ints bigint[], texts text[], meta jsonb)`,
		`INSERT INTO native_t (ints, texts, meta) VALUES ('{1,2}', '{a,"b c"}', '{"k": "v"}')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS native_t`) })

	check := func(what string, r nativeRow, err error) {
		t.Helper()
		if err != nil || !slices.Equal(r.Ints, []int64{1, 2}) || !slices.Equal(r.Texts, []string{"a", "b c"}) || r.Meta["k"] != "v" {
			t.Errorf("%s: %+v, %v", what, r, err)
		}
	}
	rows, err := db.Select[nativeRow]().Slice(ctx)
	if len(rows) != 1 {
		t.Fatalf("slice: %+v, %v", rows, err)
	}
	check("slice", rows[0], err)
	one, err := db.Select[nativeRow]().One(ctx)
	check("one", one, err)

	// writing needs no tag either: pgx has the column types from its cache
	if _, err := db.Insert[nativeRow]().Values(&nativeRow{Ints: []int64{3}, Texts: []string{"z"}, Meta: map[string]any{"w": 1.0}}).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	n, err := db.Select[nativeRow]().Where("3 = ANY(ints) AND meta->>'w' = '1'").Count(ctx)
	if err != nil || n != 1 {
		t.Errorf("array and JSON written: count = %d, %v", n, err)
	}
}

// barm hands pgx a plain query and nothing else, so what gets prepared is pgx's
// configuration: nothing under exec mode, pgx's own cache under its default.
// A name from Prepare is prepared either way, and a batch's plain queries never
// are.
func TestFollowsPgxMode(t *testing.T) {
	ctx := t.Context()
	run := func(t *testing.T, db *barm.DB) []string {
		t.Helper()
		seed(t, db)
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		for range 3 {
			if _, err := c.Select[User]().Where("age > ?", 1).Slice(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Update[User]().Set("age = age").Where("age > ?", 1).Exec(ctx); err != nil {
				t.Fatal(err)
			}
		}
		b := c.Batch()
		b.Count(c.Select[User]())
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Select[User]().Where("age > ?", 1).Prepare("opted_in").Slice(ctx); err != nil {
			t.Fatal(err)
		}
		names, err := c.Select[string]().Table("pg_prepared_statements").Column("name").OrderBy("name").Slice(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return names
	}
	t.Run("exec mode", func(t *testing.T) {
		if got := run(t, openDSN(t, dsn(), 0, execMode)); len(got) != 1 {
			t.Errorf("prepared = %v, want only the named one", got)
		}
	})
	t.Run("pgx default", func(t *testing.T) {
		if got := run(t, open(t)); len(got) < 3 {
			t.Errorf("prepared = %v, want pgx's cached statements as well as the named one", got)
		}
	})
}

// A plain query in exec mode is one round trip: the unnamed statement goes out
// with its arguments in a single flush.
func TestPlainQueryIsOneRoundTrip(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t, execMode)
	seed(t, db)
	for i := range 3 {
		before := sc.n.Load()
		if _, err := db.Select[User]().Where("age = ?", 20).One(ctx); err != nil {
			t.Fatal(err)
		}
		if got := sc.n.Load() - before; got != 1 {
			t.Errorf("query %d: %d round trips, want 1", i, got)
		}
	}
}

// Under pgx's default mode a plain query is prepared once per connection by
// pgx's own cache and run by name after, so a repeat skips the server's parse.
func TestPgxStatementCache(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t)
	seed(t, db)
	for i, parses := range []int64{1, 0} {
		before, parsedBefore := sc.n.Load(), sc.parses.Load()
		u, err := db.Select[User]().Where("age = ?", 20).One(ctx)
		if err != nil || u.Name != "ann" {
			t.Fatalf("one = %+v, %v", u, err)
		}
		if got := sc.parses.Load() - parsedBefore; got != parses {
			t.Errorf("run %d: %d parses, want %d", i, got, parses)
		}
		if got := sc.n.Load() - before; i == 1 && got != 1 {
			t.Errorf("run %d: %d round trips, want 1", i, got)
		}
	}
	b := db.Batch()
	n := b.Count(db.Select[User]())
	us := b.Slice(db.Select[User]().Where("age > ?", 25))
	if err := b.Run(ctx); err != nil || n.Value() != 3 || len(us.Value()) != 2 {
		t.Errorf("batch: %v, %d, %v", err, n.Value(), us.Value())
	}
	names, err := db.Select[string]().Table("pg_prepared_statements").Column("name").Slice(ctx)
	if err != nil || len(names) == 0 {
		t.Errorf("prepared statements = %v, %v, want some", names, err)
	}
}

// A batch goes out in one round trip whatever pgx's mode. A statement it keeps
// is parsed the first time — five sharing one text are one Parse and five runs
// of it — and run by name after; one with arguments new to the pool is
// described first, in a round trip of its own. Named ones are kept in any
// mode, plain ones under pgx's default, and in exec mode plain ones are parsed
// on every run.
func TestBatchRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure []func(*pgxpool.Config)
		prepare   string
		trips     [3]int64
		parses    [3]int64
	}{
		{"pgx default", nil, "", [3]int64{2, 1, 1}, [3]int64{2, 0, 0}},
		{"exec mode", []func(*pgxpool.Config){execMode}, "", [3]int64{1, 1, 1}, [3]int64{6, 6, 6}},
		{"named", nil, "rt_one", [3]int64{2, 1, 1}, [3]int64{2, 0, 0}},
		{"named in exec mode", []func(*pgxpool.Config){execMode}, "rt_one", [3]int64{2, 1, 1}, [3]int64{2, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db, sc := openCounted(t, tc.configure...)
			seed(t, db)
			for run := range 3 {
				before, parsed := sc.n.Load(), sc.parses.Load()
				b := db.Batch()
				ones := make([]*barm.BatchResult[User], 5)
				for i := range ones {
					q := db.Select[User]().Where("age >= ?", 20+i)
					if tc.prepare != "" {
						q.Prepare(tc.prepare)
					}
					ones[i] = b.One(q)
				}
				n := b.Count(db.Select[User]())
				if err := b.Run(ctx); err != nil {
					t.Fatal(err)
				}
				if ones[0].Value().Name != "ann" || ones[4].Value().Name != "bo" || n.Value() != 3 {
					t.Fatalf("run %d: wrong rows: %+v %+v %d", run, ones[0].Value(), ones[4].Value(), n.Value())
				}
				if got := sc.n.Load() - before; got != tc.trips[run] {
					t.Errorf("run %d: %d round trips, want %d", run, got, tc.trips[run])
				}
				if got := sc.parses.Load() - parsed; got != tc.parses[run] {
					t.Errorf("run %d: %d parses, want %d", run, got, tc.parses[run])
				}
			}
		})
	}
}

// A transaction's batch of named statements between Begin and Commit is one
// round trip — the SAVEPOINT, each statement's Parse, every run of them and the
// RELEASE in one flush — on every connection but the first to meet them, which
// describes them first. Their types are the pool's, so a second connection
// parses them with no describe of its own.
func TestTxBatchOfNamed(t *testing.T) {
	ctx := t.Context()
	db, sc := openCounted(t, func(c *pgxpool.Config) { c.MaxConns = 2 })
	seed(t, db)
	bump := func(t *testing.T, c *barm.Conn, run int, trips, parses int64) {
		t.Helper()
		tx, err := c.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		before, parsed := sc.n.Load(), sc.parses.Load()
		b := tx.Batch()
		b.Begin()
		for i := range 4 {
			b.Exec(tx.Update[User]().Set("age = age + ?", 1).Where("name = ?", "ann").Prepare("tx_bump"))
			b.One(tx.Select[User]().Where("age > ?", i).Prepare("tx_first"))
		}
		b.Commit()
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if got := sc.n.Load() - before; got != trips {
			t.Errorf("run %d: %d round trips, want %d", run, got, trips)
		}
		if got := sc.parses.Load() - parsed; got != parses {
			t.Errorf("run %d: %d parses, want %d", run, got, parses)
		}
		u, err := tx.Select[User]().Where("name = ?", "ann").One(ctx)
		if err != nil || u.Age != 24 {
			t.Errorf("run %d: ann = %+v, %v, want age 24 after four bumps", run, u, err)
		}
	}
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// The describe is a round trip of its own, under a savepoint of its own:
	// SAVEPOINT, two Parses and RELEASE. The batch then parses its SAVEPOINT
	// and RELEASE, as it keeps them too.
	bump(t, first, 0, 2, 6)
	bump(t, first, 1, 1, 0)

	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	bump(t, second, 2, 1, 4) // the two statements, the SAVEPOINT and the RELEASE
}

type encRow struct {
	barm.BaseModel `barm:"table:enc"`

	K string `barm:"k"`
	V any    `barm:"v"`
}

type typeChange struct {
	barm.BaseModel `barm:"table:type_change"`

	V any `barm:"v"`
}

// A schema change that leaves a statement's parameter types invalid fails the
// call that finds out, and the next one learns the new types, rather than every
// call after parsing with the old ones, on any connection.
func TestTypesFollowSchemaChange(t *testing.T) {
	ctx := t.Context()
	db := openPool(t, 1)
	for _, q := range []string{
		`DROP TABLE IF EXISTS type_change`,
		`CREATE TABLE type_change (v int)`,
		`INSERT INTO type_change VALUES (1)`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE type_change`) })
	named := func() error {
		_, err := db.Select[typeChange]().Where("v = ?", 1).Prepare("tc_named").One(ctx)
		return err
	}
	batched := func() error {
		b := db.Batch()
		r := b.One(db.Select[typeChange]().Where("v = ? AND TRUE", 1))
		if err := b.Run(ctx); err != nil {
			return err
		}
		return r.Err()
	}
	for _, run := range []func() error{named, batched} {
		if err := run(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, `ALTER TABLE type_change ALTER COLUMN v TYPE jsonb USING to_jsonb(v)`); err != nil {
		t.Fatal(err)
	}
	for i, run := range []func() error{named, batched} {
		_ = run() // finds the types stale
		if err := run(); err != nil {
			t.Errorf("query %d after ALTER: %v", i, err)
		}
	}
}

// A batch and a named query store what a plain query stores, from their first
// run on: their arguments are encoded for the types the server gives them,
// not blind.
func TestBatchEncodesAsPlain(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	type obj struct{ A string }
	zone := time.FixedZone("x", 3600)
	for i, c := range []struct {
		typ string
		v   any
	}{
		{"jsonb", map[string]any{"k": 1}},
		{"jsonb", obj{"x"}},
		{"jsonb", []obj{{"x"}}},
		{"jsonb", []byte(`{"a": 1}`)},
		{"uuid", [16]byte{1}},
		{"timestamp", time.Date(2026, 1, 2, 3, 4, 5, 0, zone)},
		{"date", time.Date(2026, 1, 2, 23, 4, 5, 0, time.FixedZone("y", -5*3600))},
		{"text", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{"numeric", 0.1},
		{"int[]", []int64{1, 2}},
		{"text", 5}, // pgx cannot encode an int for text, whichever way it runs
	} {
		tbl := fmt.Sprintf("enc_%d", i)
		if _, err := db.Exec(ctx, `DROP TABLE IF EXISTS `+tbl); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, `CREATE TABLE `+tbl+` (k text, v `+c.typ+`)`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE `+tbl) })
		insert := func(k string) *barm.InsertQuery[encRow] {
			return db.Insert[encRow]().Table(tbl).Values(&encRow{K: k, V: c.v})
		}
		errs := map[string]error{}
		_, errs["plain"] = insert("plain").Exec(ctx)
		for _, k := range []string{"batch 1", "batch 2"} {
			b := db.Batch()
			b.Exec(insert(k))
			errs[k] = b.Run(ctx)
		}
		for _, k := range []string{"named 1", "named 2"} {
			_, errs[k] = insert(k).Prepare(tbl).Exec(ctx)
		}
		for k, err := range errs {
			if (err == nil) != (errs["plain"] == nil) {
				t.Errorf("%s %T: %s: %v, plain: %v", c.typ, c.v, k, err, errs["plain"])
			}
		}
		vals, err := db.Select[struct {
			K string  `barm:"k"`
			V *string `barm:"v"`
		}]().Table(tbl).ColumnExpr("k, v::text AS v").Slice(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range vals {
			if *v.V != *vals[0].V {
				t.Errorf("%s %T: %s stored %s, %s stored %s", c.typ, c.v, v.K, *v.V, vals[0].K, *vals[0].V)
			}
		}
	}
}

// The plain statements a batch keeps are bounded by pgx's statement cache
// capacity: past it the least recently run is closed on the server.
func TestBatchStatementsAreBounded(t *testing.T) {
	ctx := t.Context()
	db := openDSN(t, dsn(), 1, func(c *pgxpool.Config) { c.ConnConfig.StatementCacheCapacity = 2 })
	seed(t, db)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for range 2 {
		b := c.Batch()
		ns := make([]*barm.BatchResult[int64], 3)
		for i := range ns {
			q := c.Select[User]().Where("age > ?", 0)
			for range i {
				q.Where("TRUE") // three texts, one more than fits
			}
			ns[i] = b.Count(q)
		}
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		for i, n := range ns {
			if n.Value() != 3 {
				t.Errorf("count %d = %d", i, n.Value())
			}
		}
	}
	kept, err := c.Select[int64]().Table("pg_prepared_statements").ColumnExpr("count(*)").Where(`name LIKE 'barm\_%'`).One(ctx)
	if err != nil || kept != 2 {
		t.Errorf("%d statements kept, %v, want 2", kept, err)
	}
}

// Inside a transaction a batch's SAVEPOINT is the first thing in its pipeline,
// so a statement that fails to parse — after the SAVEPOINT, before anything
// else runs — still leaves the savepoint for Rollback to return to, and the
// transaction usable. One with arguments fails its describe first, under a
// savepoint of its own that the batch then rolls back to. The failed statement
// is not kept: a later batch parses it again rather than running a name the
// server never had.
func TestBatchRollbackAfterPlanError(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := range 4 {
		bad := tx.Select[User]().Table("no_such_table").Prepare("no_table")
		if i >= 2 {
			bad.Where("age > ?", 1).Prepare("no_table_args")
		}
		b := tx.Batch()
		b.Begin()
		b.Exec(tx.Update[User]().Set("age = age + 1").Where("name = ?", "ann"))
		b.Slice(bad)
		b.Commit()
		err := b.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "no_such_table") {
			t.Fatalf("the batch should fail on the missing table, got %v", err)
		}
		if err := b.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}
	}
	n, err := tx.Select[User]().Count(ctx)
	if err != nil || n != 3 {
		t.Errorf("after rollback: %d, %v, want the transaction usable", n, err)
	}
}

// Rows runs a named query like any other.
func TestRowsPrepared(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	rows, err := db.Select[User]().Column("name").Where("age > ?", 25).OrderBy("age").Prepare("rows_named").Rows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil || !slices.Equal(names, []string{"bo", "cy"}) {
		t.Errorf("names = %v, %v", names, err)
	}
}

// A trigger that drops a row shifts the RETURNING rows after it onto the wrong
// values, so a count that does not match is reported rather than written back
// quietly.
func TestReturningCountMismatch(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	for _, q := range []string{
		`CREATE OR REPLACE FUNCTION skip_bo() RETURNS trigger AS $$ BEGIN IF NEW.name = 'bo' THEN RETURN NULL; END IF; RETURN NEW; END $$ LANGUAGE plpgsql`,
		`CREATE TRIGGER skip_bo BEFORE INSERT ON batch_users FOR EACH ROW EXECUTE FUNCTION skip_bo()`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	rows := []*User{
		{Name: "ann", Email: "a", Age: 1, CreatedAt: time.Now()},
		{Name: "bo", Email: "b", Age: 2, CreatedAt: time.Now()},
		{Name: "cy", Email: "c", Age: 3, CreatedAt: time.Now()},
	}
	_, err := db.Insert[User]().Values(rows...).Returning("id").Exec(ctx)
	if err == nil {
		t.Error("a RETURNING count short of the values should be reported")
	}
}

type pgArrayRow struct {
	barm.BaseModel `barm:"table:pg_arrays"`

	ID     int64       `barm:"id,pk,autoincrement"`
	Ints   []int64     `barm:"ints,array"`
	Texts  []string    `barm:"texts,array"`
	Maybe  []*string   `barm:"maybe,array"`
	Floats []float64   `barm:"floats,array"`
	Flags  []bool      `barm:"flags,array"`
	Grid   [][]int32   `barm:"grid,array"`
	Blobs  [][]byte    `barm:"blobs,array"`
	Times  []time.Time `barm:"times,array"`
	Nulled []int64     `barm:"nulled,array,nullzero"`
}

func createArrays(t *testing.T, db *barm.DB) {
	t.Helper()
	for _, q := range []string{
		`DROP TABLE IF EXISTS pg_arrays`,
		`CREATE TABLE pg_arrays (id bigserial PRIMARY KEY, ints int8[], texts text[], maybe text[],
			floats float8[], flags bool[], grid int4[][], blobs bytea[], times timestamptz[], nulled int8[])`,
	} {
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS pg_arrays`) })
}

// roundTripArrays writes rows through every way a query reaches the database —
// plain, prepared and batched — and checks they read back as written.
func roundTripArrays(t *testing.T, db *barm.DB) {
	t.Helper()
	ctx := t.Context()
	createArrays(t, db)
	s := `q"u,o{t}e\ NULL`
	at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	row := func() *pgArrayRow {
		return &pgArrayRow{
			Ints: []int64{1, -2}, Texts: []string{"a", "", "NULL", s}, Maybe: []*string{&s, nil},
			Floats: []float64{1.5, -0.25}, Flags: []bool{true, false}, Grid: [][]int32{{1, 2}, {3, 4}},
			Blobs: [][]byte{{0, 0xff}}, Times: []time.Time{at}, Nulled: []int64{},
		}
	}
	if _, err := db.Insert[pgArrayRow]().Values(row(), &pgArrayRow{}).Exec(ctx); err != nil {
		t.Fatalf("plain: %v", err)
	}
	if _, err := db.Insert[pgArrayRow]().Values(row()).Prepare("arr_ins").Exec(ctx); err != nil {
		t.Fatalf("prepared: %v", err)
	}
	b := db.Batch()
	b.Exec(db.Insert[pgArrayRow]().Values(row()))
	if err := b.Run(ctx); err != nil && !errors.Is(err, barm.ErrNoBatcher) {
		t.Fatalf("batch: %v", err)
	}
	var nulls int
	if err := queryRow(ctx, db, `SELECT count(*) FROM pg_arrays WHERE ints IS NULL AND nulled IS NULL`).Scan(&nulls); err != nil || nulls != 1 {
		t.Errorf("nil slices: %d rows of NULLs, %v", nulls, err)
	}
	var stored string
	if err := queryRow(ctx, db, `SELECT texts[4] FROM pg_arrays WHERE id = 1`).Scan(&stored); err != nil || stored != s {
		t.Errorf("stored %q, %v, want %q", stored, err, s)
	}

	for _, q := range []*barm.SelectQuery[pgArrayRow]{
		db.Select[pgArrayRow]().OrderBy("id"),
		db.Select[pgArrayRow]().OrderBy("id").Prepare("arr_sel"),
	} {
		out, err := q.Slice(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) < 3 {
			t.Fatalf("%d rows", len(out))
		}
		for i, got := range out {
			if i == 1 {
				if !reflect.DeepEqual(got, pgArrayRow{ID: got.ID}) {
					t.Errorf("the NULL row read as %+v", got)
				}
				continue
			}
			want := row()
			want.ID = got.ID
			if len(got.Times) != 1 || !got.Times[0].Equal(at) {
				t.Errorf("row %d times = %v", i, got.Times)
			}
			got.Times, want.Times = nil, nil
			if !reflect.DeepEqual(got, *want) {
				t.Errorf("row %d = %+v\nwant %+v", i, got, *want)
			}
		}
	}
}

// Over database/sql an array field is an array literal both ways: through pgx's
// stdlib driver, and through its simple protocol, as go-txdb runs it.
func TestArrayTagOverSQL(t *testing.T) {
	for _, mode := range []string{"", "simple_protocol"} {
		t.Run("mode "+mode, func(t *testing.T) {
			u, err := url.Parse(dsn())
			if err != nil {
				t.Fatal(err)
			}
			if mode != "" {
				q := u.Query()
				q.Set("default_query_exec_mode", mode)
				u.RawQuery = q.Encode()
			}
			sqldb, err := sql.Open("pgx", u.String())
			if err != nil {
				t.Fatal(err)
			}
			db := barm.New(barm.SQL(sqldb, barm.Postgres, barm.SequentialBatches()))
			defer db.Close()
			if err := sqldb.Ping(); err != nil {
				t.Skipf("no postgres: %v", err)
			}
			roundTripArrays(t, db)
		})
	}
}

// On pgx's own pool the tag is left to pgx, which encodes the slice as the
// array its parameter is, unless the pool's mode sends arguments untyped.
func TestArrayTagOnPgx(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*pgxpool.Config)
		native    bool
	}{
		{"pgx default", nil, true},
		{"exec mode", execMode, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var asText atomic.Bool
			hook := barm.WithHook(barm.QueryHook{BeforeQuery: func(ctx context.Context, ev *barm.QueryEvent) context.Context {
				if strings.HasPrefix(ev.Query, "INSERT") {
					for _, a := range ev.Args {
						if _, ok := a.(string); ok {
							asText.Store(true)
						}
					}
				}
				return ctx
			}})
			roundTripArrays(t, openDSN(t, dsn(), 0, tc.configure, hook))
			if asText.Load() == tc.native {
				t.Errorf("arrays sent as literals: %v, want %v", asText.Load(), !tc.native)
			}
		})
	}
}

type ageChange struct {
	Name  string    `barm:"name"`
	Age   int       `barm:"age"`
	Since time.Time `barm:"since"`
}

// An update reading a VALUES list matches its rows by columns that are not all
// text: the first row's casts give them their types, so pgx can
// encode them however the statement runs.
func TestUpdateFromValuesOnPgx(t *testing.T) {
	ctx := t.Context()
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name string
		db   func(*testing.T) *barm.DB
	}{
		{"pgx default", func(t *testing.T) *barm.DB { return open(t) }},
		{"exec mode", func(t *testing.T) *barm.DB { return openDSN(t, dsn(), 0, execMode) }},
		{"database/sql", func(t *testing.T) *barm.DB {
			sqldb, err := sql.Open("pgx", dsn())
			if err != nil {
				t.Fatal(err)
			}
			db := barm.New(barm.SQL(sqldb, barm.Postgres, barm.SequentialBatches()))
			t.Cleanup(func() { db.Close() })
			if _, err := db.Exec(ctx, `DROP TABLE IF EXISTS batch_users`); err != nil {
				t.Skipf("no postgres: %v", err)
			}
			if _, err := db.Exec(ctx, `CREATE TABLE batch_users (id bigserial PRIMARY KEY, name text NOT NULL,
				email text NOT NULL, age int NOT NULL, created_at timestamptz NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			return db
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.db(t)
			seed(t, db)
			update := func(changes []ageChange) *barm.UpdateQuery[User] {
				return db.Update[User]().
					With("data", db.Values(changes)).
					From("data").
					Set("age = data.age, created_at = data.since").
					Where("batch_users.name = data.name AND batch_users.age < data.age")
			}
			if _, err := update([]ageChange{{"ann", 21, since}, {"bo", 1, since}}).Exec(ctx); err != nil {
				t.Fatalf("plain: %v", err)
			}
			if _, err := update([]ageChange{{"bo", 31, since}}).Prepare("upd_from").Exec(ctx); err != nil {
				t.Fatalf("prepared: %v", err)
			}
			b := db.Batch()
			r := b.Exec(update([]ageChange{{"cy", 41, since}, {"nobody", 1, since}}))
			if err := b.Run(ctx); err != nil {
				t.Fatalf("batch: %v", err)
			}
			if r.Value().RowsAffected != 1 {
				t.Errorf("batch updated %d rows", r.Value().RowsAffected)
			}
			us, err := db.Select[User]().OrderBy("name").Slice(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for i, want := range []int{21, 31, 41} {
				if us[i].Age != want || !us[i].CreatedAt.Equal(since) {
					t.Errorf("%s: age %d at %v, want %d at %v", us[i].Name, us[i].Age, us[i].CreatedAt, want, since)
				}
			}
		})
	}
}

// A delete matches a VALUES list on two columns at once.
func TestDeleteUsingValuesOnPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	type key struct {
		Name string `barm:"name"`
		Age  int    `barm:"age"`
	}
	res, err := db.Delete[User]().
		With("data", db.Values([]key{{"ann", 20}, {"bo", 99}, {"cy", 40}})).
		Using("data").
		Where("batch_users.name = data.name AND batch_users.age = data.age").
		Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 2 {
		t.Errorf("deleted %d rows, want ann and cy", n)
	}
	left, err := db.Select[User]().Slice(ctx)
	if err != nil || len(left) != 1 || left[0].Name != "bo" {
		t.Errorf("left %+v, %v", left, err)
	}
}

type userArchive struct {
	barm.BaseModel `barm:"table:user_archive"`

	ID   int64  `barm:"id,pk,autoincrement"`
	Name string `barm:"name"`
	Age  int    `barm:"age"`
}

// An insert takes its rows from a query — a CTE over a VALUES list here — and
// RETURNING reads what it wrote, through Slice; Exec writes nothing back,
// having no values, and a batch runs it too.
func TestInsertSelectOnPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	for _, q := range []string{
		`DROP TABLE IF EXISTS user_archive`,
		`CREATE TABLE user_archive (id bigserial PRIMARY KEY, name text UNIQUE, age int)`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS user_archive`) })
	type pick struct {
		Name string `barm:"name"`
	}
	archive := func(names ...string) *barm.InsertQuery[userArchive] {
		picks := make([]pick, len(names))
		for i, n := range names {
			picks[i].Name = n
		}
		return db.Insert[userArchive]().
			With("picked", db.Values(picks)).
			Select(db.NewRaw("SELECT u.name, u.age + ? FROM batch_users AS u JOIN picked ON picked.name = u.name", 100)).
			On("CONFLICT (name) DO NOTHING")
	}

	got, err := archive("ann", "bo").Returning("name, age").Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b userArchive) int { return strings.Compare(a.Name, b.Name) })
	if len(got) != 2 || got[0].Name != "ann" || got[0].Age != 120 || got[1].Age != 130 {
		t.Errorf("returned %+v", got)
	}
	res, err := archive("bo", "cy").Returning("id").Exec(ctx)
	if err != nil {
		t.Fatalf("exec with returning: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("exec inserted %d rows, want cy alone", n)
	}
	b := db.Batch()
	r := b.Exec(archive("ann", "nobody").Returning("id"))
	if err := b.Run(ctx); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if r.Value().RowsAffected != 0 {
		t.Errorf("batch inserted %d rows over a conflict", r.Value().RowsAffected)
	}
	n, err := db.Select[userArchive]().Count(ctx)
	if err != nil || n != 3 {
		t.Errorf("archive holds %d rows, %v", n, err)
	}
}

// DISTINCT ON keeps the row ORDER BY puts first in each group: the oldest user
// of each age bracket here.
func TestDistinctOnPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	q := db.Select[User]().
		DistinctOn("age >= 30").
		OrderBy("age >= 30, age DESC")
	us, err := q.Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 2 || us[0].Name != "ann" || us[1].Name != "cy" {
		t.Errorf("got %+v, want ann (under 30) and cy (oldest at 30 or over)", us)
	}
	n, err := q.Count(ctx)
	if err != nil || n != 2 {
		t.Errorf("count = %d, %v", n, err)
	}
}

// A group keeps its OR from binding across the AND before it: without the
// parentheses cy, aged 40, would match too.
func TestWhereGroupPgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	us, err := db.Select[User]().
		Where("name <> ?", "cy").
		WhereGroup(func(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
			return q.Where("age = ?", 20).WhereOr("age = ?", 40)
		}).
		Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 1 || us[0].Name != "ann" {
		t.Errorf("got %+v, want ann alone", us)
	}
	res, err := db.Delete[User]().
		WhereGroup(func(q *barm.DeleteQuery[User]) *barm.DeleteQuery[User] {
			return q.Where("age = ?", 30).WhereOr("age = ?", 40)
		}).
		Where("name = ?", "bo").
		Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("deleted %d rows, want bo alone", n)
	}
}

// A temp table named at run time joins through Ident, and a raw expression goes
// in through Safe — in a plain query, a prepared one and a batch alike.
func TestIdentAndSafePgx(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seed(t, db)
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	tmp := `tmp "ids"` // a name that needs its quotes
	if _, err := c.NewRaw("CREATE TEMP TABLE ? (name text)", barm.Ident(tmp)).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.NewRaw("INSERT INTO ? VALUES ('bo'), ('cy')", barm.Ident(tmp)).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	q := func() *barm.SelectQuery[User] {
		return c.Select[User]().
			Join("JOIN ? AS picked ON picked.name = u.name", barm.Ident(tmp)).
			Where("? > ?", barm.Safe("u.age + 0"), 35)
	}
	check := func(what string, us []User, err error) {
		t.Helper()
		if err != nil || len(us) != 1 || us[0].Name != "cy" {
			t.Errorf("%s: %+v, %v", what, us, err)
		}
	}
	us, err := q().Slice(ctx)
	check("plain", us, err)
	us, err = q().Prepare("ident_safe").Slice(ctx)
	check("prepared", us, err)
	b := c.Batch()
	r := b.Slice(q())
	err = b.Run(ctx)
	check("batch", r.Value(), err)
}

type ckTemplate struct {
	barm.BaseModel `barm:"table:ck_templates,alias:p"`

	ID      string `barm:"id,pk"`
	Version int    `barm:"version,pk"`
	Text    string `barm:"text"`
}

type ckPage struct {
	barm.BaseModel `barm:"table:ck_pages,alias:s"`

	DocID      string `barm:"doc_id"`
	DocVersion int    `barm:"doc_version"`
	Pos        int    `barm:"pos"`
}

type ckDoc struct {
	barm.BaseModel `barm:"table:ck_docs,alias:t"`

	ID              string      `barm:"id,pk"`
	Version         int         `barm:"version,pk"`
	TemplateID      *string     `barm:"template_id"`
	TemplateVersion *int        `barm:"template_version"`
	Template        *ckTemplate `barm:"rel:template_id=id,rel:template_version=version"`
	Pages           []ckPage    `barm:"rel:id=doc_id,rel:version=doc_version"`
}

// pageLite leaves the key out of its columns, so it is fetched for grouping
// and nowhere else.
type pageLite struct {
	barm.BaseModel `barm:"table:ck_pages"`

	Pos int `barm:"pos"`
}

type ckDocLite struct {
	ID      string     `barm:"id"`
	Version int        `barm:"version"`
	Pages   []pageLite `barm:"rel:id=doc_id,rel:version=doc_version"`
}

func seedComposite(t *testing.T, db *barm.DB) {
	t.Helper()
	for _, q := range []string{
		`DROP TABLE IF EXISTS ck_docs, ck_templates, ck_pages`,
		`CREATE TABLE ck_templates (id text, version int, text text, PRIMARY KEY (id, version))`,
		`CREATE TABLE ck_docs (id text, version int, template_id text, template_version int, PRIMARY KEY (id, version))`,
		`CREATE TABLE ck_pages (doc_id text, doc_version int, pos int)`,
		// Same ids at different versions: a key on either column alone would
		// hand a doc the wrong template or another version's pages.
		`INSERT INTO ck_templates VALUES ('t1', 1, 't1 v1'), ('t1', 2, 't1 v2'), ('t2', 1, 't2 v1')`,
		`INSERT INTO ck_docs VALUES ('a', 1, 't1', 1), ('a', 2, 't1', 2), ('b', 1, 't1', 2), ('c', 1, NULL, NULL)`,
		`INSERT INTO ck_pages VALUES ('a', 1, 10), ('a', 2, 20), ('a', 2, 21), ('b', 1, 30)`,
	} {
		if _, err := db.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Exec(context.Background(), `DROP TABLE IF EXISTS ck_docs, ck_templates, ck_pages`) })
}

// A relation keyed on two columns matches on both: a belongs-to by id and
// version, and a has-many the same way. The keys go as one array per column, so the SQL is the same for any
// number of parents.
func TestCompositeRelation(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	var children []string
	hook := barm.WithHook(barm.QueryHook{AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
		if strings.HasPrefix(ev.Query, "SELECT") && (strings.Contains(ev.Query, "ck_templates") || strings.Contains(ev.Query, "ck_pages")) {
			mu.Lock()
			children = append(children, ev.Query)
			mu.Unlock()
		}
	}})
	for _, tc := range []struct {
		name string
		db   func(*testing.T) *barm.DB
	}{
		{"batched", func(t *testing.T) *barm.DB { return open(t, hook) }},
		{"one by one", func(t *testing.T) *barm.DB { return openPool(t, 1, hook) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			children = nil
			db := tc.db(t)
			seedComposite(t, db)
			docs, err := db.Select[ckDoc]().
				Relation[ckTemplate]("Template").
				Relation[ckPage]("Pages", func(q *barm.SelectQuery[ckPage]) *barm.SelectQuery[ckPage] { return q.OrderBy("pos") }).
				OrderBy("id, version").
				Slice(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := []struct {
				pre   string
				pages []int
			}{{"t1 v1", []int{10}}, {"t1 v2", []int{20, 21}}, {"t1 v2", []int{30}}, {"", nil}}
			if len(docs) != len(want) {
				t.Fatalf("%d docs", len(docs))
			}
			for i, w := range want {
				c := docs[i]
				pre := ""
				if c.Template != nil {
					pre = c.Template.Text
				}
				var pos []int
				for _, s := range c.Pages {
					pos = append(pos, s.Pos)
					if s.DocID != c.ID || s.DocVersion != c.Version {
						t.Errorf("%s v%d holds %+v", c.ID, c.Version, s)
					}
				}
				if pre != w.pre || !slices.Equal(pos, w.pages) {
					t.Errorf("%s v%d: template %q, pages %v; want %q, %v", c.ID, c.Version, pre, pos, w.pre, w.pages)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(children) != 2 {
				t.Errorf("%d child queries, want one per relation", len(children))
			}
			for _, q := range children {
				if !strings.Contains(q, "IN (SELECT * FROM unnest(") || strings.Count(q, "$") != 2 {
					t.Errorf("child query = %s, want two array parameters", q)
				}
			}
		})
	}
}

// A projection that leaves the key columns out still groups by them: they are
// fetched for that and land nowhere.
func TestCompositeRelationKeyNotSelected(t *testing.T) {
	ctx := t.Context()
	db := open(t)
	seedComposite(t, db)
	docs, err := db.Select[ckDoc]().
		Relation[pageLite]("Pages", func(q *barm.SelectQuery[pageLite]) *barm.SelectQuery[pageLite] { return q.OrderBy("pos") }).
		OrderBy("id, version").
		SliceAs[ckDocLite](ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]int
	for _, c := range docs {
		var pos []int
		for _, s := range c.Pages {
			pos = append(pos, s.Pos)
		}
		got = append(got, pos)
	}
	if want := [][]int{{10}, {20, 21}, {30}, nil}; !reflect.DeepEqual(got, want) {
		t.Errorf("pages = %v, want %v", got, want)
	}
}
