package barm_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/sirkostya009/barm"
)

// Author is the model the examples build queries against.
type Author struct {
	barm.BaseModel `barm:"table:authors,alias:a"`

	ID   int64  `barm:"id,pk,autoincrement"`
	Name string `barm:"name"`
	Born int    `barm:"born"`
}

// db renders SQL without a connection, which is all most of these examples need.
// Real code passes a *sql.DB to barm.New.
var db = barm.NewBuilder(barm.Postgres)

func Example() {
	query, args, err := db.Select[Author]().
		Where("born >= ?", 1900).
		OrderBy("name").
		Limit(10).
		Build()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE born >= $1 ORDER BY name LIMIT 10
	// [1900]
}

func ExampleDB_Select() {
	query, _, _ := db.Select[Author]().Where("id = ?", 1).Build()
	fmt.Println(query)
	// Output:
	// SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE id = $1
}

// A narrower result type narrows the SQL: only its columns are selected.
func ExampleSelectQuery_BuildAs() {
	query, _, _ := db.Select[Author]().BuildAs[struct {
		Name string `barm:"name"`
		Born int    `barm:"born"`
	}]()
	fmt.Println(query)
	// Output:
	// SELECT "a"."name", "a"."born" FROM "authors" AS "a"
}

// An explicit projection wins over the result type, which is what lets a query
// be read into a scalar.
func ExampleSelectQuery_ColumnExpr() {
	query, _, _ := db.Select[Author]().ColumnExpr("max(born)").BuildAs[int]()
	fmt.Println(query)
	// Output:
	// SELECT max(born) FROM "authors" AS "a"
}

// Apply threads a query through named clauses. A nil function is skipped, so an
// optional filter needs no branch at the call site.
func ExampleSelectQuery_Apply() {
	living := func(q *barm.SelectQuery[Author]) *barm.SelectQuery[Author] {
		return q.Where("died IS NULL")
	}
	bornAfter := func(year int) func(*barm.SelectQuery[Author]) *barm.SelectQuery[Author] {
		return func(q *barm.SelectQuery[Author]) *barm.SelectQuery[Author] {
			return q.Where("born > ?", year)
		}
	}
	var maybe func(*barm.SelectQuery[Author]) *barm.SelectQuery[Author]

	query, args, _ := db.Select[Author]().Apply(living, maybe, bornAfter(1950)).Build()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE (died IS NULL) AND (born > $1)
	// [1950]
}

// Exists stops at the first match instead of counting rows it will discard.
func ExampleSelectQuery_ExistsQuery() {
	query, args, _ := db.Select[Author]().Where("name = ?", "Le Guin").ExistsQuery()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT EXISTS (SELECT 1 FROM "authors" AS "a" WHERE name = $1 LIMIT 1)
	// [Le Guin]
}

func ExampleSelectQuery_CountQuery() {
	query, _, _ := db.Select[Author]().Where("born < ?", 1900).CountQuery()
	fmt.Println(query)
	// Output:
	// SELECT count(*) FROM "authors" AS "a" WHERE born < $1
}

// Table names a table no model has to describe, which is how a join is built.
// The row type goes up front, so the terminal needs no type argument.
func ExampleSelectQuery_Table() {
	query, _, _ := db.Select[struct {
		Name  string `barm:"name"`
		Books int64  `barm:"books"`
	}]().
		Table("authors a").
		Join("JOIN books b ON b.author_id = a.id").
		Column("a.name").
		ColumnExpr("count(b.id) AS books").
		GroupBy("a.name").
		Build()
	fmt.Println(query)
	// Output:
	// SELECT "a"."name", count(b.id) AS books FROM authors a JOIN books b ON b.author_id = a.id GROUP BY a.name
}

func ExampleDB_Insert() {
	query, args, _ := db.Insert[Author]().
		Values(&Author{Name: "Borges", Born: 1899}, &Author{Name: "Calvino", Born: 1923}).
		Build()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// INSERT INTO "authors" ("name", "born") VALUES ($1, $2), ($3, $4)
	// [Borges 1899 Calvino 1923]
}

// A write can hand back rows of a different type: a bare-bones value goes in,
// the full model comes out. Without an explicit Returning the clause is built
// from the result type's own columns.
func ExampleInsertQuery_BuildAs() {
	type draft struct {
		Name string `barm:"name"`
	}
	query, args, _ := db.Insert[draft]().Table("authors").Values(&draft{Name: "Lem"}).BuildAs[Author]()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// INSERT INTO "authors" ("name") VALUES ($1) RETURNING "id", "name", "born"
	// [Lem]
}

// Without a Where, the primary key of the bound value becomes the condition.
func ExampleDB_Update() {
	query, args, _ := db.Update[Author]().
		Value(&Author{ID: 7, Name: "Stanisław Lem", Born: 1921}).
		Column("name").
		WherePK().
		Build()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// UPDATE "authors" SET "name" = $1 WHERE "id" = $2
	// [Stanisław Lem 7]
}

func ExampleDB_Delete() {
	query, args, _ := db.Delete[Author]().Where("born < ?", 1800).Build()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// DELETE FROM "authors" WHERE born < $1
	// [1800]
}

// An UPDATE or DELETE says which rows, with Where or WherePK. One that does not
// is an error rather than a full-table write.
func ExampleUpdateQuery_Build_noWhere() {
	_, _, err := db.Update[Author]().Set("born = born + 1").Build()
	fmt.Println(err)
	// Output:
	// barm: no WHERE clause — say which rows, with Where or WherePK
}

// Fragments are written with `?` whatever the dialect. `?N` refers to the N-th
// argument of its own fragment, `??` is a literal question mark, and a slice is
// one argument, which the driver sends as an array, unless In spells it out.
func Example_placeholders() {
	query, args, _ := db.Select[Author]().
		Where("name = ?1 OR pen_name = ?1", "Twain").
		Where("id = ANY(?)", []int64{1, 2, 3}).
		Where("born IN (?)", barm.In([]int{1835, 1899})).
		Where("data ?? 'key'").
		Build()
	fmt.Println(query)
	fmt.Println(args)
	// Output:
	// SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE (name = $1 OR pen_name = $1) AND (id = ANY($2)) AND (born IN ($3, $4)) AND (data ? 'key')
	// [Twain [1 2 3] 1835 1899]
}

// The same builder renders for each dialect: bind markers and quoting follow the
// dialect, the fragments do not change.
func Example_dialects() {
	for _, d := range []barm.Dialect{barm.Postgres, barm.MySQL, barm.SQLite} {
		q, _, _ := barm.NewBuilder(d).Select[Author]().Where("born > ?", 1900).Build()
		fmt.Printf("%-8s %s\n", d.Name(), q)
	}
	// Output:
	// postgres SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE born > $1
	// mysql    SELECT `a`.`id`, `a`.`name`, `a`.`born` FROM `authors` AS `a` WHERE born > ?
	// sqlite   SELECT "a"."id", "a"."name", "a"."born" FROM "authors" AS "a" WHERE born > ?
}

// The examples below need a live database, so they are compiled but not run.

func ExampleNew() {
	sqldb, err := sql.Open("pgx", "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}
	db := barm.New(barm.SQL(sqldb), barm.Postgres)
	defer db.Close()

	authors, err := db.Select[Author]().Where("born >= ?", 1900).Slice(context.Background())
	if err != nil {
		log.Fatal(err) //nolint:gocritic // log.Fatal exits the process, so the deferred Close is moot
	}
	fmt.Println(len(authors))
}

// Seq streams rows and closes them on break.
func ExampleSelectQuery_Seq() {
	ctx := context.Background()
	for author, err := range db.Select[Author]().OrderBy("name").Seq(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		if author.Born > 1950 {
			break // the rows are closed on the way out
		}
		fmt.Println(author.Name)
	}
}

// A write streams what it returned, so a large delete need not be collected.
func ExampleDeleteQuery_Seq() {
	ctx := context.Background()
	for gone, err := range db.Delete[Author]().Where("born < ?", 1800).Seq(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("removed", gone.Name)
	}
}

// Transactions nest: a nested Begin is a savepoint, and only the outermost
// Commit or Rollback ends the transaction.
func ExampleDB_Begin() {
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }() // a no-op once committed

	_, err = tx.Insert[Author]().Values(&Author{Name: "Borges"}).Exec(ctx)
	if err != nil {
		log.Fatal(err) //nolint:gocritic // log.Fatal exits the process, so the deferred Rollback is moot
	}

	inner, err := tx.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = inner.Rollback() }() // undoes only the inner work
	_, err = inner.Insert[Author]().Values(&Author{Name: "Cortázar"}).Exec(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = inner.Commit()
	if err != nil {
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

// A batch sends every queued query in one round trip. Each result is typed to
// what its query returns, so a loop can queue any number of them.
func ExampleDB_Batch() {
	ctx := context.Background()

	b := db.Batch()
	recent := b.Slice(db.Select[Author]().Where("born >= ?", 1900))
	total := b.Count(db.Select[Author]())
	ids := make([]*barm.BatchResult[[]Author], 3)
	for i := range ids {
		ids[i] = b.Slice(db.Select[Author]().Where("id = ?", i+1))
	}

	err := b.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(recent.Value()), total.Value())
}

// Prepare caches a statement on the DB under a name. The name pins one SQL
// text, so reusing it for different SQL is an error rather than a re-prepare.
func ExampleSelectQuery_Prepare() {
	ctx := context.Background()

	author, err := db.Select[Author]().
		Where("id = ?", 42).
		Prepare("author_by_id").
		One(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(author.Name)
}

// A hook sees every query. Both fields are optional, and a slow-query log wants
// only the one: the context returned by BeforeQuery is what reaches the database
// call and comes back to AfterQuery, which is where a tracing hook keeps its
// span, but a log that reports what already happened needs none of that.
func ExampleWithHook() {
	sqldb, err := sql.Open("pgx", "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}
	db := barm.New(barm.SQL(sqldb), barm.Postgres, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
			if ev.Duration > time.Second {
				log.Printf("slow %s (%s): %s", ev.Op, ev.Duration, ev.Query)
			}
		},
	}))
	defer db.Close()
}

// A transaction hook sees commits and rollbacks, once, where the transaction
// really ends. BeforeCommit is the one that can refuse: returning an error stops
// the commit and leaves the transaction open, so work that has to land inside it
// cannot fail silently.
func ExampleWithTxHook() {
	sqldb, err := sql.Open("pgx", "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}
	var outbox interface{ flush(context.Context) error }

	db := barm.New(barm.SQL(sqldb), barm.Postgres, barm.WithTxHook(barm.TxHook{
		BeforeCommit: func(ctx context.Context, ev *barm.TxEvent) (context.Context, error) {
			return ctx, outbox.flush(ctx)
		},
	}))
	defer db.Close()
}

// Registering on the transaction itself is how work decides what to report when
// it commits — a cache invalidated for the rows this unit of work touched, and
// only if it is actually kept.
func ExampleTx_WithTxHook() {
	ctx := context.Background()
	var cache interface{ evict(...string) }

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Update[Author]().Set("born = ?", 1921).Where("id = ?", 7).Exec(ctx)
	if err != nil {
		log.Fatal(err) //nolint:gocritic // log.Fatal exits the process, so the deferred Rollback is moot
	}
	tx.WithTxHook(barm.TxHook{
		AfterCommit: func(context.Context, *barm.TxEvent) { cache.evict("author:7") },
	})

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
