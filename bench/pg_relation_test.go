package bench

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirkostya009/barm"
	"github.com/sirkostya009/barm/pgxdriver"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// openPG seeds Postgres with authors and perAuthor books, tags and awards each.
// barm runs on pgx's own pool, bun on database/sql over pgx, each as it is used.
func openPG(b *testing.B, authors, perAuthor int) (*barm.DB, *bun.DB) {
	b.Helper()
	dsn := os.Getenv("PGDSN")
	if dsn == "" {
		dsn = "postgres://postgres@localhost/postgres"
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Skip(err)
	}
	if err := sqldb.Ping(); err != nil {
		b.Skipf("no postgres: %v", err)
	}
	b.Cleanup(func() { sqldb.Close() })

	a, n := strconv.Itoa(authors), strconv.Itoa(perAuthor)
	for _, q := range []string{
		`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags, rel_awards`,
		`CREATE TABLE rel_authors (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE rel_books (id bigserial PRIMARY KEY, author_id bigint, title text)`,
		`CREATE TABLE rel_tags (id bigserial PRIMARY KEY, author_id bigint, label text)`,
		`CREATE TABLE rel_awards (id bigserial PRIMARY KEY, author_id bigint, name text)`,
		`CREATE INDEX ON rel_books (author_id)`,
		`CREATE INDEX ON rel_tags (author_id)`,
		`CREATE INDEX ON rel_awards (author_id)`,
		`INSERT INTO rel_authors (id, name) SELECT i, 'author' FROM generate_series(1, ` + a + `) i`,
		`INSERT INTO rel_books (author_id, title) SELECT i, 'a book title' FROM generate_series(1, ` + a + `) i, generate_series(1, ` + n + `)`,
		`INSERT INTO rel_tags (author_id, label) SELECT i, 'label' FROM generate_series(1, ` + a + `) i, generate_series(1, ` + n + `)`,
		`INSERT INTO rel_awards (author_id, name) SELECT i, 'award' FROM generate_series(1, ` + a + `) i, generate_series(1, ` + n + `)`,
		`VACUUM ANALYZE rel_authors, rel_books, rel_tags, rel_awards`,
	} {
		if _, err := sqldb.Exec(q); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { sqldb.Exec(`DROP TABLE IF EXISTS rel_authors, rel_books, rel_tags, rel_awards`) })
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(pool.Close)
	return barm.New(pgxdriver.Pool(pool), barm.Postgres), bun.NewDB(sqldb, pgdialect.New())
}

// barmLoad loads the first n relations through h.
func barmLoad(ctx context.Context, idb barm.IDB, n int) ([]barmRelAuthor, error) {
	q := barm.Handle{IDB: idb}.Select[barmRelAuthor]()
	if n >= 1 {
		q.Relation[barmRelBook]("Books")
	}
	if n >= 2 {
		q.Relation[barmRelTag]("Tags")
	}
	if n >= 3 {
		q.Relation[barmRelAward]("Awards")
	}
	return q.Slice(ctx)
}

// bunLoad does the same through either a bun.DB or a bun.Conn.
func bunLoad(ctx context.Context, q *bun.SelectQuery, n int) ([]bunRelAuthor, error) {
	var out []bunRelAuthor
	q = q.Model(&out)
	for _, name := range []string{"Books", "Tags", "Awards"}[:n] {
		q = q.Relation(name)
	}
	return out, q.Scan(ctx)
}

func BenchmarkPGRelations(b *testing.B) {
	const authors, perAuthor = 100, 10
	bd, bn := openPG(b, authors, perAuthor)
	ctx := b.Context()

	// Same rows from both, over both kinds of handle, before anything is timed.
	for n := 1; n <= 3; n++ {
		x, err := barmLoad(ctx, bd, n)
		if err != nil {
			b.Fatal(err)
		}
		y, err := bunLoad(ctx, bn.NewSelect(), n)
		if err != nil {
			b.Fatal(err)
		}
		if len(x) != authors || len(y) != authors {
			b.Fatalf("authors at %d relations: barm %d, bun %d", n, len(x), len(y))
		}
		for i := range x {
			got := [3][2]int{
				{len(x[i].Books), len(y[i].Books)},
				{len(x[i].Tags), len(y[i].Tags)},
				{len(x[i].Awards), len(y[i].Awards)},
			}
			for r := range 3 {
				want := 0
				if r < n {
					want = perAuthor
				}
				if got[r][0] != want || got[r][1] != want {
					b.Fatalf("author %d relation %d at %d relations: barm %d, bun %d, want %d",
						i, r, n, got[r][0], got[r][1], want)
				}
			}
		}
	}

	// Fresh tables are slow for the first few hundred reads, which would land on
	// whichever case happens to run first.
	for range 200 {
		if _, err := barmLoad(ctx, bd, 3); err != nil {
			b.Fatal(err)
		}
		if _, err := bunLoad(ctx, bn.NewSelect(), 3); err != nil {
			b.Fatal(err)
		}
	}

	bc, err := bd.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { bc.Close() })
	nc, err := bn.Conn(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { nc.Close() })

	for n := 1; n <= 3; n++ {
		rels := strconv.Itoa(n)
		b.Run("pool/"+rels+"/barm", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := barmLoad(ctx, bd, n); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("pool/"+rels+"/bun", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := bunLoad(ctx, bn.NewSelect(), n); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("conn/"+rels+"/barm", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := barmLoad(ctx, bc, n); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("conn/"+rels+"/bun", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := bunLoad(ctx, nc.NewSelect(), n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
