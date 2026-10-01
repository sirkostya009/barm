package bench

import (
	"database/sql"
	"testing"

	"github.com/sirkostya009/barm"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

type barmRelAuthor struct {
	barm.BaseModel `barm:"table:rel_authors,alias:a"`

	ID     int64          `barm:"id,pk,autoincrement"`
	Name   string         `barm:"name"`
	Books  []barmRelBook  `barm:"rel:id=author_id"`
	Tags   []barmRelTag   `barm:"rel:id=author_id"`
	Awards []barmRelAward `barm:"rel:id=author_id"`
}

type barmRelAward struct {
	barm.BaseModel `barm:"table:rel_awards"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Name     string `barm:"name"`
}

type barmRelBook struct {
	barm.BaseModel `barm:"table:rel_books"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Title    string `barm:"title"`
}

type barmRelTag struct {
	barm.BaseModel `barm:"table:rel_tags"`

	ID       int64  `barm:"id,pk,autoincrement"`
	AuthorID int64  `barm:"author_id"`
	Label    string `barm:"label"`
}

type bunRelAuthor struct {
	bun.BaseModel `bun:"table:rel_authors,alias:a"`

	ID     int64         `bun:"id,pk,autoincrement"`
	Name   string        `bun:"name"`
	Books  []bunRelBook  `bun:"rel:has-many,join:id=author_id"`
	Tags   []bunRelTag   `bun:"rel:has-many,join:id=author_id"`
	Awards []bunRelAward `bun:"rel:has-many,join:id=author_id"`
}

type bunRelAward struct {
	bun.BaseModel `bun:"table:rel_awards,alias:w"`

	ID       int64  `bun:"id,pk,autoincrement"`
	AuthorID int64  `bun:"author_id"`
	Name     string `bun:"name"`
}

type bunRelBook struct {
	bun.BaseModel `bun:"table:rel_books,alias:b"`

	ID       int64  `bun:"id,pk,autoincrement"`
	AuthorID int64  `bun:"author_id"`
	Title    string `bun:"title"`
}

type bunRelTag struct {
	bun.BaseModel `bun:"table:rel_tags,alias:t"`

	ID       int64  `bun:"id,pk,autoincrement"`
	AuthorID int64  `bun:"author_id"`
	Label    string `bun:"label"`
}

// openRel seeds authors with a fixed number of books and tags each.
func openRel(b *testing.B, authors, perAuthor int) (*barm.DB, *bun.DB) {
	b.Helper()
	sqldb, err := sql.Open("sqlite", "file:"+b.TempDir()+"/rel.db")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sqldb.Close() })
	for _, q := range []string{
		`CREATE TABLE rel_authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`,
		`CREATE TABLE rel_books (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, title TEXT)`,
		`CREATE TABLE rel_tags (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER, label TEXT)`,
		`CREATE INDEX rel_books_author ON rel_books (author_id)`,
		`CREATE INDEX rel_tags_author ON rel_tags (author_id)`,
	} {
		if _, err := sqldb.Exec(q); err != nil {
			b.Fatal(err)
		}
	}
	tx, err := sqldb.Begin()
	if err != nil {
		b.Fatal(err)
	}
	for a := 1; a <= authors; a++ {
		if _, err := tx.Exec(`INSERT INTO rel_authors (id, name) VALUES (?, ?)`, a, "author"); err != nil {
			b.Fatal(err)
		}
		for range perAuthor {
			if _, err := tx.Exec(`INSERT INTO rel_books (author_id, title) VALUES (?, ?)`, a, "a book title"); err != nil {
				b.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO rel_tags (author_id, label) VALUES (?, ?)`, a, "label"); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return barm.New(sqldb, barm.SQLite), bun.NewDB(sqldb, sqlitedialect.New())
}

var (
	sinkBarmRel []barmRelAuthor
	sinkBunRel  []bunRelAuthor
)

func BenchmarkRelationOne(b *testing.B) {
	for _, c := range []struct {
		name               string
		authors, perAuthor int
	}{
		{"10x10", 10, 10},
		{"100x10", 100, 10},
		{"1000x5", 1000, 5},
	} {
		bd, bn := openRel(b, c.authors, c.perAuthor)
		ctx := b.Context()
		agree(b, bd, bn, c.authors, c.perAuthor)

		b.Run("barm/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var err error
				sinkBarmRel, err = bd.Select[barmRelAuthor]().
					Relation[barmRelBook]("Books").
					Slice(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("bun/"+c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var out []bunRelAuthor
				if err := bn.NewSelect().Model(&out).Relation("Books").Scan(ctx); err != nil {
					b.Fatal(err)
				}
				sinkBunRel = out
			}
		})
	}
}

func BenchmarkRelationTwo(b *testing.B) {
	bd, bn := openRel(b, 100, 10)
	ctx := b.Context()

	b.Run("barm", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var err error
			sinkBarmRel, err = bd.Select[barmRelAuthor]().
				Relation[barmRelBook]("Books").
				Relation[barmRelTag]("Tags").
				Slice(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("bun", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var out []bunRelAuthor
			if err := bn.NewSelect().Model(&out).Relation("Books").Relation("Tags").Scan(ctx); err != nil {
				b.Fatal(err)
			}
			sinkBunRel = out
		}
	})
}

// agree checks both libraries load the same rows, or the numbers mean nothing.
func agree(b *testing.B, bd *barm.DB, bn *bun.DB, authors, perAuthor int) {
	b.Helper()
	ctx := b.Context()
	x, err := bd.Select[barmRelAuthor]().Relation[barmRelBook]("Books").Slice(ctx)
	if err != nil {
		b.Fatal(err)
	}
	var y []bunRelAuthor
	if err := bn.NewSelect().Model(&y).Relation("Books").Scan(ctx); err != nil {
		b.Fatal(err)
	}
	if len(x) != authors || len(y) != authors {
		b.Fatalf("authors: barm %d, bun %d, want %d", len(x), len(y), authors)
	}
	for i := range x {
		if len(x[i].Books) != perAuthor || len(y[i].Books) != perAuthor {
			b.Fatalf("author %d books: barm %d, bun %d, want %d", i, len(x[i].Books), len(y[i].Books), perAuthor)
		}
	}
}
