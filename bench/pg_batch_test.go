package bench

import (
	"testing"

	"github.com/sirkostya009/barm"
)

// BenchmarkPGPrepared runs the same reads plain and named with Prepare, alone
// and batched, on pgx's pool in its default mode.
func BenchmarkPGPrepared(b *testing.B) {
	bd, _ := openPG(b, 100, 10)
	ctx := b.Context()
	sel := func(name, shape string) *barm.SelectQuery[barmRelBook] {
		q := bd.Select[barmRelBook]().Where("author_id = ?", 7)
		if name != "" {
			q.Prepare(name + "_" + shape)
		}
		return q
	}
	for _, name := range []string{"", "bench_books"} {
		kind := "plain"
		if name != "" {
			kind = "named"
		}
		b.Run("one/"+kind, func(b *testing.B) {
			for b.Loop() {
				if _, err := sel(name, "one").One(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("slice/"+kind, func(b *testing.B) {
			for b.Loop() {
				if _, err := sel(name, "slice").Slice(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("batch/"+kind, func(b *testing.B) {
			for b.Loop() {
				bt := bd.Batch()
				for range 5 {
					bt.Slice(sel(name, "slice"))
				}
				if err := bt.Run(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
