package pgxdriver

import (
	"os"
	"testing"
	"unsafe"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirkostya009/barm"
)

// Every read of a statement the connection keeps shares its column names,
// rather than building them per result.
func TestBatchColumnsShared(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("PGDSN")
	if dsn == "" {
		t.Skip("PGDSN unset")
	}
	ctx := t.Context()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	q := barm.BatchQuery{Query: "SELECT $1::int AS a, $2::text AS b", Args: []any{1, "x"}}
	var cols [][]string
	err = Pool(p).SendBatch(ctx, []barm.BatchQuery{q, q}, func(br barm.BatchReader) error {
		for range 2 {
			rows, err := br.Rows()
			if err != nil {
				return err
			}
			c, err := rows.Columns()
			if err != nil {
				return err
			}
			cols = append(cols, c)
			err = rows.Close()
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cols[0]) != 2 || cols[0][0] != "a" || cols[0][1] != "b" {
		t.Fatalf("columns = %v", cols[0])
	}
	if unsafe.SliceData(cols[0]) != unsafe.SliceData(cols[1]) {
		t.Error("each result built its own column names")
	}
}
