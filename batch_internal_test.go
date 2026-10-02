package barm

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// nopExec is an Executor that runs nothing, for batches that are only queued.
type nopExec struct{}

func (nopExec) Query(context.Context, string, []any) (Rows, error)      { return nil, nil } //nolint:nilnil // never read
func (nopExec) Exec(context.Context, string, []any) (sql.Result, error) { return rowsAffected(0), nil }
func (nopExec) ExecPrepared(context.Context, string, string, []any) (sql.Result, error) {
	return rowsAffected(0), nil
}
func (nopExec) QueryPrepared(context.Context, string, string, []any) (Rows, error) {
	return nil, nil //nolint:nilnil // never read
}
func (nopExec) SendBatch(context.Context, []BatchQuery, func(BatchReader) error) error { return nil }

// The statement each of these queues is decided by what the batch runs on.
func TestBatchTxStatementsFollowWhereItRuns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		inTx, held    bool
		begin, commit string
	}{
		{"pool", false, false, "BEGIN", "COMMIT"},
		{"held connection", false, true, "BEGIN", "COMMIT"},
		{"transaction", true, true, "SAVEPOINT " + BatchSavepoint, "RELEASE SAVEPOINT " + BatchSavepoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, step := range []struct {
				what  string
				queue func(*Batch) *BatchResult[ExecResult]
				want  string
			}{
				{"Begin", (*Batch).Begin, tc.begin},
				{"Commit", (*Batch).Commit, tc.commit},
			} {
				b := newBatch(nopExec{}, &session{}, tc.inTx, tc.held)
				step.queue(b)
				if len(b.qs) != 1 {
					t.Fatalf("%s queued %d statements", step.what, len(b.qs))
				}
				if got := b.qs[0].Query; got != step.want {
					t.Errorf("%s = %q, want %q", step.what, got, step.want)
				}
			}
		})
	}
}

// Rollback runs rather than queueing, so it needs the connection the batch ran
// on. A batch on the pool has already given that back.
func TestBatchRollbackNeedsAHeldConnection(t *testing.T) {
	t.Parallel()
	b := newBatch(nopExec{}, &session{}, false, false)
	b.Begin()
	err := b.Rollback(context.Background())
	if err == nil {
		t.Fatal("no error rolling back a batch that holds no connection")
	}
	if !strings.Contains(err.Error(), "does not hold one") {
		t.Errorf("unhelpful error: %v", err)
	}
	// and it queued nothing: Begin is the only statement in the batch
	if len(b.qs) != 1 {
		t.Errorf("Rollback queued a statement: %v", b.qs)
	}
}
