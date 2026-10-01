package barm

import (
	"context"
	"strings"
	"testing"
)

// The statement each of these queues is decided by what the batch runs on, not
// by anything it remembers — so the table is worth pinning directly.
func TestBatchTxStatementsFollowTheBatcher(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		on            Batcher
		begin, commit string
	}{
		{"pool", &sqlDBBatcher{}, "BEGIN", "COMMIT"},
		{"held connection", sqlConnBatcher{}, "BEGIN", "COMMIT"},
		{"transaction", sqlTxBatcher{}, "SAVEPOINT " + BatchSavepoint, "RELEASE SAVEPOINT " + BatchSavepoint},
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
				b := newBatch(tc.on, nil)
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

// Nothing on the batch records the choice, so it is the same answer every time
// and cannot drift from what the batch is actually running on.
func TestBatchTxStatementsAreNotRemembered(t *testing.T) {
	t.Parallel()
	b := newBatch(sqlTxBatcher{}, nil)
	b.Begin()
	b.Commit()
	want := []string{"SAVEPOINT " + BatchSavepoint, "RELEASE SAVEPOINT " + BatchSavepoint}
	for i, w := range want {
		if b.qs[i].Query != w {
			t.Errorf("query %d = %q, want %q", i, b.qs[i].Query, w)
		}
	}
}

// Rollback runs rather than queueing, so it needs the connection the batch ran
// on. A batch on the pool has already given that back.
func TestBatchRollbackNeedsAHeldConnection(t *testing.T) {
	t.Parallel()
	b := newBatch(&sqlDBBatcher{}, nil)
	b.Begin()
	err := b.Rollback(context.Background())
	if err == nil {
		t.Fatal("no error rolling back a batch that holds no connection")
	}
	if !strings.Contains(err.Error(), "holds one") && !strings.Contains(err.Error(), "does not hold one") {
		t.Errorf("unhelpful error: %v", err)
	}
	// and it queued nothing: Begin is the only statement in the batch
	if len(b.qs) != 1 {
		t.Errorf("Rollback queued a statement: %v", b.qs)
	}
}
