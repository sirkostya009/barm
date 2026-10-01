package barm_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

type batchUser struct {
	barm.BaseModel `barm:"table:batch_users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at"`
}

// stubBatcher stands in for a driver, so the batch machinery can be tested
// without one.
type stubBatcher struct {
	sent []barm.BatchQuery
	fail error
}

func (s *stubBatcher) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	s.sent = qs
	if s.fail != nil {
		return s.fail
	}
	return read(stubReader{})
}

type stubReader struct{}

func (stubReader) Rows() (barm.Rows, error)       { return nil, errors.New("stub: no rows") }
func (stubReader) Exec() (barm.ExecResult, error) { return barm.ExecResult{}, nil }

// A result read before Run says it did not run, rather than looking like an
// empty answer.
func TestBatchResultBeforeRun(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	stub := &stubBatcher{}

	b := db.Batch(stub)
	users := b.Slice(db.Select[batchUser]())
	total := b.Count(db.Select[batchUser]())

	if b.Err() != nil {
		t.Fatalf("queueing failed: %v", b.Err())
	}
	for _, err := range []error{users.Err(), total.Err()} {
		if !errors.Is(err, barm.ErrNotRun) {
			t.Errorf("err = %v, want ErrNotRun", err)
		}
	}
	v, err := users.Get()
	if v != nil || !errors.Is(err, barm.ErrNotRun) {
		t.Errorf("Get() = %v, %v", v, err)
	}
	if len(stub.sent) != 0 {
		t.Errorf("nothing should have been sent yet: %v", stub.sent)
	}
}

// A failure part-way leaves the queries behind it not-run.
func TestBatchResultAfterAbort(t *testing.T) {
	t.Parallel()
	db := barm.NewBuilder(barm.Postgres)
	stub := &stubBatcher{}

	b := db.Batch(stub)
	first := b.Slice(db.Select[batchUser]()) // the stub fails every Rows call
	second := b.Exec(db.Insert[batchUser]().Values(&batchUser{Name: "x"}))

	err := b.Run(t.Context())
	if err == nil {
		t.Fatal("expected an error")
	}
	if first.Err() == nil || errors.Is(first.Err(), barm.ErrNotRun) {
		t.Errorf("first.Err() = %v, want the real failure", first.Err())
	}
	if !errors.Is(second.Err(), barm.ErrNotRun) {
		t.Errorf("second.Err() = %v, want ErrNotRun", second.Err())
	}
	if len(stub.sent) != 2 {
		t.Errorf("sent %d queries, want 2", len(stub.sent))
	}
}

func TestBatchWithoutDriver(t *testing.T) {
	t.Parallel()
	plain := barm.NewBuilder(barm.Postgres)
	b := plain.Batch()
	users := b.Slice(plain.Select[batchUser]())

	if !errors.Is(b.Err(), barm.ErrNoBatcher) {
		t.Errorf("Err() = %v, want ErrNoBatcher", b.Err())
	}
	if !errors.Is(users.Err(), barm.ErrNoBatcher) {
		t.Errorf("result err = %v, want ErrNoBatcher", users.Err())
	}
	err := b.Run(t.Context())
	if !errors.Is(err, barm.ErrNoBatcher) {
		t.Errorf("Run() = %v, want ErrNoBatcher", err)
	}
}

// A batch fires the same query hooks a plain query does: one event per queued
// query, all started together because that is how they are sent.
func TestBatchFiresQueryHooks(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var got []barm.QueryEvent
	db := barm.NewBuilder(barm.Postgres, barm.WithHook(barm.QueryHook{
		BeforeQuery: func(ctx context.Context, ev *barm.QueryEvent) context.Context {
			return context.WithValue(ctx, ctxKey{}, ev.StartedAt)
		},
		AfterQuery: func(ctx context.Context, ev *barm.QueryEvent) {
			if ctx.Value(ctxKey{}) == nil {
				t.Error("context from BeforeQuery did not reach AfterQuery")
			}
			mu.Lock()
			defer mu.Unlock()
			got = append(got, *ev)
		},
	}))

	b := db.Batch(&stubBatcher{})
	b.Count(db.Select[batchUser]())
	b.Exec(db.Insert[batchUser]().Values(&batchUser{Name: "a"}))

	err := b.Run(t.Context())
	if err == nil {
		t.Fatal("the stub reports no rows, so the batch should fail")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("events = %d, want one per queued query", len(got))
	}
	if got[0].Op != "SELECT" || got[1].Op != "INSERT" {
		t.Errorf("ops = %s, %s", got[0].Op, got[1].Op)
	}
	for i, ev := range got {
		if ev.Query == "" || ev.Duration <= 0 {
			t.Errorf("event %d = %+v, want a query and a duration", i, ev)
		}
	}
	// The first failed, so the second never ran and says so.
	if got[0].Err == nil || !errors.Is(got[1].Err, barm.ErrNotRun) {
		t.Errorf("errs = %v, %v; want a failure then ErrNotRun", got[0].Err, got[1].Err)
	}
}

type ctxKey struct{}
