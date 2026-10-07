package barm_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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
	sent   []barm.BatchQuery
	fail   error
	reader barm.BatchReader // stubReader when nil
}

func (s *stubBatcher) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	s.sent = qs
	if s.fail != nil {
		return s.fail
	}
	if s.reader != nil {
		return read(s.reader)
	}
	return read(stubReader{})
}

// stubPool is a Pool whose batches go to the stub; nothing else is called.
type stubPool struct{ *stubBatcher }

func (stubPool) Query(context.Context, string, []any) (barm.Rows, error) { panic("unused") }
func (stubPool) Exec(context.Context, string, []any) (sql.Result, error) { panic("unused") }
func (stubPool) QueryPrepared(context.Context, string, string, []any) (barm.Rows, error) {
	panic("unused")
}
func (stubPool) ExecPrepared(context.Context, string, string, []any) (sql.Result, error) {
	panic("unused")
}
func (stubPool) Acquire(context.Context) (barm.DriverConn, error) { panic("unused") }
func (stubPool) Begin(context.Context, *sql.TxOptions) (barm.DriverTx, error) {
	panic("unused")
}
func (stubPool) Dialect() barm.Dialect      { return barm.Postgres }
func (stubPool) Ping(context.Context) error { return nil }
func (stubPool) Underlying() any            { return nil }
func (stubPool) Close() error               { return nil }

func stubbed(stub *stubBatcher, opts ...barm.Option) *barm.DB {
	return barm.New(stubPool{stub}, opts...)
}

type stubReader struct{}

func (stubReader) Rows() (barm.Rows, error)       { return nil, errors.New("stub: no rows") }
func (stubReader) Exec() (barm.ExecResult, error) { return barm.ExecResult{}, nil }

// A result read before Run says it did not run, rather than looking like an
// empty answer.
func TestBatchResultBeforeRun(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{}
	db := stubbed(stub)

	b := db.Batch()
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
	stub := &stubBatcher{}
	db := stubbed(stub)

	b := db.Batch()
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

// A builder-only DB has nothing to batch on, and says so when queueing.
func TestBatchWithoutConnection(t *testing.T) {
	t.Parallel()
	plain := barm.NewBuilder(barm.Postgres)
	b := plain.Batch()
	users := b.Slice(plain.Select[batchUser]())

	if !errors.Is(b.Err(), barm.ErrNoConn) {
		t.Errorf("Err() = %v, want ErrNoConn", b.Err())
	}
	if !errors.Is(users.Err(), barm.ErrNoConn) {
		t.Errorf("result err = %v, want ErrNoConn", users.Err())
	}
	err := b.Run(t.Context())
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("Run() = %v, want ErrNoConn", err)
	}
}

// database/sql cannot send statements together: its batch fails before
// sending anything, unless SequentialBatches asked for one at a time.
func TestSQLBatchNeedsSequential(t *testing.T) {
	t.Parallel()
	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1}), barm.Postgres))
	b := db.Batch()
	users := b.Slice(db.Select[User]())
	err := b.Run(t.Context())
	if !errors.Is(err, barm.ErrNoBatcher) || !errors.Is(users.Err(), barm.ErrNoBatcher) {
		t.Errorf("Run() = %v, result = %v, want ErrNoBatcher", err, users.Err())
	}

	seq := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1}), barm.Postgres, barm.SequentialBatches()))
	b = seq.Batch()
	users = b.Slice(seq.Select[User]())
	one := b.One(seq.Select[User]())
	err = b.Run(t.Context())
	if err != nil || len(users.Value()) != 1 || one.Value().Name != "name" {
		t.Errorf("sequential: %v, %v, %+v", err, users.Value(), one.Value())
	}
}

// A batch fires the same query hooks a plain query does: one event per queued
// query, all started together because that is how they are sent.
func TestBatchFiresQueryHooks(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var got []barm.QueryEvent
	db := stubbed(&stubBatcher{}, barm.WithHook(barm.QueryHook{
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

	b := db.Batch()
	b.Count(db.Select[batchUser]())
	b.Exec(db.Insert[batchUser]().Values(&batchUser{Name: "a"}))
	b.Slice(db.Select[batchUser]())

	err := b.Run(t.Context())
	if err == nil {
		t.Fatal("the stub reports no rows, so the batch should fail")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("events = %d, want one per queued query", len(got))
	}
	if got[0].Op != "SELECT" || got[1].Op != "INSERT" {
		t.Errorf("ops = %s, %s", got[0].Op, got[1].Op)
	}
	for i, ev := range got {
		if ev.Table != "batch_users" {
			t.Errorf("event %d: table = %q", i, ev.Table)
		}
	}
	// The first reached the database and failed; the rest never did, so they
	// report no times.
	if ev := got[0]; ev.StartedAt.IsZero() || ev.Duration <= 0 || ev.FirstResponse <= 0 || ev.FirstResponse > ev.Duration {
		t.Errorf("event 0 = %+v, want times", ev)
	}
	for i, ev := range got[1:] {
		if !ev.StartedAt.IsZero() || ev.Duration != 0 || ev.FirstResponse != 0 {
			t.Errorf("event %d = %+v, want no times", i+1, ev)
		}
	}
	for i, ev := range got {
		if ev.Query == "" {
			t.Errorf("event %d has no query", i)
		}
	}
	// The first failed, so the second never ran and says so.
	if got[0].Err == nil || !errors.Is(got[1].Err, barm.ErrNotRun) {
		t.Errorf("errs = %v, %v; want a failure then ErrNotRun", got[0].Err, got[1].Err)
	}
}

type ctxKey struct{}

// slowCountReader answers every query with one count row, which takes rowDelay
// to arrive after the result does.
type slowCountReader struct{}

func (slowCountReader) Rows() (barm.Rows, error)       { return &slowCount{}, nil }
func (slowCountReader) Exec() (barm.ExecResult, error) { return barm.ExecResult{}, nil }

type slowCount struct{ read bool }

func (*slowCount) Columns() ([]string, error) { return []string{"count"}, nil }
func (*slowCount) Err() error                 { return nil }
func (*slowCount) Close() error               { return nil }

func (c *slowCount) Next() bool {
	time.Sleep(rowDelay)
	ok := !c.read
	c.read = true
	return ok
}

func (*slowCount) Scan(dest ...any) error {
	*dest[0].(*int64) = 1 //nolint:forcetypeassert // a count scans into an int64
	return nil
}

// Each query in a batch responds when its result is reached, and finishes once
// its rows are read.
func TestBatchFirstResponse(t *testing.T) {
	t.Parallel()
	var got []barm.QueryEvent
	db := stubbed(&stubBatcher{reader: slowCountReader{}}, barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	}))
	b := db.Batch()
	b.Count(db.Select[batchUser]())
	b.Count(db.Select[batchUser]())
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2", len(got))
	}
	for i, ev := range got {
		if ev.FirstResponse <= 0 || ev.Duration-ev.FirstResponse < rowDelay || ev.Rows != 1 {
			t.Errorf("event %d: first response %s, duration %s, rows %d", i, ev.FirstResponse, ev.Duration, ev.Rows)
		}
	}
	// The second starts when barm turns to it, once the first is read.
	if end := got[0].StartedAt.Add(got[0].Duration); got[1].StartedAt.Before(end) {
		t.Errorf("second started %s before the first ended", end.Sub(got[1].StartedAt))
	}
}

type batchUserName struct {
	Name string `barm:"name"`
}

type newBatchUser struct {
	Name  string `barm:"name"`
	Email string `barm:"email"`
}

type batchAuthor struct {
	barm.BaseModel `barm:"table:authors"`

	ID    int64       `barm:"id,pk"`
	Books []batchBook `barm:"rel:id=author_id"`
}

type batchBook struct {
	barm.BaseModel `barm:"table:books"`

	AuthorID int64 `barm:"author_id"`
}

// The As calls queue a query rendered for the type its rows are read into, as
// the query's own As terminals render it.
func TestBatchAsRendersForTheResult(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{}
	db := stubbed(stub)
	ins := db.Insert[newBatchUser]().Table("batch_users").Values(&newBatchUser{Name: "a", Email: "a@x"})
	sel := db.Select[batchUser]().Where("age > ?", 1).Limit(10)
	upd := db.Update[batchUser]().Set("age = age + 1").Where("age > ?", 1)
	del := db.Delete[batchUser]().Where("age > ?", 1)
	raw := db.NewRaw("SELECT max(age) FROM batch_users WHERE age > ?", 1)

	b := db.Batch()
	b.OneAs[batchUser](ins)
	b.OneAs[batchUserName](sel)
	b.SliceAs[batchUserName](sel)
	b.SliceAs[batchUser](upd)
	b.SliceAs[batchUser](del)
	b.OneAs[int64](raw)
	_ = b.Run(t.Context())

	for i, build := range []func() (string, []any, error){
		func() (string, []any, error) { return ins.BuildAs[batchUser]() },
		func() (string, []any, error) { return sel.Clone().Limit(1).BuildAs[batchUserName]() },
		func() (string, []any, error) { return sel.BuildAs[batchUserName]() },
		func() (string, []any, error) { return upd.BuildAs[batchUser]() },
		func() (string, []any, error) { return del.BuildAs[batchUser]() },
		raw.Build,
	} {
		query, args, err := build()
		if err != nil {
			t.Fatal(err)
		}
		if i >= len(stub.sent) {
			t.Fatalf("sent %d queries, want %d", len(stub.sent), i+1)
		}
		if got := stub.sent[i]; got.Query != query || len(got.Args) != len(args) {
			t.Errorf("query %d = %q %v, want %q %v", i, got.Query, got.Args, query, args)
		}
	}
	if q, _, _ := sel.Build(); !strings.Contains(q, "LIMIT 10") {
		t.Errorf("queueing changed the query it was given: %q", q)
	}
}

// A batch loads relations through One and Slice, so the As calls refuse a
// select that asks for them rather than leave them unloaded.
func TestBatchAsRefusesRelations(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{})
	b := db.Batch()
	r := b.SliceAs[batchAuthor](db.Select[batchAuthor]().Relation[batchBook]("Books"))
	err := b.Err()
	if err == nil || !strings.Contains(err.Error(), "relations") {
		t.Errorf("queue error = %v, want one about relations", err)
	}
	if r.Err() == nil {
		t.Error("the result carries no error")
	}
}
