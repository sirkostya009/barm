package barm_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/sirkostya009/barm"
)

// slowConnector answers at once and takes rowDelay over every row, so the
// first response and the end of the reading are far apart.
type slowConnector struct{ rows int }

const rowDelay = 20 * time.Millisecond

func (c slowConnector) Connect(context.Context) (driver.Conn, error) {
	return slowConn{fakeConn(c)}, nil
}
func (slowConnector) Driver() driver.Driver { return nil }

type slowConn struct{ fakeConn }

func (c slowConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return slowRows{&fakeDriverRows{n: c.rows}}, nil
}

type slowRows struct{ *fakeDriverRows }

func (r slowRows) Next(dest []driver.Value) error {
	time.Sleep(rowDelay)
	return r.fakeDriverRows.Next(dest)
}

// FirstResponse is when the database started answering, Duration when the rows
// were read and closed.
func TestHookFirstResponse(t *testing.T) {
	t.Parallel()
	var got []barm.QueryEvent
	db := barm.New(barm.SQL(sql.OpenDB(slowConnector{rows: 2}), barm.Postgres), barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	}))
	ctx := t.Context()

	_, err := db.Select[User]().Slice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Select[User]().One(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Select[User]().Rows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	_ = rows.Close()
	for _, err := range db.Select[User]().Seq(ctx) {
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(got) != 4 {
		t.Fatalf("events = %d, want 4", len(got))
	}
	for i, ev := range got {
		if ev.FirstResponse <= 0 || ev.Duration-ev.FirstResponse < rowDelay {
			t.Errorf("event %d: first response %s, duration %s, want the rows read in between", i, ev.FirstResponse, ev.Duration)
		}
	}
}

// A call that never reached the database still fires its hooks, with no times:
// there was no round trip to measure.
func TestHookUnsentHasNoTimes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var got []barm.QueryEvent
	hook := barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	})
	unsent := func(what string, want error) {
		t.Helper()
		if len(got) != 1 {
			t.Fatalf("%s: events = %d, want 1", what, len(got))
		}
		ev := got[0]
		got = nil
		if !errors.Is(ev.Err, want) || !ev.StartedAt.IsZero() || ev.FirstResponse != 0 || ev.Duration != 0 {
			t.Errorf("%s: err %v, started %s, first response %s, duration %s; want %v and no times",
				what, ev.Err, ev.StartedAt, ev.FirstResponse, ev.Duration, want)
		}
	}

	nothing := barm.NewBuilder(barm.Postgres, hook)
	_, err := nothing.Select[User]().Slice(ctx)
	unsent("no connection", err)
	_, err = nothing.BeginTx(ctx, nil)
	unsent("BEGIN with no connection", err)

	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1}), barm.Postgres), hook)
	_, _ = db.Select[User]().Prepare("taken").Slice(ctx) // binds the name; the fake cannot prepare, which is beside the point
	got = nil
	_, err = db.Select[User]().Where("id = 1").Prepare("taken").Slice(ctx)
	unsent("name bound to other SQL", err)

	cannot := stubbed(&stubBatcher{fail: barm.ErrNoBatcher}, hook)
	b := cannot.Batch()
	b.Count(cannot.Select[batchUser]())
	unsent("driver that cannot batch", b.Run(ctx))

	stub := stubbed(&stubBatcher{}, hook)
	b = stub.Batch()
	b.Count(stub.Select[batchUser]())
	b.Slice(stub.Select[batchUser]().Prepare("n"))
	b.Slice(stub.Select[batchUser]().Where("id = 1").Prepare("n"))
	if b.Run(ctx) == nil {
		t.Fatal("the name clash should fail the batch")
	}
	if len(got) != 2 {
		t.Fatalf("queued before the failure: events = %d, want 2", len(got))
	}
	for i, ev := range got {
		if !errors.Is(ev.Err, barm.ErrNotRun) || !ev.StartedAt.IsZero() || ev.Duration != 0 {
			t.Errorf("event %d: err %v, started %s, duration %s; want ErrNotRun and no times", i, ev.Err, ev.StartedAt, ev.Duration)
		}
	}
}

// reportingPool reports the same stats for every exec and batch barm hands it
// a watched ctx for, and describes every statement of a batch.
type reportingPool struct {
	stubPool
	stats barm.CallStats
	saw   []bool // whether each call came with somewhere to report
}

func (p *reportingPool) report(ctx context.Context) {
	s := barm.CallStatsFrom(ctx)
	p.saw = append(p.saw, s != nil)
	if s != nil {
		*s = p.stats
	}
}

func (p *reportingPool) Exec(ctx context.Context, _ string, _ []any) (sql.Result, error) {
	p.report(ctx)
	return driver.RowsAffected(0), nil
}

func (p *reportingPool) SendBatch(ctx context.Context, _ []barm.BatchQuery, read func(barm.BatchReader) error) error {
	p.report(ctx)
	return read(describingReader{})
}

type describingReader struct{ stubReader }

func (describingReader) Rows() (barm.Rows, error) { return &slowCount{}, nil }
func (describingReader) Described() bool          { return true }

// What a driver reports on a call reaches the event with WithCallStats, and
// without it, or with no hook watching, the driver has nowhere to report.
func TestHookCallStats(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	stats := barm.CallStats{Wait: time.Millisecond, Described: true, PID: 42}
	var got []barm.QueryEvent
	p := &reportingPool{stats: stats}
	hook := barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	})
	db := barm.New(p, hook, barm.WithCallStats())
	_, err := db.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CallStats != stats {
		t.Fatalf("exec: stats %+v, want %+v", got[0].CallStats, stats)
	}

	got = nil
	b := db.Batch()
	b.Count(db.Select[batchUser]())
	b.Count(db.Select[batchUser]())
	err = b.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The wait was for the batch's connection, ahead of all of it.
	want := []barm.CallStats{stats, {Described: true, PID: 42}}
	if len(got) != 2 || got[0].CallStats != want[0] || got[1].CallStats != want[1] {
		t.Errorf("batch: stats %+v, want %+v", got, want)
	}

	for what, opts := range map[string][]barm.Option{
		"no hook":          {barm.WithCallStats()},
		"no WithCallStats": {hook},
	} {
		got = nil
		unwatched := &reportingPool{stats: stats}
		db := barm.New(unwatched, opts...)
		_, err = db.Exec(ctx, "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		b := db.Batch()
		b.Count(db.Select[batchUser]())
		err = b.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(unwatched.saw, true) {
			t.Errorf("%s: the driver was given somewhere to report", what)
		}
		for _, ev := range got {
			if ev.CallStats != (barm.CallStats{}) {
				t.Errorf("%s: %s reported %+v", what, ev.Query, ev.CallStats)
			}
		}
	}
	if !slices.Equal(p.saw, []bool{true, true}) {
		t.Errorf("watched calls had somewhere to report: %v", p.saw)
	}
}

// Caller is the line that made the call, however deep in barm its event
// starts, and for a batch's query the line that queued it.
func TestHookCaller(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var got []barm.QueryEvent
	hook := barm.WithHook(barm.QueryHook{
		AfterQuery: func(_ context.Context, ev *barm.QueryEvent) { got = append(got, *ev) },
	})
	next := func() int { _, _, l, _ := runtime.Caller(1); return l + 1 }
	want := make([]int, 0, 6)

	db := barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 2}), barm.Postgres), hook, barm.WithCaller())
	want = append(want, next())
	_, _ = db.Select[User]().Slice(ctx)
	want = append(want, next())
	for range db.Select[User]().Seq(ctx) {
	}
	want = append(want, next())
	_, _ = db.BeginTx(ctx, nil) // the fake cannot begin, but the BEGIN was sent
	want = append(want, next())
	_, _ = barm.NewBuilder(barm.Postgres, hook, barm.WithCaller()).Select[User]().Slice(ctx)

	stub := stubbed(&stubBatcher{}, hook, barm.WithCaller())
	b := stub.Batch()
	want = append(want, next())
	b.Exec(stub.NewRaw("SELECT 1"))
	want = append(want, next())
	b.Exec(stub.NewRaw("SELECT 2"))
	_ = b.Run(ctx)

	if len(got) != len(want) {
		t.Fatalf("events = %d, want %d", len(got), len(want))
	}
	self := "github.com/sirkostya009/barm_test.TestHookCaller"
	for i, ev := range got {
		if ev.Caller == nil || ev.Caller.Function != self || ev.Caller.Line != want[i] {
			t.Errorf("%s: caller %+v, want %s:%d", ev.Query, ev.Caller, self, want[i])
		}
	}

	got = nil
	_, _ = barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1}), barm.Postgres), hook).Select[User]().Slice(ctx)
	if len(got) != 1 || got[0].Caller != nil {
		t.Errorf("without WithCaller: %+v", got)
	}
}
