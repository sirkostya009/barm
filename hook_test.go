package barm_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
