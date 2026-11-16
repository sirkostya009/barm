package barm_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
		if ev.BatchSize != 3 || int(ev.BatchIndex) != i {
			t.Errorf("event %d: batch %d of %d, want %d of 3", i, ev.BatchIndex, ev.BatchSize, i)
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

// emptyReader answers every query with no rows, the rows ending in err.
type emptyReader struct{ err error }

func (r emptyReader) Rows() (barm.Rows, error)     { return emptyRows(r), nil }
func (emptyReader) Exec() (barm.ExecResult, error) { return barm.ExecResult{RowsAffected: 1}, nil }

type emptyRows struct{ err error }

func (emptyRows) Columns() ([]string, error) { return []string{"id"}, nil }
func (emptyRows) Next() bool                 { return false }
func (emptyRows) Scan(...any) error          { return nil }
func (r emptyRows) Err() error               { return r.err }
func (r emptyRows) Close() error             { return r.err }

// A result that arrived whole but holds no row fails its own query, and the
// ones behind it ran all the same.
func TestBatchNoRowsFailsOnlyItsQuery(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	b := db.Batch()
	missing := b.One(db.Select[batchUser]())
	written := b.Exec(db.Insert[batchUser]().Values(&batchUser{Name: "x"}))

	err := b.Run(t.Context())
	if err != nil {
		t.Fatalf("Run = %v, want nil: every statement ran", err)
	}
	if !errors.Is(missing.Err(), barm.ErrNoRows) {
		t.Errorf("missing.Err() = %v, want ErrNoRows", missing.Err())
	}
	if written.Err() != nil || written.Value().RowsAffected != 1 {
		t.Errorf("written = %v, %v; want it run", written.Value(), written.Err())
	}
}

// An error the rows report as they end is the database's, which skipped what
// followed, so it still ends the batch.
func TestBatchRowsErrorEndsIt(t *testing.T) {
	t.Parallel()
	failed := errors.New("division by zero")
	db := stubbed(&stubBatcher{reader: emptyReader{err: failed}})
	b := db.Batch()
	broken := b.Slice(db.Select[batchUser]())
	after := b.Exec(db.Insert[batchUser]().Values(&batchUser{Name: "x"}))

	err := b.Run(t.Context())
	if !errors.Is(err, failed) {
		t.Fatalf("Run = %v, want %v", err, failed)
	}
	if !errors.Is(broken.Err(), failed) {
		t.Errorf("broken.Err() = %v, want %v", broken.Err(), failed)
	}
	if !errors.Is(after.Err(), barm.ErrNotRun) {
		t.Errorf("after.Err() = %v, want ErrNotRun", after.Err())
	}
}

type batchPost struct {
	barm.BaseModel `barm:"table:batch_posts"`

	AuthorID int64  `barm:"author_id"`
	Title    string `barm:"title"`
	Body     string `barm:"body"`
}

type batchPostRow struct {
	barm.BaseModel `barm:"table:batch_posts"`

	ID       int64  `barm:"id,pk"`
	AuthorID int64  `barm:"author_id"`
	Title    string `barm:"title"`
}

func insertIfAuthor(db *barm.DB, author, title, body any) *barm.InsertQuery[batchPost] {
	return db.Insert[batchPost]().Select(db.Select[batchUser]().
		ColumnExpr("u.id, ?, ?", title, body).
		Where("u.id = ?", author).
		For("KEY SHARE"))
}

type postArgs struct {
	Author int64  `barm:"author"`
	Title  string `barm:"title"`
	Body   string `barm:"body"`
}

func postTemplate(db *barm.DB) *barm.Template[postArgs, batchPostRow] {
	return insertIfAuthor(db, barm.Param("author"), barm.Param("title"), barm.Param("body")).TemplateAs[postArgs, batchPostRow]()
}

// A template runs as the query it was built from would, its Params bound from
// the fields of P.
func TestTemplateQueuesAsBuilt(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	ins := postTemplate(db)

	b := db.Batch()
	b.OneAs[batchPostRow](insertIfAuthor(db, int64(7), "t", "b"))
	ins.In(b).One(postArgs{Author: 7, Title: "t", Body: "b"})
	ins.In(b).Slice(postArgs{Author: 8, Title: "u", Body: "c"})
	ins.In(b).Exec(postArgs{Author: 9, Title: "v", Body: "d"})
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	built := stub.sent[0]
	for i, want := range [][]any{built.Args, {"u", "c", int64(8)}, {"v", "d", int64(9)}} {
		got := stub.sent[i+1]
		if got.Query != built.Query {
			t.Errorf("template renders\n%s\nwant\n%s", got.Query, built.Query)
		}
		if !reflect.DeepEqual(got.Args, want) {
			t.Errorf("args = %v, want %v", got.Args, want)
		}
	}
}

// Binding a template costs what writing its argument list out by hand does: P
// stays where it was, and nothing else is made.
//
//nolint:paralleltest // AllocsPerRun refuses a parallel test
func TestTemplateBindCost(t *testing.T) {
	ins := postTemplate(barm.NewBuilder(barm.Postgres))
	p := postArgs{Author: 7000, Title: strings.Repeat("t", 3), Body: strings.Repeat("b", 3)}
	var sink []any
	floor := testing.AllocsPerRun(100, func() { sink = []any{p.Title, p.Body, p.Author} })
	n := testing.AllocsPerRun(100, func() { _, sink, _ = ins.Build(p) })
	if n != floor {
		t.Errorf("binding takes %v allocs, writing the arguments out %v", n, floor)
	}
	_ = sink
}

type ageIn struct {
	Schema string `barm:"schema"`
	Age    int    `barm:"age"`
}

func schemaTemplate(db *barm.DB) *barm.Template[ageIn, batchUser] {
	return db.Select[batchUser]().Table("?.batch_users AS u", barm.IdentSlot("schema")).
		Where("u.age > ?", barm.Param("age")).Template[ageIn]()
}

// An IdentSlot renders the identifier P's field gives it, quoted, binding
// nothing.
func TestTemplateIdentSlot(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	tp := schemaTemplate(db)

	b := db.Batch()
	for _, schema := range []string{"t1", `t"2`, "t1"} {
		tp.In(b).Slice(ageIn{Schema: schema, Age: 30})
	}
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	one, two, again := stub.sent[0], stub.sent[1], stub.sent[2]
	if want := `FROM "t1".batch_users AS u WHERE u.age > $1`; !strings.Contains(one.Query, want) {
		t.Errorf("t1: %s\nwant it to read %s", one.Query, want)
	}
	if want := `FROM "t""2".batch_users AS u`; !strings.Contains(two.Query, want) {
		t.Errorf(`t"2: %s`, two.Query)
	}
	if !reflect.DeepEqual(one.Args, []any{30}) {
		t.Errorf("args = %v", one.Args)
	}
	if again.Query != one.Query {
		t.Errorf("t1 again: %s", again.Query)
	}
}

// A batch queueing one template for one tenant splices its SQL once: every
// query after the first costs what one from a template with no slot does.
//
//nolint:paralleltest // AllocsPerRun refuses a parallel test
func TestTemplateIdentSlotBatched(t *testing.T) {
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	slotted := schemaTemplate(db)
	plain := db.Select[batchUser]().Table("t1.batch_users AS u").Where("u.age > ?", barm.Param("age")).Template[ageIn]()
	schema := strings.Repeat("t", 2)
	queue := func(tp *barm.Template[ageIn, batchUser]) float64 {
		b := db.Batch()
		b.Grow(256)
		return testing.AllocsPerRun(100, func() { tp.In(b).Slice(ageIn{Schema: schema, Age: 300}) })
	}
	if got, want := queue(slotted), queue(plain); got != want {
		t.Errorf("queueing a slotted template takes %v allocs, a plain one %v", got, want)
	}

	stub := &stubBatcher{reader: emptyReader{}}
	db = stubbed(stub)
	slotted = schemaTemplate(db)
	b := db.Batch()
	for _, schema := range []string{"t1", "t1", "t2", "t1"} {
		slotted.In(b).Slice(ageIn{Schema: schema, Age: 1})
	}
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i, schema := range []string{"t1", "t1", "t2", "t1"} {
		if want := `FROM "` + schema + `".batch_users`; !strings.Contains(stub.sent[i].Query, want) {
			t.Errorf("query %d = %s, want it to read %s", i, stub.sent[i].Query, want)
		}
	}
}

// A Prepare name is used as given: one name over two schemas binds it to two
// SQL texts, which fails, and Prepare gives each schema a name of its own
// without touching the template it was called on.
func TestTemplateIdentSlotPrepare(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	tp := schemaTemplate(db)

	over := tp.Prepare("over")
	b := db.Batch()
	over.In(b).Slice(ageIn{Schema: "t1", Age: 30})
	if r := over.In(b).Slice(ageIn{Schema: "t2", Age: 30}); r.Err() == nil {
		t.Error("one name took two schemas")
	}

	b = db.Batch()
	for _, schema := range []string{"t3", "t4"} {
		tp.Prepare("over_" + schema).In(b).Slice(ageIn{Schema: schema, Age: 30})
	}
	tp.In(b).Slice(ageIn{Schema: "t3", Age: 30})
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{stub.sent[0].Name, stub.sent[1].Name, stub.sent[2].Name}; !slices.Equal(got, []string{"over_t3", "over_t4", ""}) {
		t.Errorf("names = %q", got)
	}

	ins := db.Insert[batchUser]().Template[batchUser]().Prepare("ins")
	b = db.Batch()
	ins.In(b).Exec(batchUser{Name: "a"})
	err = b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stub.sent[0].Name != "ins" {
		t.Errorf("insert template name = %q", stub.sent[0].Name)
	}
}

// Splicing an IdentSlot costs the SQL it writes, and nothing else.
//
//nolint:paralleltest // AllocsPerRun refuses a parallel test
func TestTemplateIdentSlotCost(t *testing.T) {
	tp := schemaTemplate(barm.NewBuilder(barm.Postgres))
	n := testing.AllocsPerRun(100, func() {
		_, _, _ = tp.Build(ageIn{Schema: "t1", Age: 30})
	})
	if n > 2 {
		t.Errorf("binding takes %v allocs", n)
	}
}

// An IdentSlot is filled by a template alone, from a field of P holding a
// name.
func TestTemplateIdentSlotMisuse(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	type numbered struct {
		Schema int `barm:"schema"`
	}
	type both struct {
		S string `barm:"s"`
	}
	for _, tc := range []struct {
		what  string
		build func() error
	}{
		{"no template", func() error {
			_, _, err := db.Select[batchUser]().Table("?.batch_users", barm.IdentSlot("schema")).Build()
			return err
		}},
		{"no field", func() error {
			_, _, err := db.Select[batchUser]().Table("?.batch_users", barm.IdentSlot("other")).Template[ageIn]().Build(ageIn{})
			return err
		}},
		{"not a string", func() error {
			_, _, err := db.Select[batchUser]().Table("?.batch_users", barm.IdentSlot("schema")).Template[numbered]().Build(numbered{Schema: 1})
			return err
		}},
		{"empty", func() error {
			_, _, err := schemaTemplate(db).Build(ageIn{Age: 1})
			return err
		}},
		{"unnamed", func() error {
			_, _, err := db.Select[batchUser]().Table("?.batch_users", barm.IdentSlot("")).Template[ageIn]().Build(ageIn{})
			return err
		}},
		{"both kinds", func() error {
			_, _, err := db.Select[batchUser]().Table("?.batch_users", barm.IdentSlot("s")).
				Where("name = ?", barm.Param("s")).Template[both]().Build(both{S: "x"})
			return err
		}},
	} {
		if tc.build() == nil {
			t.Errorf("%s: built", tc.what)
		}
	}
}

type tagged string

type Inner struct {
	In string `barm:"in"`
}

type Behind struct {
	Far int64 `barm:"far"`
}

type fieldsIn struct {
	S     string         `barm:"s"`
	I64   int64          `barm:"i64"`
	I     int            `barm:"i"`
	I32   int32          `barm:"i32"`
	B     bool           `barm:"b"`
	F     float64        `barm:"f"`
	T     time.Time      `barm:"t"`
	Raw   []byte         `barm:"raw"`
	Named tagged         `barm:"named"`
	Zero  string         `barm:"zero,nullzero"`
	Doc   map[string]any `barm:"doc,json"`
	U8    uint8          `barm:"u8"`
	Table tagged         `barm:"table"`
	Inner
	*Behind
}

// Every field binds as its own type, whether read through its offset or
// through reflect, the same name taking its value wherever it stands.
func TestTemplateFields(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	names := []string{"s", "i64", "i", "i32", "b", "f", "t", "raw", "named", "zero", "doc", "u8", "far", "s"}
	params := make([]any, 0, len(names)+2)
	for _, n := range names[:len(names)-1] {
		params = append(params, barm.Param(n))
	}
	params = append(params, barm.IdentSlot("table"), barm.IdentSlot("in"), barm.Param("s"))
	tp := db.NewRaw("SELECT "+strings.Repeat("?, ", len(names)-2)+"? FROM ?.? WHERE x = ?", params...).Template[fieldsIn]()

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	p := fieldsIn{S: "s", I64: 1 << 40, I: 2, I32: 3, B: true, F: 1.5, T: at, Raw: []byte("r"), Named: "n",
		Doc: map[string]any{"k": "v"}, U8: 8, Table: "tbl", In: "sch", Behind: &Behind{Far: 9}}
	query, args, err := tp.Build(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"s", int64(1 << 40), 2, int32(3), true, 1.5, at, []byte("r"), tagged("n"), nil, `{"k":"v"}`, uint8(8), int64(9), "s"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("args =\n%#v\nwant\n%#v", args, want)
	}
	if !strings.HasSuffix(query, ` FROM "tbl"."sch" WHERE x = $14`) {
		t.Errorf("query = %s", query)
	}
	p.Behind = nil
	_, args, err = tp.Build(p)
	if err != nil || args[12] != int64(0) {
		t.Errorf("behind a nil pointer: %v %v, want the zero", args[12], err)
	}
}

// NewBatchUser is newBatchUser exported, for a P to embed.
type NewBatchUser = newBatchUser

type insIn struct {
	Schema string `barm:"schema"`
	NewBatchUser
}

type writeIn struct {
	Schema string `barm:"schema"`
	Table  string `barm:"table"`
	Age    int    `barm:"age"`
	ID     int64  `barm:"id"`
}

// Schema, and a write's Table, take an IdentSlot as they take a name, quoted
// as P's field fills it.
func TestSchemaIdentSlot(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	ins := db.Insert[newBatchUser]().Table("batch_users").Schema(barm.IdentSlot("schema")).TemplateAs[insIn, batchUser]()
	upd := db.Update[batchUser]().Schema(barm.IdentSlot("schema")).Set("age = ?", barm.Param("age")).
		Where("id = ?", barm.Param("id")).Template[writeIn]()
	del := db.Delete[batchUser]().Schema(barm.IdentSlot("schema")).Where("id = ?", barm.Param("id")).Template[writeIn]()
	tbl := db.Delete[batchUser]().Schema("fixed").Table(barm.IdentSlot("table")).Where("id = 1").Template[writeIn]()
	sel := db.Select[batchUser]().Schema(barm.IdentSlot("schema")).Where("u.age > ?", barm.Param("age")).Template[writeIn]()
	for _, tc := range []struct {
		build func() (string, []any, error)
		want  string
	}{
		{func() (string, []any, error) {
			return ins.Build(insIn{Schema: `t"1`, Name: "ann"})
		}, `INSERT INTO "t""1"."batch_users" (`},
		{func() (string, []any, error) { return upd.Build(writeIn{Schema: "t3", Age: 1, ID: 2}) }, `UPDATE "t3"."batch_users" SET`},
		{func() (string, []any, error) { return del.Build(writeIn{Schema: "t4", ID: 2}) }, `DELETE FROM "t4"."batch_users" WHERE`},
		{func() (string, []any, error) { return tbl.Build(writeIn{Table: "archive"}) }, `DELETE FROM "fixed"."archive" WHERE`},
		{func() (string, []any, error) { return sel.Build(writeIn{Schema: "t5", Age: 1}) }, `SELECT "u"."id", `},
	} {
		query, _, err := tc.build()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(query, tc.want) {
			t.Errorf("%s\nwant it to start %s", query, tc.want)
		}
	}
	query, _, err := sel.Build(writeIn{Schema: "t5", Age: 1})
	if want := `FROM "t5"."batch_users" AS "u" WHERE`; err != nil || !strings.Contains(query, want) {
		t.Errorf("select: %s %v\nwant it to read %s", query, err, want)
	}
	_, args, err := ins.Build(insIn{Schema: "t1", Name: "ann", Email: "a@x.io"})
	if err != nil || !reflect.DeepEqual(args, []any{"ann", "a@x.io"}) {
		t.Errorf("insert args = %v, %v", args, err)
	}
	_, _, err = db.Insert[newBatchUser]().Table("batch_users").Schema(barm.IdentSlot("schema")).Values(&newBatchUser{}).Build()
	if err == nil {
		t.Error("an IdentSlot built outside a template")
	}
}

// Schema keeps a name as it is, boxing nothing for taking an IdentSlot too.
func TestSchemaAllocs(t *testing.T) { //nolint:paralleltest // AllocsPerRun refuses parallel tests
	q, name := barm.NewBuilder(barm.Postgres).Delete[batchUser](), strings.Repeat("s", 3)
	if n := testing.AllocsPerRun(100, func() { q.Schema(name) }); n != 0 {
		t.Errorf("Schema allocates %v times", n)
	}
}

type ageEmail struct {
	Age   int    `barm:"age"`
	Email string `barm:"email"`
}

// A name bound twice takes the value in both places, around arguments the
// query fixed.
func TestTemplateNames(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	sel := db.Select[batchUser]().
		Where("age > ? AND name = ? AND email = ?", barm.Param("age"), "fixed", barm.Param("email")).
		Where("id <> ?", barm.Param("age")).Template[ageEmail]()

	b := db.Batch()
	sel.In(b).Slice(ageEmail{Age: 30, Email: "e"})
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := stub.sent[0].Args; !reflect.DeepEqual(got, []any{30, "fixed", "e", 30}) {
		t.Errorf("args = %v", got)
	}
}

// TemplateAs renders the query in its one shape up front, and whatever reads
// it later reads that SQL as it is.
func TestTemplateAsShape(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	names := db.Select[batchUser]().Where("age > ?", barm.Param("age")).TemplateAs[ageIn, batchUserName]()
	want, _, err := db.Select[batchUser]().Where("age > ?", 1).BuildAs[batchUserName]()
	if err != nil {
		t.Fatal(err)
	}

	b := db.Batch()
	names.In(b).One(ageIn{Age: 1})
	names.In(b).Slice(ageIn{Age: 2})
	err = b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i, q := range stub.sent {
		if q.Query != want {
			t.Errorf("query %d = %s, want %s", i, q.Query, want)
		}
	}
}

// BatchUser is batchUser exported, for a P to embed.
type BatchUser = batchUser

type renameIn struct {
	BatchUser
	Rename string `barm:"rename"`
}

// An insert template writes P as its row, as the builder writes one, and binds
// its Params around it.
func TestInsertTemplateRow(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	u := batchUser{Name: "ann", Email: "a@x.io", Age: 30, CreatedAt: at}
	ins := db.Insert[batchUser]().TemplateAs[batchUser, batchUser]()
	upsert := db.Insert[batchUser]().On("CONFLICT (email) DO UPDATE").Set("name = ?", barm.Param("rename")).Template[renameIn]()

	wantSQL, wantArgs, err := db.Insert[batchUser]().Values(&u).BuildAs[batchUser]()
	if err != nil {
		t.Fatal(err)
	}
	b := db.Batch()
	ins.In(b).One(u)
	upsert.In(b).Exec(renameIn{BatchUser: u, Rename: "bo"})
	err = b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := stub.sent[0]; got.Query != wantSQL || !reflect.DeepEqual(got.Args, wantArgs) {
		t.Errorf("template: %s %v\nbuilder: %s %v", got.Query, got.Args, wantSQL, wantArgs)
	}
	if got := stub.sent[1].Args; !reflect.DeepEqual(got, []any{"ann", "a@x.io", 30, at, "bo"}) {
		t.Errorf("upsert args = %v", got)
	}

	_, _, err = db.Insert[batchUser]().Template[ageIn]().Build(ageIn{})
	if err == nil {
		t.Error("a P with no field for a column built")
	}
}

// nativeStubPool is a stubPool whose driver takes JSON as it is.
type nativeStubPool struct{ stubPool }

func (nativeStubPool) NativeJSON() bool { return true }

type batchDoc struct {
	barm.BaseModel `barm:"table:docs"`

	Meta map[string]any `barm:"meta,json"`
}

// A template's row encodes for where it runs, not where it was built: a
// builder-only DB encodes JSON, and a pool taking it as it is gets the map.
func TestInsertTemplateEncodesForItsSession(t *testing.T) {
	t.Parallel()
	stub := &stubBatcher{reader: emptyReader{}}
	db := barm.New(nativeStubPool{stubPool{stub}})
	ins := barm.NewBuilder(barm.Postgres).Insert[batchDoc]().Template[batchDoc]()
	doc := batchDoc{Meta: map[string]any{"k": "v"}}

	_, built, err := ins.Build(doc)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := built[0].(string); !ok {
		t.Errorf("on the builder: %T, want encoded JSON", built[0])
	}
	b := db.Batch()
	ins.In(b).Exec(doc)
	err = b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stub.sent[0].Args[0].(map[string]any); !ok {
		t.Errorf("in the batch: %T, want the map", stub.sent[0].Args[0])
	}
}

// One template binds and queues from many goroutines at once, each getting
// its own arguments.
func TestTemplateConcurrent(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	ins := postTemplate(db)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 100 {
				p := postArgs{Author: int64(g*1000 + i), Title: "t", Body: "b"}
				b := db.Batch()
				ins.In(b).One(p)
				ins.In(b).Slice(p)
				ins.In(b).Exec(p)
				_, args, err := ins.Build(p)
				if err != nil || b.Err() != nil || args[2] != p.Author {
					t.Errorf("goroutine %d: args %v, err %v, %v", g, args, err, b.Err())
					return
				}
			}
		})
	}
	wg.Wait()
}

// A Param P has no field for fails the template, and so does a Param anywhere
// but a template.
func TestTemplateMisuse(t *testing.T) {
	t.Parallel()
	db := stubbed(&stubBatcher{reader: emptyReader{}})
	for _, tc := range []struct {
		what string
		tp   *barm.Template[ageIn, batchUser]
	}{
		{"a name P has no field for", db.Select[batchUser]().Where("name = ?", barm.Param("name")).Template[ageIn]()},
		{"an unnamed Param", db.Select[batchUser]().Where("age > ?", barm.Param("")).Template[ageIn]()},
	} {
		r := tc.tp.In(db.Batch()).Slice(ageIn{})
		if r.Err() == nil || errors.Is(r.Err(), barm.ErrNotRun) {
			t.Errorf("%s: %v", tc.what, r.Err())
		}
	}
	_, _, err := db.Select[batchUser]().Where("age > ?", barm.Param("age")).Template[int]().Build(1)
	if err == nil {
		t.Error("a P that is no struct built")
	}
	_, _, err = db.Select[batchUser]().Where("age > ?", barm.Param("age")).Build()
	if !strings.Contains(fmt.Sprint(err), "Template") {
		t.Errorf("a Param outside a template built: %v", err)
	}
}

func BenchmarkBatchQueueBuilt(b *testing.B) {
	db := stubbed(&stubBatcher{})
	b.ReportAllocs()
	bt := db.Batch()
	for i := 0; b.Loop(); i++ {
		if bt.Len() == 10000 {
			bt = db.Batch()
		}
		bt.SliceAs[batchPostRow](insertIfAuthor(db, int64(i), "a title", "a body"))
	}
}

func BenchmarkBatchQueueTemplate(b *testing.B) {
	db := stubbed(&stubBatcher{})
	ins := postTemplate(db)
	b.ReportAllocs()
	bt := db.Batch()
	for i := 0; b.Loop(); i++ {
		if bt.Len() == 10000 {
			bt = db.Batch()
		}
		ins.In(bt).Slice(postArgs{Author: int64(i), Title: "a title", Body: "a body"})
	}
}

func BenchmarkBatchQueueTemplateGrown(b *testing.B) {
	db := stubbed(&stubBatcher{})
	ins := postTemplate(db)
	b.ReportAllocs()
	bt := db.Batch()
	bt.Grow(10000)
	for i := 0; b.Loop(); i++ {
		if bt.Len() == 10000 {
			bt = db.Batch()
			bt.Grow(10000)
		}
		ins.In(bt).Slice(postArgs{Author: int64(i), Title: "a title", Body: "a body"})
	}
}

func BenchmarkTemplateIdentSlot(b *testing.B) {
	db := stubbed(&stubBatcher{})
	slotted := schemaTemplate(db)
	b.Run("slot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _, err := slotted.Build(ageIn{Schema: "tenant_42", Age: 30})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	tp := db.Select[batchUser]().Table("tenant_42.batch_users AS u").
		Where("u.age > ?", barm.Param("age")).Template[ageIn]()
	b.Run("no-slot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _, err := tp.Build(ageIn{Age: 30})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ranOn is a DB answering every query with one User row, which records its
// name in ran when it runs one.
func ranOn(name string, ran *[]string) *barm.DB {
	return barm.New(barm.SQL(sql.OpenDB(fakeConnector{rows: 1}), barm.Postgres), barm.WithHook(barm.QueryHook{
		AfterQuery: func(context.Context, *barm.QueryEvent) { *ran = append(*ran, name) },
	}))
}

type idIn struct {
	ID int64 `barm:"id"`
}

// A template runs on Via's handle, else the one in ctx, else its own; Build,
// with no ctx, never looks for one.
func TestTemplateHandleFromContext(t *testing.T) {
	t.Parallel()
	var ran []string
	own, inCtx, via := ranOn("own", &ran), ranOn("ctx", &ran), ranOn("via", &ran)
	tp := own.Select[User]().Where("id = ?", barm.Param("id")).Template[idIn]()
	ctx := barm.WithIDB(t.Context(), inCtx)

	for _, run := range []func() error{
		func() error { _, err := tp.One(t.Context(), idIn{1}); return err },
		func() error { _, err := tp.One(ctx, idIn{1}); return err },
		func() error { _, err := tp.Via(via).One(ctx, idIn{1}); return err },
		func() error { _, err := tp.Slice(ctx, idIn{1}); return err },
		func() error {
			for _, err := range tp.Seq(ctx, idIn{1}) {
				if err != nil {
					return err
				}
			}
			return nil
		},
	} {
		err := run()
		if err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"own", "ctx", "via", "ctx", "ctx"}; !slices.Equal(ran, want) {
		t.Errorf("ran on %v, want %v", ran, want)
	}
	qb := barm.NewBuilder(barm.Postgres).Select[User]().Where("id = ?", barm.Param("id")).Template[idIn]()
	_, err := qb.One(t.Context(), idIn{1})
	if !errors.Is(err, barm.ErrNoConn) {
		t.Errorf("a builder's template with nothing in ctx: %v, want ErrNoConn", err)
	}
	_, err = qb.One(ctx, idIn{1})
	if err != nil {
		t.Errorf("a builder's template with a DB in ctx: %v", err)
	}
}

// HandleFrom is the handle in ctx, or the fallback; a ctx carrying another
// replaces it.
func TestHandleFrom(t *testing.T) {
	t.Parallel()
	a, b := barm.NewBuilder(barm.Postgres), barm.NewBuilder(barm.Postgres)
	if h := barm.HandleFrom(t.Context(), a); h.IDB != a {
		t.Error("empty ctx: not the fallback")
	}
	if barm.IDBFrom(t.Context()) != nil {
		t.Error("empty ctx: IDBFrom is not nil")
	}
	ctx := barm.WithIDB(t.Context(), b)
	if h := barm.HandleFrom(ctx, a); h.IDB != b {
		t.Error("ctx with b: not b")
	}
	if h := barm.HandleFrom(barm.WithIDB(ctx, a), b); h.IDB != a {
		t.Error("ctx with b, then a: not a")
	}
}

// A template with nothing to bind takes struct{}, and sends what its builder
// would, without copying the arguments it rendered.
//
//nolint:paralleltest // AllocsPerRun refuses a parallel test
func TestTemplateNoArgs(t *testing.T) {
	stub := &stubBatcher{reader: emptyReader{}}
	db := stubbed(stub)
	adults := db.Select[batchUser]().Where("age >= ?", 18).Template[struct{}]()

	b := db.Batch()
	b.Slice(db.Select[batchUser]().Where("age >= ?", 18))
	adults.In(b).Slice(struct{}{})
	err := b.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stub.sent[1], stub.sent[0]; got.Query != want.Query || !reflect.DeepEqual(got.Args, want.Args) {
		t.Errorf("template sends %s %v, want %s %v", got.Query, got.Args, want.Query, want.Args)
	}
	if n := testing.AllocsPerRun(100, func() { _, _, _ = adults.Build(struct{}{}) }); n != 0 {
		t.Errorf("building a template with no arguments takes %v allocs", n)
	}
}
