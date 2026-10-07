// Hooks let callers watch what barm does. A QueryHook sees every query before
// and after it runs, for logging, tracing and metrics. A TxHook sees the
// commit and rollback of the outermost transaction, for cache invalidation,
// and never a savepoint: hooks registered inside a nested transaction wait for
// the real commit, and a rollback to that savepoint drops them.

package barm

import (
	"cmp"
	"context"
	"database/sql"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// QueryEvent describes one database call.
type QueryEvent struct {
	// Op is what the statement does: SELECT, INSERT, UPDATE or DELETE for a
	// builder, whatever its WITH clause holds, and the leading keyword of raw
	// SQL.
	Op string
	// Table is the table a builder reads or writes, as its model or Table names
	// it, and Schema what Schema qualified it with. Raw SQL has neither, and a
	// select from an expression rather than a table name has no Table.
	Table  string
	Schema string
	// Relation names the relation a query loads, as Type.Field, and is empty
	// for any query you wrote.
	Relation string
	Query    string
	Args     []any
	// Prepared is the name given to Prepare, if any.
	Prepared string
	// Caller is where in your code the call was made, with WithCaller: the first
	// frame outside barm, or for a batch's query where it was queued. It is
	// shared by every call from that place, so it must not be changed, and nil
	// without WithCaller.
	Caller *runtime.Frame
	// BatchSize is how many queries went in the batch this one is part of, and
	// BatchIndex its place among them. Both are zero outside a batch, and
	// int32 so that the event fits the 256-byte allocation class.
	BatchSize, BatchIndex int32
	// StartedAt is when the call went to the database. It is zero for a call
	// that never reached it, which reports no FirstResponse or Duration either.
	StartedAt time.Time

	// Set before AfterQuery runs.
	CallStats // what the driver reports, with WithCallStats
	Err       error
	Result    sql.Result // exec only
	// Rows counts the rows read from the result. An exec reads none, and
	// reports what it changed through Result.
	Rows int64
	// FirstResponse is how long the database took to start answering.
	FirstResponse time.Duration
	// Duration is how long until the rows were read and closed: the end of a Seq
	// loop, or the Close of rows handed to the caller, the caller's own work
	// included. Both include CallStats.Wait.
	Duration time.Duration

	call statsCtx // what the driver is handed, pointing at CallStats
}

// target is what a builder knows about its statement and raw SQL does not.
type target struct {
	op, table, schema, rel string
}

// The ops of the statements barm sends to begin and end transactions, named in
// full where a leading keyword alone would pass a rewind for a rollback.
var (
	opBegin             = target{op: "BEGIN"}
	opCommit            = target{op: "COMMIT"}
	opRollback          = target{op: "ROLLBACK"}
	opSavepoint         = target{op: "SAVEPOINT"}
	opReleaseSavepoint  = target{op: "RELEASE SAVEPOINT"}
	opRollbackSavepoint = target{op: "ROLLBACK TO SAVEPOINT"}
)

// keepTarget moves t to the heap for something that holds on to it, and only
// when a hook will read it: without one, holding it costs a nil pointer.
func keepTarget(hooks []QueryHook, t target) *target {
	if len(hooks) == 0 {
		return nil
	}
	p := new(target)
	*p = t
	return p
}

func (t *target) get() target {
	if t == nil {
		return target{}
	}
	return *t
}

// targetOf is q's target, empty for raw SQL.
func targetOf(q Query) target {
	if t, ok := q.(interface{ target() target }); ok {
		return t.target()
	}
	return target{}
}

// QueryHook observes queries. Both fields are optional: a hook that only logs
// finished queries sets AfterQuery and leaves BeforeQuery nil.
//
//	barm.WithHook(barm.QueryHook{
//		AfterQuery: func(ctx context.Context, ev *barm.QueryEvent) {
//			log.Printf("%s in %s: %v", ev.Op, ev.Duration, ev.Err)
//		},
//	})
//
// BeforeQuery may return a derived context, which is passed to the database
// call and back to AfterQuery — that is where a tracing hook stashes its span.
type QueryHook struct {
	BeforeQuery func(context.Context, *QueryEvent) context.Context
	AfterQuery  func(context.Context, *QueryEvent)
}

// TxEvent describes one transaction ending.
type TxEvent struct {
	// StartedAt is when the transaction began, so Duration covers all of it.
	StartedAt time.Time

	// Set before AfterCommit and AfterRollback run.
	Err      error
	Duration time.Duration
}

// TxHook observes transactions. It is a struct rather than an interface because
// watching one end of a transaction is the common case: set the field you want
// and leave the rest nil.
//
//	barm.WithTxHook(barm.TxHook{
//		AfterCommit: func(ctx context.Context, ev *barm.TxEvent) {
//			log.Printf("commit in %s: %v", ev.Duration, ev.Err)
//		},
//	})
//
// Like QueryHook, the Before functions may return a derived context, which is
// passed on to the matching After.
//
// Hooks fire once per transaction, at the end of the outermost one — the only
// end that is durable. A nested commit is a RELEASE SAVEPOINT, which decides
// nothing on its own, so it fires nothing: a cache invalidated there would be
// invalidated for work the enclosing transaction can still roll back.
//
// Hooks belong to the transaction rather than to the handle they were
// registered on, so WithTxHook inside a nested transaction still fires at that
// end. A nested rollback discards the ones registered within it, along with the
// work they were about to report.
type TxHook struct {
	// BeforeCommit runs before the transaction commits, which is where work
	// that belongs inside it goes — draining an outbox, say. An error stops the
	// commit and is what Commit returns; the transaction is left open, for the
	// caller's deferred Rollback to undo.
	BeforeCommit func(context.Context, *TxEvent) (context.Context, error)
	// AfterCommit pairs with a BeforeCommit that returned no error: the hook
	// that fails unwinds its own work before returning, and the hooks after it
	// never ran. Every earlier hook is unwound with the error on the event.
	AfterCommit func(context.Context, *TxEvent)

	// BeforeRollback runs before the transaction rolls back. Nothing can refuse
	// a rollback, so it reports no error.
	BeforeRollback func(context.Context, *TxEvent) context.Context
	AfterRollback  func(context.Context, *TxEvent)
}

// txHook is a registered TxHook and the depth of the transaction it was
// registered on.
type txHook struct {
	TxHook
	depth int
}

// dropHooks discards the hooks registered at this transaction's depth or below,
// with the work a savepoint rollback undoes. It filters rather than rewinding to
// an earlier length, which would drop what an enclosing transaction registered
// meanwhile and bring back what an earlier rollback dropped.
func (tx *Tx) dropHooks() {
	*tx.txHooks = slices.DeleteFunc(*tx.txHooks, func(h txHook) bool { return h.depth >= tx.depth })
}

// handHooksUp gives the hooks at this depth or below to the parent once the
// savepoint is released: its work is the parent's now, and so is reporting it.
func (tx *Tx) handHooksUp() {
	for i := range *tx.txHooks {
		if h := &(*tx.txHooks)[i]; h.depth >= tx.depth {
			h.depth = tx.depth - 1
		}
	}
}

// txEvent builds the event, or nil when nothing should fire: a nested
// transaction, whose ending is not the transaction's, or no hook registered —
// the fast path, the same one QueryEvent takes.
func (tx *Tx) txEvent() *TxEvent {
	if tx.savepoint != "" || len(*tx.txHooks) == 0 {
		return nil
	}
	return &TxEvent{StartedAt: tx.startedAt}
}

// beforeCommit fires BeforeCommit in registration order, stopping at the first
// error. It returns how many hooks ran, so that only those are unwound.
func (tx *Tx) beforeCommit(ev *TxEvent) (context.Context, int, error) {
	ctx := tx.ctx
	if ev == nil {
		return ctx, 0, nil
	}
	for i, h := range *tx.txHooks {
		if h.BeforeCommit == nil {
			continue
		}
		c, err := h.BeforeCommit(ctx, ev)
		if c != nil {
			ctx = c //nolint:fatcontext // hooks thread ctx through each other by design; the loop bound is len(hooks)
		}
		if err != nil {
			return ctx, i, err
		}
	}
	return ctx, len(*tx.txHooks), nil
}

// afterCommit fires AfterCommit in reverse over the n hooks that ran, so they
// unwind like defers.
func (tx *Tx) afterCommit(ctx context.Context, ev *TxEvent, n int, err error) {
	if ev == nil {
		return
	}
	ev.Err, ev.Duration = err, time.Since(ev.StartedAt)
	for _, h := range slices.Backward((*tx.txHooks)[:n]) {
		if h.AfterCommit != nil {
			h.AfterCommit(ctx, ev)
		}
	}
}

func (tx *Tx) beforeRollback(ev *TxEvent) context.Context {
	ctx := tx.ctx
	if ev == nil {
		return ctx
	}
	for _, h := range *tx.txHooks {
		if h.BeforeRollback == nil {
			continue
		}
		if c := h.BeforeRollback(ctx, ev); c != nil {
			ctx = c //nolint:fatcontext // hooks thread ctx through each other by design; the loop bound is len(hooks)
		}
	}
	return ctx
}

func (tx *Tx) afterRollback(ctx context.Context, ev *TxEvent, err error) {
	if ev == nil {
		return
	}
	ev.Err, ev.Duration = err, time.Since(ev.StartedAt)
	for _, h := range slices.Backward(*tx.txHooks) {
		if h.AfterRollback != nil {
			h.AfterRollback(ctx, ev)
		}
	}
}

// startQuery fires BeforeQuery for a call about to reach the database. It
// returns a nil event when no hook is registered, which is the fast path: no
// timestamp, no allocation. It takes the hooks rather than a runner because a
// batch has queries but no runner.
func startQuery(ctx context.Context, hooks []QueryHook, w watch, t target, name, query string, args []any) (context.Context, *QueryEvent) {
	if len(hooks) == 0 {
		return ctx, nil
	}
	ev := newEvent(t, name, query, args)
	if w&watchCaller != 0 {
		ev.Caller = callerFrame() // ahead of the clock, so its cost is not the call's
	}
	ev.StartedAt = time.Now()
	ctx = beforeQuery(ctx, hooks, ev)
	if w&watchStats != 0 {
		ctx = ev.report(ctx)
	}
	return ctx, ev
}

// watch is what an event records beyond the call itself, as options asked.
type watch uint8

const (
	watchStats  watch = 1 << iota // CallStats, by WithCallStats
	watchCaller                   // Caller, by WithCaller
)

// barmFuncs prefixes the names of barm's own functions, generic ones included.
var barmFuncs = reflect.TypeFor[DB]().PkgPath() + "."

// callerFrame is the first frame outside barm on the way to this call.
func callerFrame() *runtime.Frame {
	var pcs [32]uintptr
	for _, pc := range pcs[:runtime.Callers(2, pcs[:])] {
		if s := siteOf(pc); !s.barm {
			return &s.frame
		}
	}
	return nil
}

// callSite is what a return address resolves to: barm's own code, or the first
// frame outside it, of the functions inlined there.
type callSite struct {
	frame runtime.Frame
	barm  bool
}

// callSites caches each return address resolved, since resolving one costs
// more than the query being watched: a program has only so many of them.
var callSites sync.Map // uintptr → *callSite

func siteOf(pc uintptr) *callSite {
	if s, ok := callSites.Load(pc); ok {
		return s.(*callSite) //nolint:forcetypeassert // callSites only ever stores *callSite
	}
	site := &callSite{barm: true}
	frames := runtime.CallersFrames([]uintptr{pc})
	for more := true; more; {
		var f runtime.Frame
		f, more = frames.Next()
		if !strings.HasPrefix(f.Function, barmFuncs) {
			site = &callSite{frame: f}
			break
		}
	}
	s, _ := callSites.LoadOrStore(pc, site)
	return s.(*callSite) //nolint:forcetypeassert // callSites only ever stores *callSite
}

// report is the ctx to hand the driver, so it can report on the call.
func (ev *QueryEvent) report(ctx context.Context) context.Context {
	ev.call = statsCtx{ctx, &ev.CallStats}
	return &ev.call
}

// unsentQuery fires both hooks for a call that never reached the database. It
// has no round trip to time, so its times stay zero.
func unsentQuery(ctx context.Context, hooks []QueryHook, w watch, t target, name, query string, args []any, err error) {
	if len(hooks) == 0 {
		return
	}
	ev := newEvent(t, name, query, args)
	if w&watchCaller != 0 {
		ev.Caller = callerFrame()
	}
	finishQuery(beforeQuery(ctx, hooks, ev), hooks, ev, nil, 0, err)
}

func newEvent(t target, name, query string, args []any) *QueryEvent {
	ev := &QueryEvent{
		Op:       t.op,
		Schema:   t.schema,
		Relation: t.rel,
		Query:    query,
		Args:     slices.Clone(args), // a hook that masks a value in place must not change what is bound
		Prepared: name,
	}
	if ev.Op == "" {
		ev.Op = operation(query)
	}
	if isName(t.table) {
		ev.Table = t.table
	}
	return ev
}

func beforeQuery(ctx context.Context, hooks []QueryHook, ev *QueryEvent) context.Context {
	for _, h := range hooks {
		if h.BeforeQuery == nil {
			continue
		}
		if c := h.BeforeQuery(ctx, ev); c != nil {
			ctx = c //nolint:fatcontext // hooks thread ctx through each other by design; the loop bound is len(hooks)
		}
	}
	return ctx
}

// finishQuery fires AfterQuery in reverse order, so hooks unwind like defers.
func finishQuery(ctx context.Context, hooks []QueryHook, ev *QueryEvent, res sql.Result, rows int64, err error) {
	if ev == nil {
		return
	}
	ev.Err, ev.Result, ev.Rows = err, res, rows
	if !ev.StartedAt.IsZero() {
		ev.Duration = time.Since(ev.StartedAt)
		if ev.FirstResponse == 0 {
			ev.FirstResponse = ev.Duration
		}
	}
	for _, v := range slices.Backward(hooks) {
		if v.AfterQuery != nil {
			v.AfterQuery(ctx, ev)
		}
	}
}

// start readies a call and fires BeforeQuery. A call that cannot reach the
// database comes back as its error, its hooks already finished.
func (r *runner) start(ctx context.Context, t target, query string, args []any) (context.Context, *QueryEvent, Executor, error) {
	e, err := r.reach(query)
	if err != nil {
		r.h.sess().unsent(ctx, t, r.name, query, args, err)
		return ctx, nil, nil, err
	}
	ctx, ev := r.h.sess().startCall(ctx, t, r.name, query, args)
	return ctx, ev, e, nil
}

func (r *runner) finish(ctx context.Context, ev *QueryEvent, res sql.Result, rows int64, err error) {
	finishQuery(ctx, r.h.sess().hooks, ev, res, rows, err)
}

// responded marks the database's first response to a query whose rows are
// read before the hooks finish.
func (ev *QueryEvent) responded() {
	if ev != nil {
		ev.FirstResponse = time.Since(ev.StartedAt)
	}
}

// hookedRows are rows handed to the caller while a hook watches: the hooks
// finish when they are closed, with the rows read through them counted.
type hookedRows struct {
	Rows
	ctx   context.Context
	hooks []QueryHook
	ev    *QueryEvent
	n     int64
	err   error // the first Scan failure
}

func (r *hookedRows) Next() bool {
	if !r.Rows.Next() {
		return false
	}
	r.n++
	return true
}

func (r *hookedRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	if r.err == nil {
		r.err = err
	}
	return err
}

func (r *hookedRows) Close() error {
	err := r.Err()
	cerr := r.Rows.Close()
	if r.ev != nil {
		finishQuery(r.ctx, r.hooks, r.ev, nil, r.n, cmp.Or(r.err, err, cerr))
		r.ev = nil // closing twice finishes once
	}
	return cerr
}

// NativeJSON and NativeArrays pass through, so that wrapping the rows does not
// hide from the scan what they decode themselves.
func (r *hookedRows) NativeJSON() bool {
	n, ok := r.Rows.(NativeJSON)
	return ok && n.NativeJSON()
}

func (r *hookedRows) NativeArrays() bool {
	n, ok := r.Rows.(NativeArrays)
	return ok && n.NativeArrays()
}

// respondedReader marks the first response of each batch query as its result
// is reached.
type respondedReader struct {
	BatchReader
	ev *QueryEvent
}

func (r *respondedReader) Rows() (Rows, error) {
	rows, err := r.BatchReader.Rows()
	r.ev.responded()
	return rows, err
}

func (r *respondedReader) Exec() (ExecResult, error) {
	res, err := r.BatchReader.Exec()
	r.ev.responded()
	return res, err
}

// closeRows closes rows barm read itself, and reports the error the reading
// ended in, or else the one closing did.
func closeRows(rows Rows, err error) error {
	cerr := rows.Close()
	if err == nil {
		err = cerr
	}
	return err
}

// operation returns the leading SQL keyword.
func operation(query string) string {
	for i := range len(query) {
		switch query[i] {
		case ' ', '\t', '\n':
			return query[:i]
		}
	}
	return query
}

// isName reports whether table is a name, dotted or not, rather than an
// expression a select reads from.
func isName(table string) bool {
	if table == "" {
		return false
	}
	for i := range len(table) {
		switch c := table[i]; {
		case c == '_' || c == '.' || c == '$' || c >= 0x80,
			'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		default:
			return false
		}
	}
	return true
}
