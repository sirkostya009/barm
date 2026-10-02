// Hooks let callers watch what barm does. A QueryHook sees every query before
// and after it runs, for logging, tracing and metrics. A TxHook sees the
// commit and rollback of the outermost transaction, for cache invalidation,
// and never a savepoint: hooks registered inside a nested transaction wait for
// the real commit, and a rollback to that savepoint drops them.

package barm

import (
	"context"
	"database/sql"
	"slices"
	"time"
)

// QueryEvent describes one database call.
type QueryEvent struct {
	// Op is the leading keyword of the query: SELECT, INSERT, UPDATE, DELETE.
	Op    string
	Query string
	Args  []any
	// Prepared is the name given to Prepare, if any.
	Prepared  string
	StartedAt time.Time

	// Set before AfterQuery runs.
	Err      error
	Result   sql.Result // exec only
	Duration time.Duration
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

// startQuery fires BeforeQuery. It returns a nil event when no hook is
// registered, which is the fast path: no timestamp, no allocation. It takes the
// hooks rather than a runner because a batch has queries but no runner.
func startQuery(ctx context.Context, hooks []QueryHook, name, query string, args []any) (context.Context, *QueryEvent) {
	if len(hooks) == 0 {
		return ctx, nil
	}
	ev := &QueryEvent{
		Op:        operation(query),
		Query:     query,
		Args:      slices.Clone(args), // a hook that masks a value in place must not change what is bound
		Prepared:  name,
		StartedAt: time.Now(),
	}
	for _, h := range hooks {
		if h.BeforeQuery == nil {
			continue
		}
		if c := h.BeforeQuery(ctx, ev); c != nil {
			ctx = c //nolint:fatcontext // hooks thread ctx through each other by design; the loop bound is len(hooks)
		}
	}
	return ctx, ev
}

// finishQuery fires AfterQuery in reverse order, so hooks unwind like defers.
func finishQuery(ctx context.Context, hooks []QueryHook, ev *QueryEvent, res sql.Result, err error) {
	if ev == nil {
		return
	}
	ev.Err, ev.Result, ev.Duration = err, res, time.Since(ev.StartedAt)
	for _, v := range slices.Backward(hooks) {
		if v.AfterQuery != nil {
			v.AfterQuery(ctx, ev)
		}
	}
}

func (r *runner) start(ctx context.Context, query string, args []any) (context.Context, *QueryEvent) {
	return startQuery(ctx, r.h.sess().hooks, r.name, query, args)
}

func (r *runner) finish(ctx context.Context, ev *QueryEvent, res sql.Result, err error) {
	finishQuery(ctx, r.h.sess().hooks, ev, res, err)
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
