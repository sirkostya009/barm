// Batching: several queries sent to the database in one round trip, their
// results read back into typed BatchResults once Run returns. Sending them is
// the driver's: pgxdriver pipelines them, and database/sql cannot, so SQL's
// batches fail with ErrNoBatcher unless SequentialBatches says to run them one
// at a time.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// BatchQuery is one rendered query on its way to a driver.
type BatchQuery struct {
	Query string
	Args  []any
	// Name is the query's Prepare name, empty when it was not named: a driver
	// prepares a named one, and runs the rest however it runs a plain query.
	Name string
}

// ExecResult is what a write reports back, in a form any driver can fill.
type ExecResult struct {
	RowsAffected int64
}

// BatchReader yields one result per queued query, in queue order. Rows and Exec
// each advance to the next result.
type BatchReader interface {
	Rows() (Rows, error)
	Exec() (ExecResult, error)
}

// ErrNoBatcher is returned when the driver cannot send a batch.
var ErrNoBatcher = errors.New("barm: batching needs a driver that pipelines — pgxdriver, or barm.SQL with SequentialBatches")

// ErrNotRun is the error on a result whose query did not execute: the batch has
// not been run yet, or an earlier query in it failed and the rest never went.
var ErrNotRun = errors.New("barm: batch query did not run")

// BatchResult carries one query's outcome. Until Run fills it, it holds the zero
// value and ErrNotRun, so a result read too early says so rather than looking
// like an empty answer.
type BatchResult[T any] struct {
	v   T
	err error
}

// newResult starts out not-run, which is what an unread result honestly is.
func newResult[T any]() *BatchResult[T] { return &BatchResult[T]{err: ErrNotRun} }

// Value returns the result, or the zero value if the query failed or never ran.
// Check Run's error first and this is total.
func (r *BatchResult[T]) Value() T { return r.v }

// Err returns this query's error, if it had one.
func (r *BatchResult[T]) Err() error { return r.err }

// Get returns the result and its error together.
func (r *BatchResult[T]) Get() (T, error) { return r.v, r.err }

func (r *BatchResult[T]) set(v T, err error) { r.v, r.err = v, err }

// Batch sends queries together, in one round trip. Each queued query hands back
// a BatchResult typed to what that query returns, so one batch mixes as many row
// types as it likes and a loop can queue any amount:
//
//	b := db.Batch()
//
//	adults := b.Slice(db.Select[User]().Where("age >= ?", 18))
//	total := b.Count(db.Select[User]())
//
//	if err := b.Run(ctx); err != nil {
//		return err
//	}
//	for _, u := range adults.Value() { ... }
//
// Batching needs a driver that pipelines, which pgxdriver does.
type Batch struct {
	e Executor
	// inTx says the batch runs inside a transaction, where Begin opens a
	// savepoint rather than a transaction of its own.
	inTx bool
	// held says the batch runs on a connection it keeps past Run, which is what
	// Rollback needs to reach.
	held bool
	// fallback says a driver that cannot batch has its queries sent one at a
	// time instead, which reports them, so the batch must not.
	fallback bool
	names    *names
	qs       []BatchQuery
	items    []item
	// rels renders the relations of the queries that asked for them, which
	// hang off rows the batch hands over only once it is done with its
	// connection. They load then, a depth at a time across every query.
	rels  []relLoad
	hooks *batchHooks
	err   error
}

// batchHooks are a batch's hooks, with the target of each query queued. They sit
// behind a pointer that is nil without a hook, so a batch pays a word for them
// and its queries nothing.
type batchHooks struct {
	hooks []QueryHook
	ts    []target
}

func newBatchHooks(hooks []QueryHook) *batchHooks {
	if len(hooks) == 0 {
		return nil
	}
	return &batchHooks{hooks: hooks}
}

func (h *batchHooks) list() []QueryHook {
	if h == nil {
		return nil
	}
	return h.hooks
}

func (h *batchHooks) start(ctx context.Context, i int, q BatchQuery) (context.Context, *QueryEvent) {
	if h == nil {
		return ctx, nil
	}
	return startQuery(ctx, h.hooks, h.ts[i], q.Name, q.Query, q.Args)
}

func (h *batchHooks) unsent(ctx context.Context, i int, q BatchQuery, err error) {
	if h != nil {
		unsentQuery(ctx, h.hooks, h.ts[i], q.Name, q.Query, q.Args, err)
	}
}

type item struct {
	// read returns the exec result when there is one, for the hooks, and how
	// many rows it read when there is none.
	read func(BatchReader) (sql.Result, int64, error)
	fail func(error)
}

func newBatch(e Executor, s *session, inTx, held bool) *Batch {
	out := &Batch{e: e, hooks: newBatchHooks(s.hooks), names: s.names, inTx: inTx, held: held}
	if e == nil {
		out.err = ErrNoConn
	}
	return out
}

// Len returns the number of queued queries.
func (b *Batch) Len() int { return len(b.items) }

// Err returns the first queueing error, if any. Run reports it too.
func (b *Batch) Err() error { return b.err }

// queue renders a query and records how to read its result. A named one is
// bound to its SQL as any named query is.
func (b *Batch) queue(t target, name, query string, args []any, err error, it item) {
	if err == nil && b.err != nil {
		err = b.err // a batch with nothing to run on fails every query
	}
	if err == nil && name != "" {
		err = b.names.bind(name, query)
	}
	if err != nil {
		if b.err == nil {
			b.err = err
		}
		it.fail(err)
		return
	}
	b.qs = append(b.qs, BatchQuery{Query: query, Args: args, Name: name})
	if b.hooks != nil {
		b.hooks.ts = append(b.hooks.ts, t)
	}
	b.items = append(b.items, it)
}

// relLoad is one query's relations in a batch: how to render them over the rows
// it read, and where to record the outcome.
type relLoad struct {
	prepare func() ([]*pending, error)
	done    func(error)
}

// Slice queues the query and collects its rows. Its relations load once the
// batch is done, together with every other query's, a round trip per depth.
func (b *Batch) Slice[U any](q *SelectQuery[U]) *BatchResult[[]U] {
	query, args, err := q.Build()
	r := queueSlice[U](b, q.target(), q.name, query, args, err, q.limit)
	if len(q.rels) > 0 {
		b.rels = append(b.rels, relLoad{
			prepare: func() ([]*pending, error) { return q.prepareRelations(r.v) },
			done: func(err error) {
				if err != nil {
					r.err = err
				}
			},
		})
	}
	return r
}

// One queues the query and takes its single row, or ErrNoRows.
func (b *Batch) One[U any](q *SelectQuery[U]) *BatchResult[U] {
	c := *q
	c.limit = 1
	query, args, err := c.Build()
	r := queueRow[U](b, c.target(), q.name, query, args, err)
	if len(q.rels) > 0 {
		rows := make([]U, 1)
		b.rels = append(b.rels, relLoad{
			prepare: func() ([]*pending, error) {
				rows[0] = r.v
				return q.prepareRelations(rows)
			},
			done: func(err error) { r.set(rows[0], err) },
		})
	}
	return r
}

// Count queues the query as a COUNT(*).
func (b *Batch) Count[T any](q *SelectQuery[T]) *BatchResult[int64] {
	query, args, err := q.CountQuery()
	return queueRow[int64](b, q.target(), "", query, args, err)
}

// Exists queues the query as SELECT EXISTS (...).
func (b *Batch) Exists[T any](q *SelectQuery[T]) *BatchResult[bool] {
	query, args, err := q.ExistsQuery()
	return queueRow[bool](b, q.target(), "", query, args, err)
}

var errBatchAsRelations = errors.New("barm: a batch loads relations through One and Slice, not OneAs and SliceAs")

// asQuery is a query whose SQL depends on what its rows are read into: a select
// narrows its columns to the result, a write returns them.
type asQuery interface {
	batchAs(m *model, err error, one bool) (string, []any, error)
}

func buildAs[U any](q Query, one bool) (string, []any, error) {
	a, ok := q.(asQuery)
	if !ok {
		return q.Build()
	}
	m, err := modelOf[U]()
	return a.batchAs(m, err, one)
}

// OneAs queues any query and reads its first row into U, or ErrNoRows. It is
// the batch's form of the builders' OneAs: a select narrows to U's columns and
// runs with LIMIT 1, a write returns U's columns, and a raw query is read as
// written.
//
//	b.OneAs[User](db.Insert[CreateUser]().Table("users").Values(&in))
//
// A select's relations load through One, so they are an error here.
func (b *Batch) OneAs[U any](q Query) *BatchResult[U] {
	query, args, err := buildAs[U](q, true)
	return queueRow[U](b, targetOf(q), stmtName(q), query, args, err)
}

// SliceAs queues any query and collects its rows as U, rendered as OneAs
// renders it, without the LIMIT.
func (b *Batch) SliceAs[U any](q Query) *BatchResult[[]U] {
	query, args, err := buildAs[U](q, false)
	return queueSlice[U](b, targetOf(q), stmtName(q), query, args, err, 0)
}

// queueSlice queues a query read as all its rows, hint being how many to expect.
func queueSlice[U any](b *Batch, t target, name, query string, args []any, err error, hint int64) *BatchResult[[]U] {
	r := newResult[[]U]()
	b.queue(t, name, query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, int64, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, 0, err
			}
			defer rows.Close()

			v, err := scanSlice[U](rows, hint)
			r.set(v, err)
			return nil, int64(len(v)), err
		},
	})
	return r
}

// queueRow queues a query read as a single row, its first.
func queueRow[U any](b *Batch, t target, name, query string, args []any, err error) *BatchResult[U] {
	r := newResult[U]()
	b.queue(t, name, query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, int64, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, 0, err
			}
			defer rows.Close()

			var v U
			err = scanRow(rows, &v)
			r.set(v, err)
			if err != nil {
				return nil, 0, err
			}
			return nil, 1, nil
		},
	})
	return r
}

// BatchSavepoint is the savepoint a batch opens inside a transaction. It is a
// fixed name rather than a generated one because a batch keeps no state of its
// own: Begin, Commit and Rollback each work it out again from what the batch is
// running on. Nesting still behaves, since ROLLBACK TO and RELEASE both act on
// the most recent savepoint of a name.
//
// It is exported because recovering from a failed batch happens outside that
// batch — see Rollback.
const BatchSavepoint = "barm_batch"

// Begin queues the statement that opens a transaction for the queries behind
// it — BEGIN on a pool or a held connection, and SAVEPOINT inside a transaction,
// where BEGIN would be a warning and the matching COMMIT would end the
// transaction you are already in.
//
//	b := tx.Batch()
//	b.Begin()
//	for _, r := range rows {
//		b.Exec(tx.Insert[Row]().Values(r))
//	}
//	b.Commit()
//	err := b.Run(ctx)
//
// It goes out with the rest, so the transaction costs no round trip of its own.
func (b *Batch) Begin() *BatchResult[ExecResult] {
	if b.inTx {
		return b.queueExec(opSavepoint, "", "SAVEPOINT "+BatchSavepoint, nil, nil)
	}
	return b.queueExec(opBegin, "", "BEGIN", nil, nil)
}

// Commit queues the statement that ends what Begin opened: RELEASE SAVEPOINT
// inside a transaction, COMMIT otherwise.
func (b *Batch) Commit() *BatchResult[ExecResult] {
	if b.inTx {
		return b.queueExec(opReleaseSavepoint, "", "RELEASE SAVEPOINT "+BatchSavepoint, nil, nil)
	}
	return b.queueExec(opCommit, "", "COMMIT", nil, nil)
}

// Rollback undoes what Begin opened, and runs immediately rather than queueing:
// ROLLBACK TO SAVEPOINT inside a transaction, ROLLBACK otherwise.
//
// It is the one of the three that cannot ride in the batch. A query that fails
// takes every statement queued behind it — the Commit among them — and the
// session then refuses everything until the transaction ends, so a queued
// rollback would be skipped exactly when it is needed. Recovery is its own round
// trip, and this is it:
//
//	if err := b.Run(ctx); err != nil {
//		b.Rollback(ctx)
//		return err
//	}
//
// The statement goes to the connection the batch ran on, which is the only one
// it means anything on — so the batch has to be holding one, as it is on a
// transaction or a held connection. A batch on the pool borrows a connection per
// Run and has given it back by the time this could be called. A failure there
// costs that connection: both drivers close it rather than hand on a
// transaction left open.
//
// The context is used for the statement but not for cancellation: a batch that
// failed because its context ended still has a transaction to close.
func (b *Batch) Rollback(ctx context.Context) error {
	if !b.held {
		return errors.New("barm: rolling back a batch needs the connection it ran on, and the pool does not hold one — batch on a transaction or a held connection")
	}
	stmt, op := "ROLLBACK", opRollback
	if b.inTx {
		stmt, op = "ROLLBACK TO SAVEPOINT "+BatchSavepoint, opRollbackSavepoint
	}
	hooks := b.hooks.list()
	ctx, ev := startQuery(context.WithoutCancel(ctx), hooks, op, "", stmt, nil)
	res, err := b.e.Exec(ctx, stmt, nil)
	finishQuery(ctx, hooks, ev, res, 0, err)
	return err
}

// Exec queues a write. An insert with a RETURNING clause scans the returned
// columns back into its values, as its own Exec does.
func (b *Batch) Exec(q Query) *BatchResult[ExecResult] {
	if w, ok := q.(returningWrite); ok && w.hasReturning() {
		return b.queueReturning(q, w)
	}
	query, args, err := q.Build()
	return b.queueExec(targetOf(q), stmtName(q), query, args, err)
}

// stmtName is the name q was given to Prepare, if any.
func stmtName(q Query) string {
	if n, ok := q.(interface{ stmtName() string }); ok {
		return n.stmtName()
	}
	return ""
}

// returningWrite is a write whose Exec reads RETURNING back into its values.
type returningWrite interface {
	hasReturning() bool
	scanReturning(Rows) (int64, error)
}

// queueReturning queues a write that reads its RETURNING rows back, counting
// them as what it affected.
func (b *Batch) queueReturning(q Query, w returningWrite) *BatchResult[ExecResult] {
	r := newResult[ExecResult]()
	query, args, err := q.Build()
	b.queue(targetOf(q), stmtName(q), query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, int64, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, 0, err
			}
			defer rows.Close()
			n, err := w.scanReturning(rows)
			r.set(ExecResult{RowsAffected: n}, err)
			return rowsAffected(n), n, err
		},
	})
	return r
}

// queueExec queues a statement whose result is a row count. A write and a
// transaction-control statement report the same shape, so they read the same
// way; only where the SQL comes from differs.
func (b *Batch) queueExec(t target, name, query string, args []any, err error) *BatchResult[ExecResult] {
	r := newResult[ExecResult]()
	b.queue(t, name, query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, int64, error) {
			res, err := br.Exec()
			r.set(res, err)
			return rowsAffected(res.RowsAffected), 0, err
		},
	})
	return r
}

// Run sends the batch and fills every result. It reports the first failure,
// naming the query that broke; the results after it report ErrNotRun.
//
// Whether a failure rolls the batch back is the driver's business: pgx runs one
// in an implicit transaction unless it is already inside one.
func (b *Batch) Run(ctx context.Context) error {
	items, qs, rels, err := b.items, b.qs, b.rels, b.err
	b.items, b.qs, b.rels, b.err = nil, nil, nil, nil // a batch runs once
	hooks := b.hooks
	b.hooks = newBatchHooks(hooks.list())
	if err != nil {
		for i, q := range qs {
			hooks.unsent(ctx, i, q, ErrNotRun)
		}
		return err
	}
	if len(items) == 0 {
		return nil
	}

	// A query's event starts when barm turns to its result, the first one as the
	// batch goes out. On a driver that pipelines that is how long the query kept
	// the batch waiting, and on one sending a query at a time it is that query's
	// round trip. A query that never reached the database reports no times.
	var runErr error
	reached := false
	sendErr := b.e.SendBatch(ctx, qs, func(br BatchReader) error {
		reached = true
		var rr *respondedReader
		if hooks != nil {
			rr = &respondedReader{BatchReader: br}
			br = rr
		}
		for i, it := range items {
			if runErr != nil {
				it.fail(ErrNotRun)
				hooks.unsent(ctx, i, qs[i], ErrNotRun)
				continue
			}
			c, ev := hooks.start(ctx, i, qs[i])
			if rr != nil {
				rr.ev = ev
			}
			res, n, err := it.read(br)
			finishQuery(c, hooks.list(), ev, res, n, err)
			if err != nil {
				runErr = fmt.Errorf("barm: batch query %d: %w", i, err)
			}
		}
		return runErr
	})
	if runErr != nil {
		return runErr
	}
	if sendErr != nil {
		for _, it := range items {
			it.fail(sendErr)
		}
		if !reached && (!b.fallback || !errors.Is(sendErr, ErrNoBatcher)) {
			for i, q := range qs {
				hooks.unsent(ctx, i, q, sendErr)
			}
		}
		return sendErr
	}
	return b.loadRelations(ctx, rels)
}

// loadRelations loads what the batch's queries asked for, a depth at a time
// across all of them, on the connection the batch itself ran on.
func (b *Batch) loadRelations(ctx context.Context, rels []relLoad) error {
	if len(rels) == 0 {
		return nil
	}
	var level []*pending
	var err error
	for _, l := range rels {
		var ws []*pending
		ws, err = l.prepare()
		if err != nil {
			break
		}
		level = append(level, ws...)
	}
	if err == nil {
		err = loadLevels(ctx, level, func(ctx context.Context, level []*pending) error {
			return batchRelations(ctx, &Batch{e: b.e, hooks: newBatchHooks(b.hooks.list()), names: b.names, inTx: b.inTx, held: b.held}, level)
		})
	}
	for _, l := range rels {
		l.done(err)
	}
	return err
}

// rowsAffected is a sql.Result that knows only its row count: a batch exec's,
// whose protocol carries no insert id, or a RETURNING insert's, whose keys went
// into the rows instead.
type rowsAffected int64

func (rowsAffected) LastInsertId() (int64, error) {
	return 0, errors.New("barm: this result carries no last insert id")
}
func (n rowsAffected) RowsAffected() (int64, error) { return int64(n), nil }
