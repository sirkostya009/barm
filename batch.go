// Batching: several queries sent to the database in one round trip, their
// results read back into typed BatchResults once Run returns. database/sql has
// no batch API, so barm reaches the driver connection underneath and hands the
// queries to a Batcher registered for that driver (pgxdriver registers pgx's).
// Without one, Run fails with ErrNoBatcher.

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
}

// ExecResult is what a write reports back, in a form any driver can fill.
type ExecResult struct {
	RowsAffected int64
}

// Batcher is the driver side of batching: it sends rendered queries together and
// calls read with their results. The driver owns the connection for the length
// of that call, which is what lets a batch ride a connection it has borrowed —
// including one with a transaction open on it.
//
// Implementing this is all a driver needs to do. barm ships pgx, natively and
// through database/sql; another driver that can pipeline plugs in the same way.
type Batcher interface {
	SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error
}

// StmtDriver is the driver side of named statements, for a driver that can
// prepare one in the same round trip as its first run: database/sql prepares
// in a call of its own, which is a round trip of its own, and on a pool it
// needs a connection of its own too. A Batcher a registered driver hands out
// that implements it runs every Prepare'd query on a connection it recognises.
//
// The statement is the driver's per connection: it prepares it on first use
// there, and runs it by name after.
type StmtDriver interface {
	// QueryStmt runs the statement and hands its rows to read, which must be
	// done with them by the time it returns.
	QueryStmt(ctx context.Context, name, query string, args []any, read func(Rows) error) error
	ExecStmt(ctx context.Context, name, query string, args []any) (ExecResult, error)
}

// BatchReader yields one result per queued query, in queue order. Row, Rows and
// Exec each advance to the next result.
type BatchReader interface {
	Rows() (Rows, error)
	Exec() (ExecResult, error)
}

// ErrNoBatcher is returned when nothing behind the DB can pipeline.
var ErrNoBatcher = errors.New("barm: batching needs a driver that pipelines")

// driverProbes recognise driver connections. A driver package registers one in
// its init, the way database/sql drivers register themselves.
var driverProbes []func(driverConn any) (Batcher, bool)

// RegisterDriver teaches barm to batch on a driver. The probe receives the
// driver's own connection — whatever sql.Conn.Raw hands out — and returns a
// Batcher for it, or false if it does not recognise the type.
//
// Importing a driver package for its side effect is all a caller does:
//
//	import _ "github.com/sirkostya009/barm/pgxdriver"
//
// after which any DB opened with that driver batches, with no configuration.
// It is not safe to call concurrently.
func RegisterDriver(probe func(driverConn any) (Batcher, bool)) {
	driverProbes = append(driverProbes, probe)
}

// batcherFor asks every registered driver whether it recognises this connection.
func batcherFor(driverConn any) (Batcher, bool) {
	for _, fn := range driverProbes {
		if b, ok := fn(driverConn); ok {
			return b, true
		}
	}
	return nil, false
}

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
// Batching needs a driver that pipelines. database/sql has no batch API of its
// own, but barm reaches the driver underneath it, so a DB or a transaction opened
// with pgx batches without any pgx types at the call site.
type Batch struct {
	b     Batcher
	qs    []BatchQuery
	items []item
	// rels renders the relations of the queries that asked for them, which
	// hang off rows the batch hands over only once it is done with its
	// connection. They load then, a depth at a time across every query.
	rels  []relLoad
	hooks []QueryHook
	err   error
}

type item struct {
	// read returns the exec result when there is one, for the hooks; a query
	// reading rows has none.
	read func(BatchReader) (sql.Result, error)
	fail func(error)
}

func newBatch(b Batcher, hooks []QueryHook) *Batch {
	out := &Batch{b: b, hooks: hooks}
	if b == nil {
		out.err = ErrNoBatcher
	}
	return out
}

// Len returns the number of queued queries.
func (b *Batch) Len() int { return len(b.items) }

// Err returns the first queueing error, if any. Run reports it too.
func (b *Batch) Err() error { return b.err }

// queue renders a query and records how to read its result.
func (b *Batch) queue(query string, args []any, err error, it item) {
	if err == nil && b.err != nil {
		err = b.err // a batch with no driver fails every query
	}
	if err != nil {
		if b.err == nil {
			b.err = err
		}
		it.fail(err)
		return
	}
	b.qs = append(b.qs, BatchQuery{Query: query, Args: args})
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
	r := newResult[[]U]()
	query, args, err := q.Build()

	b.queue(query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, err
			}
			defer rows.Close()

			v, err := scanSlice[U](rows, q.limit)
			r.set(v, err)
			return nil, err
		},
	})
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
	r := queueRow[U](b, query, args, err)
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
	return queueRow[int64](b, query, args, err)
}

// Exists queues the query as SELECT EXISTS (...).
func (b *Batch) Exists[T any](q *SelectQuery[T]) *BatchResult[bool] {
	query, args, err := q.ExistsQuery()
	return queueRow[bool](b, query, args, err)
}

// queueRow queues a query read as a single row, its first.
func queueRow[U any](b *Batch, query string, args []any, err error) *BatchResult[U] {
	r := newResult[U]()
	b.queue(query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, err
			}
			defer rows.Close()

			var v U
			err = scanRow(rows, &v)
			r.set(v, err)
			return nil, err
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

// inTransaction reports whether this batch rides a connection that already has
// a transaction on it. It is read from the batcher every time rather than
// remembered, so there is no second copy of the truth to go stale.
//
// A Batcher of your own reads as no transaction, since where it sends the queue
// is its business and barm cannot know.
func (b *Batch) inTransaction() bool {
	_, ok := b.b.(sqlTxBatcher)
	return ok
}

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
	if b.inTransaction() {
		return b.queueExec("SAVEPOINT "+BatchSavepoint, nil, nil)
	}
	return b.queueExec("BEGIN", nil, nil)
}

// Commit queues the statement that ends what Begin opened: RELEASE SAVEPOINT
// inside a transaction, COMMIT otherwise.
func (b *Batch) Commit() *BatchResult[ExecResult] {
	if b.inTransaction() {
		return b.queueExec("RELEASE SAVEPOINT "+BatchSavepoint, nil, nil)
	}
	return b.queueExec("COMMIT", nil, nil)
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
// Run and has given it back by the time this could be called; a failure there has
// already cost that connection, which the pool discards rather than reuse.
//
// The context is used for the statement but not for cancellation: a batch that
// failed because its context ended still has a transaction to close.
func (b *Batch) Rollback(ctx context.Context) error {
	conn, ok := b.conn()
	if !ok {
		return fmt.Errorf("barm: rolling back a batch needs the connection it ran on, and %T does not hold one — batch on a transaction or a held connection", b.b)
	}
	stmt := "ROLLBACK"
	if b.inTransaction() {
		stmt = "ROLLBACK TO SAVEPOINT " + BatchSavepoint
	}
	_, err := conn.ExecContext(context.WithoutCancel(ctx), stmt)
	return err
}

// conn returns the connection this batch runs on, when it holds one for longer
// than a single Run.
func (b *Batch) conn() (*sql.Conn, bool) {
	switch bb := b.b.(type) {
	case sqlTxBatcher:
		return bb.conn, true
	case sqlConnBatcher:
		return bb.conn, true
	}
	return nil, false
}

// Exec queues a write. An insert with a RETURNING clause scans the returned
// columns back into its values, as its own Exec does.
func (b *Batch) Exec(q Query) *BatchResult[ExecResult] {
	if w, ok := q.(returningWrite); ok && w.hasReturning() {
		return b.queueReturning(q, w)
	}
	return b.queueExec(q.Build())
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
	b.queue(query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, error) {
			rows, err := br.Rows()
			if err != nil {
				r.err = err
				return nil, err
			}
			defer rows.Close()
			n, err := w.scanReturning(rows)
			r.set(ExecResult{RowsAffected: n}, err)
			return rowsAffected(n), err
		},
	})
	return r
}

// queueExec queues a statement whose result is a row count. A write and a
// transaction-control statement report the same shape, so they read the same
// way; only where the SQL comes from differs.
func (b *Batch) queueExec(query string, args []any, err error) *BatchResult[ExecResult] {
	r := newResult[ExecResult]()
	b.queue(query, args, err, item{
		fail: func(err error) { r.err = err },
		read: func(br BatchReader) (sql.Result, error) {
			res, err := br.Exec()
			r.set(res, err)
			return rowsAffected(res.RowsAffected), err
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
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	// Every query in a batch goes out at the same moment, so they all start
	// together; each one finishes when its own result is read. A query that
	// never ran finishes too, with the error that stopped it.
	var ctxs []context.Context
	var evs []*QueryEvent
	if len(b.hooks) > 0 {
		ctxs, evs = make([]context.Context, len(items)), make([]*QueryEvent, len(items))
		for i, q := range qs {
			ctxs[i], evs[i] = startQuery(ctx, b.hooks, "", q.Query, q.Args) //nolint:fatcontext // each call derives from the same base ctx, not from the previous iteration's
		}
	}
	done := func(i int, res sql.Result, err error) {
		if evs == nil || evs[i] == nil {
			return
		}
		finishQuery(ctxs[i], b.hooks, evs[i], res, err)
		evs[i] = nil // an event finishes once, whichever path reaches it first
	}

	var runErr error
	sendErr := b.b.SendBatch(ctx, qs, func(br BatchReader) error {
		for i, it := range items {
			if runErr != nil {
				it.fail(ErrNotRun)
				done(i, nil, ErrNotRun)
				continue
			}
			res, err := it.read(br)
			done(i, res, err)
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
		for i, it := range items {
			it.fail(sendErr)
			done(i, nil, sendErr) // the batch never reached them
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
			return batchRelations(ctx, newBatch(b.b, b.hooks), level)
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

// sqlConnBatcher batches on one borrowed database/sql connection, over whichever
// registered driver is underneath it. Anything already running on that
// connection — an open transaction — therefore contains the batch.
type sqlConnBatcher struct{ conn *sql.Conn }

func (b sqlConnBatcher) SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error {
	return b.conn.Raw(func(dc any) error {
		driver, ok := batcherFor(dc)
		if !ok {
			return fmt.Errorf("%w: nothing registered for %T — import a barm driver package, "+
				"e.g. _ \"github.com/sirkostya009/barm/pgxdriver\"", ErrNoBatcher, dc)
		}
		return driver.SendBatch(ctx, qs, read)
	})
}

// sqlTxBatcher is sqlConnBatcher on a connection that already has a transaction
// open on it. It behaves identically — the type is the whole point, because it
// is what Begin reads to tell a savepoint from a transaction of its own.
type sqlTxBatcher struct{ sqlConnBatcher }

// sqlDBBatcher borrows a connection per batch.
type sqlDBBatcher struct{ db *sql.DB }

func (b *sqlDBBatcher) SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return sqlConnBatcher{conn}.SendBatch(ctx, qs, read)
}
