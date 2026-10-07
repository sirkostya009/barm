// Tx is a transaction on one connection, from begin until commit or rollback.
// Beginning one inside another opens a savepoint on that same connection.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"time"
)

// Tx is a transaction bound to a dialect. A nested one is a savepoint on the
// same driver transaction.
type Tx struct {
	tx DriverTx
	db *DB // where the dialect comes from
	// session is the transaction's own. Without it a hook registered here would
	// land on the list of whatever began it.
	session
	_done bool
	// _seq and _done back seq and done, which point at them. The transaction is
	// on the heap already, so pointing into it costs nothing where allocating
	// the two separately costs two. Never read them directly: nesting shares the
	// outermost's counter through the pointer, not through a copy of the value.
	_seq int
	// savepoint names the savepoint a nested transaction sits on, empty for the
	// outermost one.
	savepoint string
	seq       *int  // shared savepoint counter, so nested names stay unique
	done      *bool // whether this one has ended; a nested transaction gets its own
	// ctx is the context the transaction began with, kept for the hooks that
	// run at its end — Commit and Rollback take none of their own.
	ctx       context.Context
	startedAt time.Time
	// depth is how deep the transaction nests: 0 for the outermost, one more
	// for each savepoint. Hooks registered on it carry it, which is how a
	// rollback finds the ones that came with the work it is undoing.
	depth int
}

func newTx(tx DriverTx, db *DB, s session, ctx context.Context) *Tx {
	t := &Tx{tx: tx, db: db, session: s, ctx: ctx, startedAt: time.Now()}
	t.seq, t.done = &t._seq, &t._done
	return t
}

// Dialect returns the dialect of the DB the transaction came from.
func (tx *Tx) Dialect() Dialect { return tx.db.Dialect() }

// Driver returns the driver's own transaction.
func (tx *Tx) Driver() DriverTx { return tx.tx }

func (tx *Tx) executor() Executor { return tx.tx }

// Exec runs SQL as written in this transaction, in the driver's own
// placeholders.
func (tx *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	r := tx.runner()
	return r.exec(ctx, target{}, query, args)
}

// Query runs SQL as written in this transaction; the caller closes the rows.
func (tx *Tx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	r := tx.runner()
	return r.query(ctx, target{}, query, args)
}

// Begin starts a nested transaction, which is the same call that started this
// one — nesting is a savepoint underneath, and nothing about that surfaces:
//
//	tx, err := db.BeginTx(ctx, nil)
//	defer tx.Rollback()
//
//	inner, err := tx.BeginTx(ctx, nil)
//	defer inner.Rollback()   // rewinds to here, tx carries on
//	...
//	inner.Commit()           // keeps the inner work, tx still open
//	return tx.Commit()       // ends the transaction for real
//
// A savepoint cannot carry its own isolation level, so opts must be nil.
func (tx *Tx) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if opts != nil {
		return nil, errors.New("barm: a nested transaction inherits its isolation — pass nil opts")
	}
	*tx.seq++
	name := "barm_sp_" + strconv.Itoa(*tx.seq)
	err := tx.control(ctx, opSavepoint, "SAVEPOINT "+tx.ident(name))
	if err != nil {
		return nil, err
	}
	nested := *tx
	nested.savepoint = name
	nested._done = false
	nested.done = &nested._done // its own, while seq still points at the outermost's
	nested.ctx, nested.startedAt = ctx, time.Now()
	nested.depth = tx.depth + 1
	return &nested, nil
}

func (tx *Tx) Begin() (*Tx, error) { return tx.BeginTx(context.Background(), nil) }

// WithHook registers query hooks on this transaction alone, on top of the DB's.
// They see every query run through it from here on, and through any nested
// transaction started after this call.
func (tx *Tx) WithHook(hooks ...QueryHook) *Tx {
	// Clipped, so that appending here cannot write into an array this
	// transaction shares with a sibling — Begin copies the slice header.
	tx.hooks = append(slices.Clip(tx.hooks), hooks...)
	return tx
}

// WithTxHook registers transaction hooks on this transaction alone, on top of
// the DB's:
//
//	tx, err := db.BeginTx(ctx, nil)
//	tx.WithTxHook(barm.TxHook{AfterCommit: invalidate(keys)})
//
// They fire where the transaction really ends, so registering inside a nested
// transaction is how work decided there reports itself once the whole thing
// commits — and a nested rollback drops them with the work they described.
func (tx *Tx) WithTxHook(hooks ...TxHook) *Tx {
	for _, h := range hooks {
		*tx.txHooks = append(*tx.txHooks, txHook{TxHook: h, depth: tx.depth})
	}
	return tx
}

// control sends a savepoint statement under the query hooks, as any query is.
func (tx *Tx) control(ctx context.Context, op target, stmt string) error {
	ctx, ev := tx.startCall(ctx, op, "", stmt, nil)
	res, err := tx.tx.Exec(ctx, stmt, nil)
	finishQuery(ctx, tx.hooks, ev, res, 0, err)
	return err
}

func (tx *Tx) ident(name string) string {
	return string(tx.Dialect().AppendIdent(nil, name))
}

// Commit commits the transaction. A nested one keeps its work but leaves the
// enclosing transaction open.
func (tx *Tx) Commit() error {
	if *tx.done {
		return sql.ErrTxDone
	}
	ev := tx.txEvent()
	ctx, n, err := tx.beforeCommit(ev)
	if err != nil {
		tx.afterCommit(ctx, ev, n, err) // the transaction is still open
		return err
	}
	if tx.savepoint != "" {
		// A release that fails keeps the savepoint and the nested transaction
		// open, for the deferred Rollback to rewind to: in Postgres a failed
		// statement aborts everything, and rewinding is the only way back short
		// of ending the whole transaction.
		err = tx.control(context.WithoutCancel(tx.ctx), opReleaseSavepoint, "RELEASE SAVEPOINT "+tx.ident(tx.savepoint))
		if err == nil {
			*tx.done = true
			tx.handHooksUp()
		}
		return err
	}
	*tx.done = true
	c, qev := tx.startCall(context.WithoutCancel(ctx), opCommit, "", "COMMIT", nil)
	err = tx.tx.Commit(c)
	finishQuery(c, tx.hooks, qev, nil, 0, err)
	tx.afterCommit(ctx, ev, n, err)
	return err
}

// Rollback rolls the transaction back. A nested one undoes only its own work and
// leaves the enclosing transaction usable. Rolling back a finished transaction is
// a no-op returning sql.ErrTxDone, so the deferred call after a Commit or a
// Rollback costs nothing.
func (tx *Tx) Rollback() error {
	if *tx.done {
		return sql.ErrTxDone
	}
	ev := tx.txEvent()
	ctx := tx.beforeRollback(ev)

	var err error
	if tx.savepoint != "" {
		// Released once rewound, so a loop of failed nested transactions does
		// not leave a savepoint open per iteration, each nesting the next: past
		// 64 open subtransactions Postgres's per-backend cache overflows, and
		// every snapshot on the server slows down. A rewind that fails keeps the
		// savepoint, for another attempt.
		c := context.WithoutCancel(tx.ctx)
		err = tx.control(c, opRollbackSavepoint, "ROLLBACK TO SAVEPOINT "+tx.ident(tx.savepoint))
		if err == nil {
			*tx.done = true
			// The work is gone, so the hooks registered alongside it go too.
			tx.dropHooks()
			err = tx.control(c, opReleaseSavepoint, "RELEASE SAVEPOINT "+tx.ident(tx.savepoint))
		}
	} else {
		*tx.done = true
		c, qev := tx.startCall(context.WithoutCancel(ctx), opRollback, "", "ROLLBACK", nil)
		err = tx.tx.Rollback(c)
		finishQuery(c, tx.hooks, qev, nil, 0, err)
	}
	tx.afterRollback(ctx, ev, err)
	return err
}

func (tx *Tx) runner() runner { return runner{h: tx} }

func (tx *Tx) Select[T any]() *SelectQuery[T] { return newSelect[T](tx.runner()) }
func (tx *Tx) Insert[T any]() *InsertQuery[T] { return newInsert[T](tx.runner()) }
func (tx *Tx) Update[T any]() *UpdateQuery[T] { return newUpdate[T](tx.runner()) }
func (tx *Tx) Delete[T any]() *DeleteQuery[T] { return newDelete[T](tx.runner()) }

// NewRaw starts a hand-written query on this transaction.
func (tx *Tx) NewRaw(query string, args ...any) *RawQuery { return newRaw(tx.runner(), query, args) }

// Values renders rows as a VALUES list, for a CTE or a subquery. See
// [ValuesQuery].
func (tx *Tx) Values[T any](rows []T) *ValuesQuery[T] { return newValues(tx.runner(), rows) }

// Batch starts a batch inside this transaction, so the queued queries are part
// of it and roll back with it.
func (tx *Tx) Batch() *Batch { return newBatch(tx.tx, &tx.session, true, true) }
