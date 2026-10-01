// Tx is a transaction that holds its own connection from begin until commit or
// rollback, since a *sql.Tx cannot reach the driver and so cannot batch.
// Beginning one inside another opens a savepoint on the same connection.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"time"
)

// Tx is a transaction bound to a dialect. Statements prepared by name inside a
// transaction reuse the DB-wide cache, scoped to the transaction.
type Tx struct {
	*sql.Tx
	// Conn is what the transaction runs on, which is what batching inside one
	// needs: database/sql gives no way to it from a *sql.Tx. Queries still go to
	// the transaction — *sql.Tx is the shallower embedding, so it answers first.
	*Conn
	// session is the transaction's own, and shadows the one inside Conn. Without
	// it a hook registered here would land on the connection's list and fire for
	// the next transaction begun on it.
	session
	// owns says the transaction took the connection out of the pool itself and
	// closes it when it ends. One begun on a Conn only borrows it — that one
	// belongs to the caller, who is still holding it.
	owns  bool
	_done bool
	// _seq and _done back seq and done, which point at them. The transaction is
	// on the heap already, so pointing into it costs nothing where allocating
	// the two separately costs two. Never read them directly: nesting shares the
	// outermost's counter through the pointer, not through a copy of the value.
	_seq int
	// ended is closed when the outermost transaction finishes, to release the
	// goroutine watching its context.
	ended chan struct{}
	// savepoint names the savepoint a nested transaction sits on, empty for the
	// outermost one.
	savepoint string
	seq       *int  // shared savepoint counter, so nested names stay unique
	done      *bool // whether this one has ended; a nested transaction gets its own
	// ctx is the context the transaction began with, kept for the hooks that
	// run at its end — Commit and Rollback take none of their own. *sql.Tx
	// holds onto its context the same way.
	ctx       context.Context
	startedAt time.Time
	// depth is how deep the transaction nests: 0 for the outermost, one more
	// for each savepoint. Hooks registered on it carry it, which is how a
	// rollback finds the ones that came with the work it is undoing.
	depth int
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
	_, err := tx.ExecContext(ctx, "SAVEPOINT "+tx.ident(name))
	if err != nil {
		return nil, err
	}
	nested := *tx
	nested.owns = false // the connection belongs to the transaction this nests in
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
		_, err = tx.ExecContext(context.Background(), "RELEASE SAVEPOINT "+tx.ident(tx.savepoint))
		if err == nil {
			*tx.done = true
			tx.handHooksUp()
		}
		return err
	}
	*tx.done = true
	err = tx.Tx.Commit()
	tx.release()
	tx.afterCommit(ctx, ev, n, err)
	return err
}

// Rollback rolls the transaction back. A nested one undoes only its own work and
// leaves the enclosing transaction usable. Rolling back a finished transaction is
// a no-op returning sql.ErrTxDone, so the deferred call after a Commit costs
// nothing.
func (tx *Tx) Rollback() error {
	if *tx.done {
		return sql.ErrTxDone
	}
	ev := tx.txEvent()
	ctx := tx.beforeRollback(ev)

	var err error
	if tx.savepoint != "" {
		// A savepoint can be rewound to more than once, so this one stays open.
		_, err = tx.ExecContext(context.Background(), "ROLLBACK TO SAVEPOINT "+tx.ident(tx.savepoint))
		// The work is gone, so the hooks registered alongside it go too.
		tx.dropHooks()
	} else {
		*tx.done = true
		err = tx.Tx.Rollback()
		tx.release()
	}
	tx.afterRollback(ctx, ev, err)
	return err
}

// release gives the connection back, when it was this transaction's to take.
func (tx *Tx) release() {
	if tx.ended != nil {
		close(tx.ended) // the watcher reads its own copy, so this only ever runs once
	}
	if tx.owns {
		tx.Conn.Close()
	}
}

// Close shadows the connection's, which would give it back with the transaction
// still open on it. A transaction ends with Commit or Rollback.
func (tx *Tx) Close() error {
	return errors.New("barm: a transaction ends with Commit or Rollback, not Close")
}

func (tx *Tx) runner() runner { return runner{h: tx} }

func (tx *Tx) Select[T any]() *SelectQuery[T] { return newSelect[T](tx.runner()) }
func (tx *Tx) Insert[T any]() *InsertQuery[T] { return newInsert[T](tx.runner()) }
func (tx *Tx) Update[T any]() *UpdateQuery[T] { return newUpdate[T](tx.runner()) }
func (tx *Tx) Delete[T any]() *DeleteQuery[T] { return newDelete[T](tx.runner()) }

// NewRaw starts a hand-written query on this transaction.
func (tx *Tx) NewRaw(query string, args ...any) *RawQuery { return newRaw(tx.runner(), query, args) }

// Batch starts a batch inside this transaction. It borrows the transaction's own
// connection, so the queued queries are part of it and roll back with it.
func (tx *Tx) Batch(on ...Batcher) *Batch {
	if len(on) > 0 {
		return newBatch(on[0], tx.hooks)
	}
	return newBatch(sqlTxBatcher{sqlConnBatcher{tx.Conn.Conn}}, tx.hooks)
}
