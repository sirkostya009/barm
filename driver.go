// What barm needs from a database to run on it. barm ships two
// implementations: SQL, over database/sql, and pgxdriver.Pool, over pgx's own
// pool. Everything else in barm talks to these interfaces alone, so batching,
// prepared statements and the types a column can hold are the driver's to get
// right rather than whatever database/sql lets through.

package barm

import (
	"context"
	"database/sql"
)

// Executor runs statements on whatever a driver hands barm: the pool, one
// connection held out of it, or a transaction. Arguments arrive as barm bound
// them, one per placeholder, and the query is the dialect's own SQL.
type Executor interface {
	// Query runs a statement unprepared.
	Query(ctx context.Context, query string, args []any) (Rows, error)
	Exec(ctx context.Context, query string, args []any) (sql.Result, error)

	// QueryPrepared and ExecPrepared run query prepared: name is barm's promise
	// that it is worth keeping, bound to this one query text, and how the
	// driver prepares and keeps it is the driver's business.
	QueryPrepared(ctx context.Context, name, query string, args []any) (Rows, error)
	ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error)

	// SendBatch sends the queries together and hands read their results, in
	// order. A driver that cannot send them together returns ErrNoBatcher
	// before sending anything.
	SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error
}

// Pool is the database a DB runs on.
type Pool interface {
	Executor
	// Acquire takes one connection out of the pool, until its Release.
	Acquire(ctx context.Context) (DriverConn, error)
	Begin(ctx context.Context, opts *sql.TxOptions) (DriverTx, error)
	Close() error
}

// DriverConn is one connection held out of a Pool.
type DriverConn interface {
	Executor
	// Begin starts a transaction on this connection, which stays held after it.
	Begin(ctx context.Context, opts *sql.TxOptions) (DriverTx, error)
	Release() error
}

// DriverTx is a transaction, on one connection until it commits or rolls back.
type DriverTx interface {
	Executor
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
