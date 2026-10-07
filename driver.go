// What barm needs from a database to run on it. barm ships two
// implementations: SQL, over database/sql, and pgxdriver.Pool, over pgx's own
// pool. Everything else in barm talks to these interfaces alone, so batching,
// prepared statements and the types a column can hold are the driver's to get
// right rather than whatever database/sql lets through.

package barm

import (
	"context"
	"database/sql"
	"time"
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

// NativeJSON is implemented by a Pool whose driver encodes JSON from Go values
// itself, and by Rows that decode it into them. barm leaves the json tag to
// whichever reports true, other than writing a nil value as NULL.
type NativeJSON interface {
	NativeJSON() bool
}

// NativeArrays is NativeJSON for Postgres arrays and the array tag.
type NativeArrays interface {
	NativeArrays() bool
}

// Pool is the database a DB runs on.
type Pool interface {
	Executor
	// Dialect is the SQL flavor the database speaks, which a DB builds in.
	Dialect() Dialect
	// Acquire takes one connection out of the pool, until its Release.
	Acquire(ctx context.Context) (DriverConn, error)
	Begin(ctx context.Context, opts *sql.TxOptions) (DriverTx, error)
	// Ping checks the database is reachable, connecting if it has to.
	Ping(ctx context.Context) error
	// Underlying returns the driver's own pool, for a type assertion to reach
	// what barm does not wrap: a *sql.DB for SQL, a *pgxpool.Pool for pgxdriver.
	Underlying() any
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

// CallStats is what a driver tells the hooks about one call, beyond its result.
// A driver fills what it knows and leaves the rest zero.
type CallStats struct {
	// Wait is how long the call waited for a connection from the pool.
	Wait time.Duration
	// Described says the statement's parameter types had to be asked of the
	// database first, in a round trip of its own.
	Described bool
	// PID is the server process that ran the call, to find it in the server's
	// own logs and activity.
	PID uint32
}

// CallStatsFrom is where a driver reports on the call it was handed ctx for,
// and nil unless a hook watches it with WithCallStats, which is when to skip
// measuring. It checks
// ctx itself rather than walking up its values, so it costs a type check, and
// a driver asks with the ctx barm passed rather than one derived from it.
func CallStatsFrom(ctx context.Context) *CallStats {
	if c, ok := ctx.(*statsCtx); ok {
		return c.s
	}
	return nil
}

// statsCtx is the ctx barm hands a driver for a call a hook watches.
type statsCtx struct {
	context.Context
	s *CallStats
}
