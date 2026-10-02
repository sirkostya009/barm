// Conn: one connection taken out of the pool and held until Close. Queries
// built on it all run on that connection, which is what session state needs:
// SET, temporary tables, advisory locks.

package barm

import (
	"context"
	"database/sql"
	"slices"
)

// Conn is one connection taken out of the pool and held until Close. It builds
// everything a DB builds, and every query it builds runs on that one
// connection — which is what session state needs: SET LOCAL, temporary tables,
// advisory locks, anything that would be lost by landing on another connection.
//
//	c, err := db.Conn(ctx)
//	defer c.Close()
//
//	c.Exec(ctx, "CREATE TEMP TABLE seen (id bigint)")
//	users, err := c.Select[User]().Join("JOIN seen ON seen.id = u.id").Slice(ctx)
//
// SET LOCAL belongs inside a transaction begun on it: outside one, Postgres
// accepts it and ignores it.
type Conn struct {
	c DriverConn
	session
	db *DB // where the dialect comes from
}

// Dialect returns the dialect of the DB the connection came from.
func (c *Conn) Dialect() Dialect { return c.db.Dialect() }

// Driver returns the driver's own connection.
func (c *Conn) Driver() DriverConn { return c.c }

func (c *Conn) executor() Executor { return c.c }

func (c *Conn) runner() runner { return runner{h: c} }

func (c *Conn) Select[T any]() *SelectQuery[T] { return newSelect[T](c.runner()) }
func (c *Conn) Insert[T any]() *InsertQuery[T] { return newInsert[T](c.runner()) }
func (c *Conn) Update[T any]() *UpdateQuery[T] { return newUpdate[T](c.runner()) }
func (c *Conn) Delete[T any]() *DeleteQuery[T] { return newDelete[T](c.runner()) }

// NewRaw starts a hand-written query on this connection.
func (c *Conn) NewRaw(query string, args ...any) *RawQuery { return newRaw(c.runner(), query, args) }

// Values renders rows as a VALUES list, for a CTE or a subquery. See
// [ValuesQuery].
func (c *Conn) Values[T any](rows []T) *ValuesQuery[T] { return newValues(c.runner(), rows) }

// Exec runs SQL as written on this connection, in the driver's own placeholders.
func (c *Conn) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	r := c.runner()
	return r.exec(ctx, query, args)
}

// Query runs SQL as written on this connection; the caller closes the rows.
func (c *Conn) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	r := c.runner()
	return r.query(ctx, query, args)
}

// WithHook registers query hooks on this connection alone, on top of the DB's.
func (c *Conn) WithHook(hooks ...QueryHook) *Conn {
	c.hooks = append(slices.Clip(c.hooks), hooks...)
	return c
}

// WithTxHook registers transaction hooks on the transactions this connection
// begins, on top of the DB's.
func (c *Conn) WithTxHook(hooks ...TxHook) *Conn {
	for _, h := range hooks {
		*c.txHooks = append(*c.txHooks, txHook{TxHook: h})
	}
	return c
}

// Batch starts a batch on this connection, which is the one it will be sent on.
func (c *Conn) Batch() *Batch { return newBatch(c.c, &c.session, false, true) }

// BeginTx starts a transaction on this connection. The connection stays the
// caller's — Close is still the only thing that gives it back.
func (c *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.c.Begin(ctx, opts)
	if err != nil {
		return nil, err
	}
	return newTx(tx, c.db, c.own(), ctx), nil
}

// Begin starts a transaction on this connection with the default isolation, on
// the background context.
func (c *Conn) Begin() (*Tx, error) { return c.BeginTx(context.Background(), nil) }

// Close gives the connection back to the pool.
func (c *Conn) Close() error { return c.c.Release() }
