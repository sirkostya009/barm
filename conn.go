// Conn: one connection taken out of the pool and held until Close. Queries
// built on it all run on that connection, which is what session state needs:
// SET, temporary tables, advisory locks. It keeps a prepared statement cache of
// its own, since a statement prepared on the pool could run anywhere.

package barm

import (
	"context"
	"database/sql"
	"slices"
	"time"
)

// Conn is one connection taken out of the pool and held until Close. It builds
// everything a DB builds, and every query it builds runs on that one
// connection — which is what session state needs: SET LOCAL, temporary tables,
// advisory locks, anything that would be lost by landing on another connection.
//
//	c, err := db.Conn(ctx)
//	defer c.Close()
//
//	c.Exec(ctx, "SET LOCAL search_path = tenant_7")
//	users, err := c.Select[User]().Slice(ctx)
//
// The embedded *sql.Conn stays reachable, so Raw and PingContext are there too.
type Conn struct {
	*sql.Conn
	session
	db *DB // where the dialect comes from
}

// Dialect returns the dialect of the DB the connection came from.
func (c *Conn) Dialect() Dialect { return c.db.Dialect() }

func (c *Conn) runner() runner { return runner{h: c} }

func (c *Conn) Select[T any]() *SelectQuery[T] { return newSelect[T](c.runner()) }
func (c *Conn) Insert[T any]() *InsertQuery[T] { return newInsert[T](c.runner()) }
func (c *Conn) Update[T any]() *UpdateQuery[T] { return newUpdate[T](c.runner()) }
func (c *Conn) Delete[T any]() *DeleteQuery[T] { return newDelete[T](c.runner()) }

// NewRaw starts a hand-written query on this connection.
func (c *Conn) NewRaw(query string, args ...any) *RawQuery { return newRaw(c.runner(), query, args) }

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
func (c *Conn) Batch(on ...Batcher) *Batch {
	if len(on) > 0 {
		return newBatch(on[0], c.hooks)
	}
	return newBatch(sqlConnBatcher{c.Conn}, c.hooks)
}

// BeginTx starts a transaction on this connection. The connection stays the
// caller's — Close is still the only thing that gives it back — and because the
// transaction runs on a connection barm can reach, it is the one that can batch.
//
// It shadows the embedded *sql.Conn's, which would return a *sql.Tx that builds
// nothing.
func (c *Conn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	t := &Tx{
		Tx: tx, Conn: c, session: c.own(),
		ctx: ctx, startedAt: time.Now(),
	}
	t.seq, t.done = &t._seq, &t._done
	return t, nil
}

// Begin starts a transaction on this connection with the default isolation, on
// the background context.
func (c *Conn) Begin() (*Tx, error) { return c.BeginTx(context.Background(), nil) }

// Close closes the statements prepared on this connection, then gives it back
// to the pool.
func (c *Conn) Close() error {
	err := c.stmts.close()
	cerr := c.Conn.Close()
	if cerr != nil {
		return cerr
	}
	return err
}
