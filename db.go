// DB is the entry point: it wraps a *sql.DB with a dialect and starts queries.
// It also declares what the other handles share: the IDB interface Tx and Conn
// satisfy, the session holding statements and hooks, and Handle, which gives an
// IDB the generic methods an interface cannot declare.

package barm

import (
	"context"
	"database/sql"
	"iter"
	"slices"
	"time"
)

// Querier is database/sql's query surface, satisfied by *sql.DB, *sql.Tx and
// *sql.Conn — and by barm's own DB, Tx and Conn, which embed them. It is the
// least a caller has to accept to stay agnostic about which one it got. barm
// itself reads through QueryContext alone.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// session is what a DB, a transaction and a held connection all carry: where
// named statements live, and who is watching. A query reads it off the handle
// it runs on when it needs it, rather than keeping a copy.
//
// txHooks is a pointer to a list rather than a list because a nested transaction
// registers hooks that the outermost one has to fire. Begin copies the handle,
// so a plain slice would give the nested one its own header: appending there
// raises that header's length and the outer, whose Commit reads the list, sees
// nothing. Sharing the backing array does not help — length is what changes.
//
// Everywhere else that sharing is the thing to prevent, so every handle that can
// diverge is built with a list of its own. See own.
type session struct {
	stmts   *stmtCache
	hooks   []QueryHook
	txHooks *[]txHook
	dedup   bool
}

// own returns a copy of the session with a transaction-hook list of its own, so
// that registering on the copy cannot be seen by the handle it came from.
func (s *session) own() session {
	c := *s
	hooks := slices.Clip(*c.txHooks)
	c.txHooks = &hooks
	return c
}

// sess returns the session itself, which is how a query reaches the one of the
// handle it runs on. A Tx's own shadows that of the Conn inside it.
func (s *session) sess() *session { return s }

// DB wraps a *sql.DB with a dialect. The embedded *sql.DB stays reachable, so
// barm plugs into existing database/sql code instead of replacing it.
type DB struct {
	*sql.DB
	session
	dialect Dialect
}

// Dialect returns the dialect this builds queries for. A Conn and a Tx ask
// the DB they came from rather than keep a copy of their own.
func (db *DB) Dialect() Dialect { return db.dialect }

// Query is a built query: every builder satisfies it, whatever it returns, and
// so does Raw. The method set is closed to barm's own types.
//
// It is closed because a query has to be nestable. A CTE body or a union branch
// renders into the enclosing query's builder rather than building text of its
// own, which is what keeps one argument numbering across the two — splicing
// separately built SQL together would mean renumbering placeholders, and that
// means parsing SQL barm deliberately does not parse. Nothing outside the
// package can render into a builder, so nothing outside it can be a Query.
// Hand-written SQL goes through Raw, which can.
type Query interface {
	Build() (string, []any, error)
	render(*builder) error
	argCount() int
}

// TypedQuery is a query that knows what its rows are. Every builder satisfies
// the one for its own model type, so a helper can take a query returning users
// without caring whether it selects, inserts or deletes them:
//
//	func newest(ctx context.Context, q barm.TypedQuery[User]) (User, error) {
//		return q.One(ctx)
//	}
type TypedQuery[T any] interface {
	Query
	One(context.Context) (T, error)
	Slice(context.Context) ([]T, error)
	Seq(context.Context) iter.Seq2[T, error]
}

// IDB is whatever barm builds queries on: [*DB], [*Tx] and [*Conn]. Go allows
// generic methods only on concrete types, never in an interface, so an IDB
// starts queries through the [Handle] wrapping it:
//
//	func adults(ctx context.Context, h barm.IDB) ([]User, error) {
//		return barm.Handle{h}.Select[User]().Where("age >= ?", 18).Slice(ctx)
//	}
//
// which is how one function serves a DB, a transaction and a held connection
// alike. The method set is closed to barm's own types.
type IDB interface {
	Querier
	Dialect() Dialect
	// BeginTx starts a transaction on whatever the handle is: a DB takes a
	// connection for it, a Conn lends its own, and a Tx opens a savepoint.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
	Begin() (*Tx, error)
	runner() runner
	sess() *session
}

// todo: just add Select/Insert/Update/Delete once generic methods are allowed on interfaces

// Handle is a wrapper around [IDB] that adds generic methods.
//
// This is a temporary workaround until Go ships generic methods on interfaces. Wrap a [*DB], [*Tx] or [*Conn]
// in a Handle for consumers that don't care about what kind of type are they going to run queries off of.
type Handle struct{ IDB }

// Select starts a SELECT on h against T's table. It is db.Select[T]() for a
// caller holding a Handle rather than a concrete DB, Tx or Conn.
func (h Handle) Select[T any]() *SelectQuery[T] { return newSelect[T](h.runner()) }

// Insert starts an INSERT on h into T's table.
func (h Handle) Insert[T any]() *InsertQuery[T] { return newInsert[T](h.runner()) }

// Update starts an UPDATE on h of T's table.
func (h Handle) Update[T any]() *UpdateQuery[T] { return newUpdate[T](h.runner()) }

// Delete starts a DELETE on h from T's table.
func (h Handle) Delete[T any]() *DeleteQuery[T] { return newDelete[T](h.runner()) }

// NewRaw starts a hand-written query on whatever this handle wraps.
func (h Handle) NewRaw(query string, args ...any) *RawQuery { return newRaw(h.runner(), query, args) }

// Option configures a DB.
type Option func(*DB)

// WithArgDedup binds an argument once when the same string or []byte — the same
// backing array, not merely equal contents — reaches several placeholders in one
// query, saving the repeated payload on the wire. It applies only to numbered
// dialects, and never to prepared queries, whose SQL text must stay
// independent of its values.
//
// Each bind looks up the string arguments bound so far, so a wide bulk INSERT
// pays roughly a map lookup per string column.
//
// One placeholder has one type. Postgres takes it from the first use, so a
// value reaching columns of different types — a string compared to a text
// column and to an integer one — fails once merged (`integer = text`) where
// two placeholders would each have been typed on their own.
//
// Off by default: it makes the rendered SQL depend on how the caller happened to
// share memory. Prefer `?N` markers, which express the same reuse in the query
// itself.
func WithArgDedup() Option { return func(db *DB) { db.dedup = true } }

// WithHook registers a QueryHook. Hooks run in registration order before a
// query and in reverse after it.
func WithHook(hooks ...QueryHook) Option {
	return func(db *DB) { db.hooks = append(db.hooks, hooks...) }
}

// WithTxHook registers a TxHook, which observes commits and rollbacks rather
// than queries. Its fields are individually optional, so a hook that watches
// only one end of a transaction sets only that one.
func WithTxHook(hooks ...TxHook) Option {
	return func(db *DB) {
		for _, h := range hooks {
			*db.txHooks = append(*db.txHooks, txHook{TxHook: h})
		}
	}
}

// New wraps an existing *sql.DB.
func New(db *sql.DB, d Dialect, opts ...Option) *DB {
	out := &DB{DB: db,
		dialect: d, stmts: &stmtCache{p: db}, txHooks: new([]txHook)}
	for _, opt := range opts {
		opt(out)
	}
	return out
}

// NewBuilder returns a DB that renders SQL but executes nothing. Use it with a
// driver outside database/sql — the queries carry their own text and arguments,
// which is all such a driver needs.
func NewBuilder(d Dialect, opts ...Option) *DB { return New(nil, d, opts...) }

// Open opens a database via database/sql and wraps it.
func Open(driver, dsn string, d Dialect, opts ...Option) (*DB, error) {
	sqldb, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return New(sqldb, d, opts...), nil
}

func (db *DB) runner() runner { return runner{h: db} }

// Select starts a SELECT against T's table, scanning into T.
func (db *DB) Select[T any]() *SelectQuery[T] { return newSelect[T](db.runner()) }

// Insert starts an INSERT into T's table.
func (db *DB) Insert[T any]() *InsertQuery[T] { return newInsert[T](db.runner()) }

// Update starts an UPDATE of T's table.
func (db *DB) Update[T any]() *UpdateQuery[T] { return newUpdate[T](db.runner()) }

// Delete starts a DELETE from T's table.
func (db *DB) Delete[T any]() *DeleteQuery[T] { return newDelete[T](db.runner()) }

// NewRaw starts a query written out by hand:
//
//	db.NewRaw("DELETE FROM sessions WHERE seen < ?", cutoff).Exec(ctx)
//
//	b := db.Batch()
//	b.Exec(db.NewRaw("REFRESH MATERIALIZED VIEW daily"))
//
// Plain Raw is taken: Conn would shadow the *sql.Conn method of that name,
// which is the way down to the driver and means something else entirely.
//
// Reading rows is Select's job — Table and ColumnExpr take SQL barm does not
// parse, and they come back typed.
func (db *DB) NewRaw(query string, args ...any) *RawQuery { return newRaw(db.runner(), query, args) }

// Batch starts a batch on this DB. Which driver runs it is settled when it is
// sent, by probing the connection then — so there is nothing to configure and
// nothing here to go stale. Pass a Batcher to send it somewhere else entirely.
func (db *DB) Batch(on ...Batcher) *Batch {
	if len(on) > 0 {
		return newBatch(on[0], db.hooks)
	}
	if db.DB == nil {
		return newBatch(nil, db.hooks) // builder-only: no connection to batch on
	}
	return newBatch(&sqlDBBatcher{db.DB}, db.hooks)
}

// Close closes every cached prepared statement, then the underlying *sql.DB.
func (db *DB) Close() error {
	err := db.stmts.close()
	if db.DB == nil {
		return err // builder-only
	}
	cerr := db.DB.Close()
	if cerr != nil {
		return cerr
	}
	return err
}

// BeginTx starts a transaction. Pair it with `defer tx.Rollback()`: rolling back
// a committed transaction is a no-op returning sql.ErrTxDone.
//
// It shadows the embedded *sql.DB's, which would hand back a *sql.Tx — one that
// carries no dialect, no hooks and no builders, with nothing in the call to say
// so.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	// Taken out of the pool rather than left to database/sql, so the transaction
	// runs somewhere barm can reach: a *sql.Tx offers no way down to the driver,
	// which is what batching needs. Commit and Rollback give it back.
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		c.Close()
		return nil, err
	}
	// The DB's session, not the connection's: named statements stay in the
	// DB-wide cache, so one prepared in a transaction outlives it.
	t := &Tx{
		Tx: tx, Conn: c, session: db.own(), owns: true,
		ctx: ctx, startedAt: time.Now(), ended: make(chan struct{}),
	}
	t.seq, t.done = &t._seq, &t._done
	// database/sql rolls the transaction back when the context ends, but the
	// connection is barm's to return, so a transaction nobody finishes does not
	// hold one for the life of the DB.
	if ended := t.ended; ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				c.Close()
			case <-ended:
			}
		}()
	}
	return t, nil
}

func (db *DB) Begin() (*Tx, error) { return db.BeginTx(context.Background(), nil) }

// Conn takes a connection out of the pool. It is held until Close, which is the
// only thing that gives it back — an unreleased connection is gone until the DB
// closes.
func (db *DB) Conn(ctx context.Context) (*Conn, error) {
	if db.DB == nil {
		return nil, ErrNoConn
	}
	c, err := db.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	s := db.own()
	// Its own cache too: a statement prepared on the pool would run wherever the
	// pool put it, which is the one thing holding a connection rules out.
	s.stmts = &stmtCache{p: c}
	return &Conn{Conn: c, session: s, db: db}, nil
}
