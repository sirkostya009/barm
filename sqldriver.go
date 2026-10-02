// The database/sql implementation of Pool: SQLite, MySQL, and any other
// database/sql driver. database/sql cannot send statements together, so a
// batch either fails or, when SequentialBatches says so, runs one statement at
// a time on one connection.

package barm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
)

// SQLOption configures the database/sql pool.
type SQLOption func(*SQLPool)

// SequentialBatches makes a batch run its statements one at a time, on one
// connection, rather than fail with ErrNoBatcher. The results are the same as a
// pipelined batch's; the round trips are one per statement.
func SequentialBatches() SQLOption { return func(p *SQLPool) { p.seq = true } }

// SQL runs barm on a *sql.DB:
//
//	sqldb, err := sql.Open("sqlite", dsn)
//	db := barm.New(barm.SQL(sqldb), barm.SQLite)
//
// The *sql.DB stays the caller's to use directly, and DB.Close closes it.
func SQL(db *sql.DB, opts ...SQLOption) *SQLPool {
	p := &SQLPool{db: db, stmts: &stmtCache{p: db}}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// SQLPool is the Pool over a *sql.DB.
type SQLPool struct {
	db    *sql.DB
	stmts *stmtCache
	seq   bool
}

// DB returns the *sql.DB the pool runs on.
func (p *SQLPool) DB() *sql.DB { return p.db }

func (p *SQLPool) Query(ctx context.Context, query string, args []any) (Rows, error) {
	return p.db.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (p *SQLPool) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

func (p *SQLPool) QueryPrepared(ctx context.Context, name, query string, args []any) (Rows, error) {
	s, err := p.stmts.get(ctx, name, query) //nolint:sqlclosecheck // the cache owns it, and closes it on Release or Close
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (p *SQLPool) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	s, err := p.stmts.get(ctx, name, query) //nolint:sqlclosecheck // the cache owns it, and closes it on Release or Close
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

func (p *SQLPool) SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error {
	if !p.seq {
		return ErrNoBatcher
	}
	c, err := p.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	// Named statements are prepared on the borrowed connection, for this batch.
	stmts := &stmtCache{p: c}
	defer stmts.close()
	err = read(&seqReader{ctx: ctx, q: c, stmt: stmts.get, qs: qs})
	if err != nil {
		// The batch may have stopped inside a transaction it began, and
		// database/sql would hand the connection to the next caller still in
		// it: ErrBadConn has the pool close it instead.
		_ = c.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
}

func (p *SQLPool) Acquire(ctx context.Context) (DriverConn, error) {
	c, err := p.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	// Its own cache: a statement prepared on the pool would run wherever the pool
	// put it, which is the one thing holding a connection rules out.
	return &sqlConn{c: c, stmts: &stmtCache{p: c}, seq: p.seq}, nil
}

func (p *SQLPool) Begin(ctx context.Context, opts *sql.TxOptions) (DriverTx, error) {
	tx, err := p.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx, stmts: p.stmts, seq: p.seq}, nil
}

// Close closes every cached prepared statement, then the *sql.DB.
func (p *SQLPool) Close() error {
	err := p.stmts.close()
	cerr := p.db.Close()
	if cerr != nil {
		return cerr
	}
	return err
}

type sqlConn struct {
	c     *sql.Conn
	stmts *stmtCache
	seq   bool
}

func (c *sqlConn) Query(ctx context.Context, query string, args []any) (Rows, error) {
	return c.c.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (c *sqlConn) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	return c.c.ExecContext(ctx, query, args...)
}

func (c *sqlConn) QueryPrepared(ctx context.Context, name, query string, args []any) (Rows, error) {
	s, err := c.stmts.get(ctx, name, query) //nolint:sqlclosecheck // the cache owns it, and closes it on Release or Close
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (c *sqlConn) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	s, err := c.stmts.get(ctx, name, query) //nolint:sqlclosecheck // the cache owns it, and closes it on Release or Close
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

func (c *sqlConn) SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error {
	if !c.seq {
		return ErrNoBatcher
	}
	return read(&seqReader{ctx: ctx, q: c.c, stmt: c.stmts.get, qs: qs})
}

func (c *sqlConn) Begin(ctx context.Context, opts *sql.TxOptions) (DriverTx, error) {
	tx, err := c.c.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx, stmts: c.stmts, seq: c.seq}, nil
}

// Release closes the statements prepared on this connection, then gives it
// back to the pool.
func (c *sqlConn) Release() error {
	err := c.stmts.close()
	cerr := c.c.Close()
	if cerr != nil {
		return cerr
	}
	return err
}

// sqlTx runs a transaction. Its named statements are the ones of whatever it
// was begun on, bound to the transaction once per name: database/sql's binding
// re-prepares a statement the pool's cache did not prepare on this connection,
// and doing that on every run would cost a round trip and a statement apiece.
type sqlTx struct {
	tx    *sql.Tx
	stmts *stmtCache
	seq   bool
	mu    sync.Mutex
	own   map[string]*sql.Stmt
}

// stmt returns the transaction's statement for a name, binding it on first use.
// It closes with the transaction.
func (t *sqlTx) stmt(ctx context.Context, name, query string) (*sql.Stmt, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.own[name]; ok {
		return s, nil
	}
	ps, err := t.stmts.get(ctx, name, query)
	if err != nil {
		return nil, err
	}
	s := t.tx.StmtContext(ctx, ps)
	if t.own == nil {
		t.own = map[string]*sql.Stmt{}
	}
	t.own[name] = s
	return s, nil
}

func (t *sqlTx) Query(ctx context.Context, query string, args []any) (Rows, error) {
	return t.tx.QueryContext(ctx, query, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (t *sqlTx) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *sqlTx) QueryPrepared(ctx context.Context, name, query string, args []any) (Rows, error) {
	s, err := t.stmt(ctx, name, query) //nolint:sqlclosecheck // bound to the transaction, which closes it
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, args...) //nolint:rowserrcheck // the caller reads them and checks rows.Err()
}

func (t *sqlTx) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	s, err := t.stmt(ctx, name, query) //nolint:sqlclosecheck // bound to the transaction, which closes it
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, args...)
}

func (t *sqlTx) SendBatch(ctx context.Context, qs []BatchQuery, read func(BatchReader) error) error {
	if !t.seq {
		return ErrNoBatcher
	}
	return read(&seqReader{ctx: ctx, q: t.tx, stmt: t.stmt, qs: qs})
}

func (t *sqlTx) Commit(context.Context) error   { return t.tx.Commit() }
func (t *sqlTx) Rollback(context.Context) error { return t.tx.Rollback() }

// sqlQuerier is what a sequential batch runs on: a held connection or a
// transaction, so its statements share one session.
type sqlQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// seqReader runs a batch's statements one at a time, each when its result is
// read, a named one prepared. The rows of one are closed before the next runs,
// as database/sql requires of a single connection.
type seqReader struct {
	ctx  context.Context
	q    sqlQuerier
	stmt func(ctx context.Context, name, query string) (*sql.Stmt, error)
	qs   []BatchQuery
	i    int
	open *sql.Rows
}

func (r *seqReader) next() BatchQuery {
	if r.open != nil {
		r.open.Close()
		r.open = nil
	}
	q := r.qs[r.i]
	r.i++
	return q
}

func (r *seqReader) Rows() (Rows, error) {
	q := r.next()
	var rows *sql.Rows
	var err error
	if q.Name == "" {
		rows, err = r.q.QueryContext(r.ctx, q.Query, q.Args...) //nolint:rowserrcheck // the caller reads them; closed here before the next statement
	} else {
		var s *sql.Stmt
		s, err = r.stmt(r.ctx, q.Name, q.Query) //nolint:sqlclosecheck // its cache closes it
		if err == nil {
			rows, err = s.QueryContext(r.ctx, q.Args...) //nolint:rowserrcheck // the caller reads them; closed here before the next statement
		}
	}
	if err != nil {
		return nil, err
	}
	r.open = rows
	return rows, nil
}

func (r *seqReader) Exec() (ExecResult, error) {
	q := r.next()
	var res sql.Result
	var err error
	if q.Name == "" {
		res, err = r.q.ExecContext(r.ctx, q.Query, q.Args...)
	} else {
		var s *sql.Stmt
		s, err = r.stmt(r.ctx, q.Name, q.Query) //nolint:sqlclosecheck // its cache closes it
		if err == nil {
			res, err = s.ExecContext(r.ctx, q.Args...)
		}
	}
	if err != nil {
		return ExecResult{}, err
	}
	n, err := res.RowsAffected()
	return ExecResult{RowsAffected: n}, err
}
