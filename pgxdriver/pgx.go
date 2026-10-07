// Package pgxdriver runs barm on pgx's own pool, with no database/sql in
// between: pgx's codecs read and write every column type it knows — arrays,
// JSON, UUIDs — and batches pipeline in one round trip.
//
//	pool, err := pgxpool.New(ctx, dsn)
//	db := barm.New(pgxdriver.Pool(pool))
//
// A plain query is handed to pgx as it is, so how it runs is the pool's
// DefaultQueryExecMode: pgx's own statement cache unless configured otherwise.
// A batch, and a query named with barm's Prepare, go out in a single round trip
// whatever the mode: a named statement is parsed in the same flush that first
// runs it on a connection, and run by name after. A batch's plain queries are
// kept the same way when the mode is QueryExecModeCacheStatement, up to the
// connection's StatementCacheCapacity, and run unprepared otherwise, as pgx's
// QueryExecModeExec runs them.
//
// A kept statement's arguments are encoded for the types the server gives its
// parameters, as pgx's statement cache encodes them: encoded blind, a
// time.Time bound for a timestamp column stores a different value. The pool
// shares those types across its connections, so only a statement with
// arguments new to the whole pool costs a round trip to describe, once.
package pgxdriver

import (
	"container/list"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirkostya009/barm"
)

// Pool runs barm on p. DB.Close closes it.
func Pool(p *pgxpool.Pool) barm.Pool {
	mode := p.Config().ConnConfig.DefaultQueryExecMode
	return &pool{p: p, t: &types{}, typed: mode != pgx.QueryExecModeExec && mode != pgx.QueryExecModeSimpleProtocol}
}

type pool struct {
	p     *pgxpool.Pool
	t     *types
	typed bool // every argument is encoded for its parameter's type
}

// NativeJSON and NativeArrays report whether pgx encodes every argument for its
// parameter's type, which is when barm can leave JSON and arrays to it. In exec
// mode and the simple protocol a plain query's arguments go untyped, and pgx
// cannot tell a map is meant as JSON, nor encode a slice of slices at all.
func (p *pool) NativeJSON() bool   { return p.typed }
func (p *pool) NativeArrays() bool { return p.typed }

// acquire takes a connection for a call, reporting how long it took to get and
// whose it is when a hook watches.
func (p *pool) acquire(ctx context.Context, s *barm.CallStats) (*pgxpool.Conn, error) {
	if s == nil {
		return p.p.Acquire(ctx)
	}
	start := time.Now()
	c, err := p.p.Acquire(ctx)
	s.Wait = time.Since(start)
	if err == nil {
		s.PID = c.Conn().PgConn().PID()
	}
	return c, err
}

// Query leaves the connection to pgx's pool unless a hook watches, which takes
// it here, as the pool would, to see the wait for it.
func (p *pool) Query(ctx context.Context, query string, args []any) (barm.Rows, error) {
	s := barm.CallStatsFrom(ctx)
	if s == nil {
		rows, err := p.p.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return pgxRows{rows}, nil
	}
	c, err := p.acquire(ctx, s)
	if err != nil {
		return nil, err
	}
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		c.Release()
		return nil, err
	}
	return &pooledRows{pgxRows{rows}, c}, nil
}

func (p *pool) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	s := barm.CallStatsFrom(ctx)
	if s == nil {
		tag, err := p.p.Exec(ctx, query, args...)
		return result(tag.RowsAffected()), err
	}
	c, err := p.acquire(ctx, s)
	if err != nil {
		return nil, err
	}
	defer c.Release()
	tag, err := c.Exec(ctx, query, args...)
	return result(tag.RowsAffected()), err
}

func (p *pool) QueryPrepared(ctx context.Context, name, query string, args []any) (barm.Rows, error) {
	c, err := p.acquire(ctx, barm.CallStatsFrom(ctx))
	if err != nil {
		return nil, err
	}
	rows, err := queryNamed(ctx, c.Conn(), p.t, name, query, args)
	if err != nil {
		c.Release()
		return nil, err
	}
	rows.release = c
	return rows, nil
}

func (p *pool) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	c, err := p.acquire(ctx, barm.CallStatsFrom(ctx))
	if err != nil {
		return nil, err
	}
	defer c.Release()
	return execNamed(ctx, c.Conn(), p.t, name, query, args)
}

func (p *pool) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	c, err := p.acquire(ctx, barm.CallStatsFrom(ctx))
	if err != nil {
		return err
	}
	defer c.Release()
	return sendBatch(ctx, c.Conn(), p.t, qs, read)
}

func (p *pool) Acquire(ctx context.Context) (barm.DriverConn, error) {
	c, err := p.p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{c, p.t}, nil
}

func (p *pool) Begin(ctx context.Context, opts *sql.TxOptions) (barm.DriverTx, error) {
	o, err := txOptions(opts)
	if err != nil {
		return nil, err
	}
	s := barm.CallStatsFrom(ctx)
	if s == nil {
		t, err := p.p.BeginTx(ctx, o)
		if err != nil {
			return nil, err
		}
		return &tx{t: t, ts: p.t}, nil
	}
	c, err := p.acquire(ctx, s)
	if err != nil {
		return nil, err
	}
	t, err := c.BeginTx(ctx, o)
	if err != nil {
		c.Release()
		return nil, err
	}
	return &tx{t: t, ts: p.t, c: c}, nil
}

func (p *pool) Ping(ctx context.Context) error { return p.p.Ping(ctx) }

func (*pool) Dialect() barm.Dialect { return barm.Postgres }

func (p *pool) Underlying() any { return p.p }

func (p *pool) Close() error {
	p.p.Close()
	return nil
}

// conn is one connection held out of the pool.
type conn struct {
	c *pgxpool.Conn
	t *types
}

// pid reports the backend that runs a call when a hook watches it.
func pid(ctx context.Context, c *pgx.Conn) {
	if s := barm.CallStatsFrom(ctx); s != nil {
		s.PID = c.PgConn().PID()
	}
}

func (c *conn) Query(ctx context.Context, query string, args []any) (barm.Rows, error) {
	pid(ctx, c.c.Conn())
	rows, err := c.c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgxRows{rows}, nil
}

func (c *conn) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	pid(ctx, c.c.Conn())
	tag, err := c.c.Exec(ctx, query, args...)
	return result(tag.RowsAffected()), err
}

func (c *conn) QueryPrepared(ctx context.Context, name, query string, args []any) (barm.Rows, error) {
	pid(ctx, c.c.Conn())
	rows, err := queryNamed(ctx, c.c.Conn(), c.t, name, query, args)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *conn) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	pid(ctx, c.c.Conn())
	return execNamed(ctx, c.c.Conn(), c.t, name, query, args)
}

func (c *conn) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	pid(ctx, c.c.Conn())
	return sendBatch(ctx, c.c.Conn(), c.t, qs, read)
}

func (c *conn) Begin(ctx context.Context, opts *sql.TxOptions) (barm.DriverTx, error) {
	o, err := txOptions(opts)
	if err != nil {
		return nil, err
	}
	pid(ctx, c.c.Conn())
	t, err := c.c.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return &tx{t: t, ts: c.t}, nil
}

func (c *conn) Release() error {
	c.c.Release()
	return nil
}

// tx is a transaction, on the connection it was begun on.
type tx struct {
	t  pgx.Tx
	ts *types
	c  *pgxpool.Conn // taken from the pool for it, to give back when it ends
}

func (t *tx) Query(ctx context.Context, query string, args []any) (barm.Rows, error) {
	pid(ctx, t.t.Conn())
	rows, err := t.t.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgxRows{rows}, nil
}

func (t *tx) Exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	pid(ctx, t.t.Conn())
	tag, err := t.t.Exec(ctx, query, args...)
	return result(tag.RowsAffected()), err
}

func (t *tx) QueryPrepared(ctx context.Context, name, query string, args []any) (barm.Rows, error) {
	pid(ctx, t.t.Conn())
	rows, err := queryNamed(ctx, t.t.Conn(), t.ts, name, query, args)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (t *tx) ExecPrepared(ctx context.Context, name, query string, args []any) (sql.Result, error) {
	pid(ctx, t.t.Conn())
	return execNamed(ctx, t.t.Conn(), t.ts, name, query, args)
}

func (t *tx) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	pid(ctx, t.t.Conn())
	return sendBatch(ctx, t.t.Conn(), t.ts, qs, read)
}

func (t *tx) Commit(ctx context.Context) error {
	pid(ctx, t.t.Conn())
	err := t.t.Commit(ctx)
	t.release()
	return err
}

func (t *tx) Rollback(ctx context.Context) error {
	pid(ctx, t.t.Conn())
	err := t.t.Rollback(ctx)
	t.release()
	return err
}

// release gives back the connection taken for the transaction, as pgx's pool
// does with its own once one ends.
func (t *tx) release() {
	if t.c != nil {
		t.c.Release()
		t.c = nil
	}
}

func txOptions(opts *sql.TxOptions) (pgx.TxOptions, error) {
	var o pgx.TxOptions
	if opts == nil {
		return o, nil
	}
	switch opts.Isolation {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		o.IsoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		o.IsoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		o.IsoLevel = pgx.RepeatableRead
	case sql.LevelSerializable, sql.LevelLinearizable:
		o.IsoLevel = pgx.Serializable
	default:
		return o, fmt.Errorf("pgxdriver: isolation level %v is not supported", opts.Isolation)
	}
	if opts.ReadOnly {
		o.AccessMode = pgx.ReadOnly
	}
	return o, nil
}

// result is a statement's row count as a sql.Result. Postgres reports no
// insert id.
type result int64

func (r result) RowsAffected() (int64, error) { return int64(r), nil }
func (result) LastInsertId() (int64, error) {
	return 0, errors.New("pgxdriver: Postgres reports no last insert id — use RETURNING")
}

// types are the parameter and column types the pool has learned for its
// statements, by SQL. A connection meeting a statement for the first time
// parses it with those types and runs it in the same flush, rather than asking
// the server for them first.
type types struct {
	mu sync.RWMutex
	m  map[string]*pgconn.StatementDescription
}

// maxTypes bounds what the pool remembers. Past it an arbitrary statement is
// forgotten, and costs a describe the next time it is new to a connection.
const maxTypes = 4096

func (t *types) get(query string) *pgconn.StatementDescription {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.m[query]
}

func (t *types) put(sd *pgconn.StatementDescription) {
	d := &pgconn.StatementDescription{SQL: sd.SQL, ParamOIDs: sd.ParamOIDs, Fields: sd.Fields}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string]*pgconn.StatementDescription{}
	}
	if _, ok := t.m[d.SQL]; !ok && len(t.m) >= maxTypes {
		for k := range t.m {
			delete(t.m, k)
			break
		}
	}
	t.m[d.SQL] = d
}

func (t *types) drop(query string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, query)
}

// state is what barm keeps on a connection: the statements it has prepared
// there, by name and SQL.
type state struct {
	stmts map[stmtKey]*stmt
	// lru orders the statements kept for a batch's plain queries, most recently
	// run first; capacity bounds them, and is 0 when they are not kept.
	lru      list.List
	capacity int
	eqb      pgx.ExtendedQueryBuilder
	t        *types // of the pool the connection was last used through
}

type stmtKey struct{ name, query string }

type stmt struct {
	pgconn.StatementDescription
	formats []int16       // the result formats, nil until the columns are known
	typed   bool          // ParamOIDs are known, and the arguments encode for them
	pending bool          // not parsed on the connection yet
	lru     *list.Element // holds s; nil for a named one, which is never evicted
}

func stateOf(c *pgx.Conn) *state {
	d := c.PgConn().CustomData()
	st, _ := d["barm"].(*state)
	if st == nil {
		st = &state{stmts: map[stmtKey]*stmt{}}
		if cfg := c.Config(); cfg.DefaultQueryExecMode == pgx.QueryExecModeCacheStatement {
			st.capacity = cfg.StatementCacheCapacity
		}
		d["barm"] = st
	}
	return st
}

// newStmt is a statement new to the connection, typed if the pool knows it.
func newStmt(m *pgtype.Map, t *types, name, query string) *stmt {
	s := &stmt{pending: true}
	s.Name, s.SQL = serverName(name, query), query
	if d := t.get(query); d != nil {
		s.setTypes(m, d)
	}
	return s
}

func (s *stmt) setTypes(m *pgtype.Map, d *pgconn.StatementDescription) {
	s.ParamOIDs, s.Fields, s.typed = d.ParamOIDs, d.Fields, true
	s.formats = make([]int16, len(d.Fields))
	for i, f := range d.Fields {
		s.formats[i] = m.FormatCodeForOID(f.DataTypeOID)
	}
}

// encode encodes args for s's parameter types, or untyped, as pgx's exec mode
// does, for a statement not kept or whose types could not be had.
func (st *state) encode(m *pgtype.Map, s *stmt, args []any) error {
	if s != nil && s.typed {
		return st.eqb.Build(m, &s.StatementDescription, args)
	}
	return st.eqb.Build(m, nil, args)
}

func (st *state) drop(k stmtKey) {
	if s := st.stmts[k]; s != nil {
		if s.lru != nil {
			st.lru.Remove(s.lru)
		}
		delete(st.stmts, k)
	}
}

// serverName is the name a statement is prepared under. It covers the SQL as
// well as barm's name, as two DBs on one pool may bind a name differently.
func serverName(name, query string) string {
	h := sha256.New()
	h.Write([]byte(name))
	h.Write([]byte{0})
	h.Write([]byte(query))
	return "barm_" + hex.EncodeToString(h.Sum(nil)[:16])
}

// queued is the description to hand SendQueryStatement. pgconn keeps it until it
// reads the result, and sends a Describe when it has no fields, which a pending
// statement does not yet: parsed fills them in before the result is read, so
// pgconn gets its own copy and still expects the row description it asked for.
func queued(s *stmt) *pgconn.StatementDescription {
	if !s.pending {
		return &s.StatementDescription
	}
	sd := s.StatementDescription
	return &sd
}

// prepare queues a statement's Parse, with its parameter types when known.
// The Close ahead of it drops one left over under the same name — from a run
// that failed after its Parse, say — and is no error when there is none.
func prepare(p *pgconn.Pipeline, s *stmt) {
	p.SendDeallocate(s.Name)
	p.SendPrepare(s.Name, s.SQL, s.ParamOIDs)
}

// parsed reads a prepare's results and records the statement's types, on it
// and for the pool.
func parsed(p *pgconn.Pipeline, m *pgtype.Map, t *types, s *stmt) error {
	_, err := p.GetResults()
	if err != nil {
		return err
	}
	res, err := p.GetResults()
	if err != nil {
		return err
	}
	sd, ok := res.(*pgconn.StatementDescription)
	if !ok {
		return fmt.Errorf("pgxdriver: unexpected pipeline result %T", res)
	}
	sd.SQL = s.SQL
	s.setTypes(m, sd)
	s.pending = false
	t.put(sd)
	return nil
}

// readResult reads the next query's results off the pipeline.
func readResult(p *pgconn.Pipeline) (*pgconn.ResultReader, error) {
	res, err := p.GetResults()
	if err != nil {
		return nil, err
	}
	rr, ok := res.(*pgconn.ResultReader)
	if !ok {
		return nil, fmt.Errorf("pgxdriver: unexpected pipeline result %T", res)
	}
	return rr, nil
}

func readExec(p *pgconn.Pipeline) error {
	rr, err := readResult(p)
	if err != nil {
		return err
	}
	_, err = rr.Close()
	return err
}

const describeSavepoint = "barm_describe"

// describe parses the statements and learns their types, in a round trip
// ahead of the one that runs them. With guard, inside a transaction, it goes
// under a savepoint of its own, which a failure leaves for the caller to roll
// back to.
func describe(ctx context.Context, pc *pgconn.PgConn, m *pgtype.Map, t *types, ss []*stmt, guard bool) error {
	p := pc.StartPipeline(ctx)
	if guard {
		p.SendQueryParams("SAVEPOINT "+describeSavepoint, nil, nil, nil, nil)
	}
	for _, s := range ss {
		prepare(p, s)
	}
	if guard {
		p.SendQueryParams("RELEASE SAVEPOINT "+describeSavepoint, nil, nil, nil, nil)
	}
	err := p.Sync()
	if err == nil && guard {
		err = readExec(p)
	}
	for _, s := range ss {
		if err != nil {
			break
		}
		err = parsed(p, m, t, s)
	}
	if err == nil && guard {
		err = readExec(p)
	}
	if cerr := p.Close(); err == nil {
		err = cerr
	}
	return err
}

// queryNamed runs a named statement: by name when the connection has it,
// otherwise parsed in the same flush that runs it, after a describe of its
// own if it has arguments and the pool has never seen it.
func queryNamed(ctx context.Context, c *pgx.Conn, t *types, name, query string, args []any) (*namedRows, error) {
	st, m, pc := stateOf(c), c.TypeMap(), c.PgConn()
	st.t = t
	key := stmtKey{name, query}
	s := st.stmts[key]
	if s == nil {
		s = newStmt(m, t, name, query)
		if !s.typed && len(args) > 0 {
			if cs := barm.CallStatsFrom(ctx); cs != nil {
				cs.Described = true
			}
			err := describe(ctx, pc, m, t, []*stmt{s}, false)
			if err != nil {
				return nil, err
			}
			st.stmts[key] = s
		}
	}
	err := st.encode(m, s, args)
	if err != nil {
		return nil, err
	}
	r := &namedRows{st: st, key: key}
	if !s.pending {
		r.Rows = pgx.RowsFromResultReader(m, pc.ExecStatement(ctx, &s.StatementDescription, st.eqb.ParamValues, st.eqb.ParamFormats, s.formats))
		return r, nil
	}
	p := pc.StartPipeline(ctx)
	prepare(p, s)
	p.SendQueryStatement(queued(s), st.eqb.ParamValues, st.eqb.ParamFormats, s.formats)
	err = p.Sync()
	if err == nil {
		err = parsed(p, m, t, s)
	}
	var rr *pgconn.ResultReader
	if err == nil {
		rr, err = readResult(p)
	}
	if err != nil {
		_ = p.Close()
		t.drop(query)
		return nil, err
	}
	st.stmts[key] = s
	r.Rows, r.pipe = pgx.RowsFromResultReader(m, rr), p
	return r, nil
}

func execNamed(ctx context.Context, c *pgx.Conn, t *types, name, query string, args []any) (sql.Result, error) {
	rows, err := queryNamed(ctx, c, t, name, query, args)
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result(rows.CommandTag().RowsAffected()), err
}

// namedRows are a named statement's rows. Closing them ends the pipeline the
// statement was prepared in, if any, and gives back the connection they hold.
// A statement that failed is forgotten, here and by the pool, so the next run
// describes and prepares it again: a schema change or a DEALLOCATE costs one
// failed call, not every call after.
type namedRows struct {
	pgxRows
	st      *state
	key     stmtKey
	pipe    *pgconn.Pipeline
	release *pgxpool.Conn
	closed  bool
	err     error
}

func (r *namedRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	_ = r.Close()
	return false
}

func (r *namedRows) Close() error {
	if r.closed {
		return r.err
	}
	r.closed = true
	r.Rows.Close()
	err := r.Err()
	if r.pipe != nil {
		if perr := r.pipe.Close(); err == nil {
			err = perr
		}
	}
	if err != nil {
		r.st.drop(r.key)
		r.st.t.drop(r.key.query)
	}
	if r.release != nil {
		r.release.Release()
	}
	r.err = err
	return err
}

// sendBatch sends the queries in one round trip: one pipeline with a single
// Sync, which runs them in an implicit transaction unless one is already open.
// Postgres takes the messages in order, so a statement new to the connection
// is parsed right before its first run, after whatever the batch queued ahead
// of it — a SAVEPOINT included — and one evicted is closed after them all.
//
// A kept statement with arguments that the pool has never seen is described
// first, in a round trip of its own. Inside a transaction that goes under a
// savepoint, so a statement that fails to describe — one that cannot parse, or
// a table the batch itself creates — fails in its place in the batch instead,
// after whatever was queued ahead of it, and unprepared if it gets that far.
func sendBatch(ctx context.Context, c *pgx.Conn, t *types, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	st, m, pc := stateOf(c), c.TypeMap(), c.PgConn()
	r := &reader{m: m, t: t, steps: make([]step, len(qs))}
	var added []stmtKey
	var unknown []*stmt
	for i, q := range qs {
		if q.Name == "" && st.capacity == 0 {
			continue
		}
		key := stmtKey{q.Name, q.Query}
		s := st.stmts[key]
		switch {
		case s == nil:
			s = newStmt(m, t, q.Name, q.Query)
			if q.Name == "" {
				s.lru = st.lru.PushFront(s)
			}
			st.stmts[key] = s
			added = append(added, key)
			r.steps[i].prepare = true
			if !s.typed && len(q.Args) > 0 {
				unknown = append(unknown, s)
				r.steps[i].described = true
			}
		case s.lru != nil:
			st.lru.MoveToFront(s.lru)
		}
		r.steps[i].s = s
	}
	forget := func() {
		for _, k := range added {
			st.drop(k)
		}
	}

	guard := pc.TxStatus() == 'T'
	var rollBack bool
	if len(unknown) > 0 {
		err := describe(ctx, pc, m, t, unknown, guard)
		if _, ok := errors.AsType[*pgconn.PgError](err); ok {
			rollBack, err = guard, nil
		}
		if err != nil {
			forget()
			return err
		}
	}

	r.p = pc.StartPipeline(ctx)
	if rollBack {
		r.p.SendQueryParams("ROLLBACK TO SAVEPOINT "+describeSavepoint, nil, nil, nil, nil)
		r.p.SendQueryParams("RELEASE SAVEPOINT "+describeSavepoint, nil, nil, nil, nil)
	}
	for i, q := range qs {
		s := r.steps[i].s
		if r.steps[i].prepare {
			if s.pending {
				prepare(r.p, s)
			} else {
				r.steps[i].prepare = false
			}
		}
		err := st.encode(m, s, q.Args)
		if err != nil {
			forget()
			_ = r.p.Close()
			return fmt.Errorf("pgxdriver: batch query %d: %w", i, err)
		}
		if s == nil {
			r.p.SendQueryParams(q.Query, st.eqb.ParamValues, nil, st.eqb.ParamFormats, nil)
			continue
		}
		r.p.SendQueryStatement(queued(s), st.eqb.ParamValues, st.eqb.ParamFormats, s.formats)
	}
	for st.lru.Len() > st.capacity {
		s, _ := st.lru.Back().Value.(*stmt)
		r.p.SendDeallocate(s.Name)
		st.drop(stmtKey{"", s.SQL})
	}
	err := r.p.Sync()
	if err == nil && rollBack {
		err = readExec(r.p)
		if err == nil {
			err = readExec(r.p)
		}
	}
	if err == nil {
		err = read(r)
	}
	if cerr := r.p.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		forget()
		if r.i > 0 {
			q := qs[r.i-1]
			st.drop(stmtKey{q.Name, q.Query})
			if r.steps[r.i-1].s != nil {
				t.drop(q.Query)
			}
		}
	}
	return err
}

// step is how one query of a batch goes out.
type step struct {
	s         *stmt // nil for a statement not kept
	prepare   bool  // parsed in the batch's own pipeline
	described bool  // its types asked for in a round trip ahead of the batch
}

type reader struct {
	p     *pgconn.Pipeline
	m     *pgtype.Map
	t     *types
	steps []step
	i     int
}

func (r *reader) next() (*pgconn.ResultReader, error) {
	s := r.steps[r.i]
	r.i++
	if s.prepare {
		err := parsed(r.p, r.m, r.t, s.s)
		if err != nil {
			return nil, err
		}
	}
	return readResult(r.p)
}

// Described reports whether the statement whose result was read last had its
// types asked for ahead of the batch.
func (r *reader) Described() bool { return r.i > 0 && r.steps[r.i-1].described }

func (r *reader) Rows() (barm.Rows, error) {
	rr, err := r.next()
	if err != nil {
		return nil, err
	}
	return pgxRows{pgx.RowsFromResultReader(r.m, rr)}, nil
}

func (r *reader) Exec() (barm.ExecResult, error) {
	rr, err := r.next()
	if err != nil {
		return barm.ExecResult{}, err
	}
	tag, err := rr.Close()
	return barm.ExecResult{RowsAffected: tag.RowsAffected()}, err
}

// pooledRows hold the connection taken for them, and give it back once read to
// the end or closed, as pgx's pool does with its own.
type pooledRows struct {
	pgxRows
	c *pgxpool.Conn
}

func (r *pooledRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	_ = r.Close()
	return false
}

func (r *pooledRows) Close() error {
	r.Rows.Close()
	if r.c != nil {
		r.c.Release()
		r.c = nil
	}
	return r.Err()
}

// pgxRows presents pgx.Rows as barm.Rows.
type pgxRows struct{ pgx.Rows }

// NativeJSON and NativeArrays report that pgx decodes JSON and arrays itself:
// the result says what each column's type is.
func (pgxRows) NativeJSON() bool   { return true }
func (pgxRows) NativeArrays() bool { return true }

func (r pgxRows) Columns() ([]string, error) {
	fds := r.FieldDescriptions()
	names := make([]string, len(fds))
	for i, fd := range fds {
		names[i] = fd.Name
	}
	return names, nil
}

// Close reports the error pgx keeps on the rows, as database/sql's do.
func (r pgxRows) Close() error {
	r.Rows.Close()
	return r.Err()
}
