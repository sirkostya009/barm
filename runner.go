// runner is what every builder ends in. It holds the handle a query runs on
// and its prepared statement name, and does the running: hooks around the
// call, the statement cache, the driver path that prepares and runs in one
// round trip, and the one/slice/seq terminals that scan into the result type.

package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
)

// runner carries everything a query needs to reach the database. It is embedded
// in every builder, so q.h stays directly reachable.
type runner struct {
	h    IDB    // the DB, transaction or connection the query was started on, and runs on
	name string // the prepared statement's, given to Prepare
}

// IDB returns the DB, transaction or connection the query was started on, so
// code handed only a query can start another on the same one. It comes wrapped
// in a Handle for the generic methods an interface cannot have yet.
func (r *runner) IDB() Handle { return Handle{r.h} }

// Dialect returns the dialect of the handle the query runs on.
func (r *runner) Dialect() Dialect { return r.h.Dialect() }

// buildOnly reports whether the query was started on a DB from NewBuilder,
// which renders SQL and has nothing to run it on: its *sql.DB is nil, and
// calling through it would panic rather than report ErrNoConn.
func (r *runner) buildOnly() bool {
	db, ok := r.h.(*DB)
	return ok && db.DB == nil
}

// ErrNoConn is returned by a DB that renders SQL but has nothing to run it on.
var ErrNoConn = errors.New("barm: this DB has no connection — it renders SQL only")

// stmt resolves the cached statement for this query, scoping it to the
// transaction when the query runs inside one. It returns nil when the query was
// not named with Prepare.
func (r *runner) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if r.name == "" {
		return nil, nil //nolint:nilnil // nil, nil means "not prepared", which callers check for via s != nil
	}
	stmts := r.h.sess().stmts
	if stmts == nil {
		return nil, fmt.Errorf("barm: Prepare(%q) needs a DB-backed query", r.name)
	}
	s, err := stmts.get(ctx, r.name, query)
	if err != nil {
		return nil, err
	}
	if tx, ok := r.h.(*Tx); ok {
		return tx.StmtContext(ctx, s), nil
	}
	return s, nil
}

func (r *runner) query(ctx context.Context, query string, args []any) (*sql.Rows, error) {
	ctx, ev := r.start(ctx, query, args)
	rows, err := r.rows(ctx, query, args)
	r.finish(ctx, ev, nil, err)
	return rows, err
}

// rows runs a query that reads rows, leaving the hooks to the caller.
func (r *runner) rows(ctx context.Context, query string, args []any) (*sql.Rows, error) {
	if r.buildOnly() {
		return nil, ErrNoConn
	}
	s, err := r.stmt(ctx, query) //nolint:sqlclosecheck // s is a cached, shared *sql.Stmt owned by stmtCache, not this call's to close
	if err != nil {
		return nil, err
	}
	if s != nil {
		return s.QueryContext(ctx, args...)
	}
	return r.h.QueryContext(ctx, query, args...)
}

func (r *runner) exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	if r.name != "" {
		res, ok, err := execViaDriver(ctx, *r, query, args)
		if ok {
			return res, err
		}
	}
	ctx, ev := r.start(ctx, query, args)
	if r.buildOnly() {
		r.finish(ctx, ev, nil, ErrNoConn)
		return nil, ErrNoConn
	}
	s, err := r.stmt(ctx, query) //nolint:sqlclosecheck // s is a cached, shared *sql.Stmt owned by stmtCache, not this call's to close
	if err != nil {
		r.finish(ctx, ev, nil, err)
		return nil, err
	}
	var res sql.Result
	if s != nil {
		res, err = s.ExecContext(ctx, args...)
	} else {
		res, err = r.h.ExecContext(ctx, query, args...)
	}
	r.finish(ctx, ev, res, err)
	return res, err
}

// Every builder ends here: it renders its own SQL and hands it over with what it
// knows about the result — the column names, and how many rows to expect. A
// write's rows need not be the type that was written, which is why the result
// type is the helper's own rather than the builder's.

// one takes the first row. It reads the columns' names off the result, as
// slice does, so any projection maps, and the hooks see the scan's error,
// ErrNoRows included. There is no *sql.Row here: it is these same rows behind
// a wrapper whose only addition is hiding the names.
func (r *runner) one[U any](ctx context.Context, query string, args []any) (U, error) {
	var v U
	if r.name != "" {
		ok, err := rowViaDriver(ctx, *r, query, args, &v)
		if ok {
			return v, err
		}
	}
	ctx, ev := r.start(ctx, query, args)
	rows, err := r.rows(ctx, query, args) //nolint:rowserrcheck // scanRow checks rows.Err() where it can hold one: when there is no row
	if err == nil {
		err = scanRow(rows, &v)
		cerr := rows.Close() //nolint:sqlclosecheck // closed here rather than deferred, since its error is the result's, as sql.Row reports it
		if err == nil {
			err = cerr
		}
	}
	r.finish(ctx, ev, nil, err)
	return v, err
}

// slice collects every row. The hint is how many to expect — a LIMIT for a
// select, the row count for an insert, one for a write keyed by primary key.
func (r *runner) slice[U any](ctx context.Context, query string, args []any, hint int64) ([]U, error) {
	if r.name != "" {
		var out []U
		ok, err := rowsViaDriver(ctx, *r, query, args, func(rows Rows) (err error) {
			out, err = scanSlice[U](rows, hint)
			return err
		})
		if ok {
			return out, err
		}
	}
	rows, err := r.query(ctx, query, args) //nolint:rowserrcheck // scanSlice checks rows.Err() itself before returning
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSlice[U](rows, hint)
}

// seq streams the rows. The query has already run by the time the first row
// arrives, so stopping early stops the reading, not the statement. A named one
// goes through database/sql even where the driver could run it: the driver
// holds the connection until the rows are read, and a loop over them that
// queried the same transaction would wait on itself. A build
// error is carried in and reported through the sequence, which is the only
// channel an iterator has.
func (r *runner) seq[U any](ctx context.Context, query string, args []any, err error) iter.Seq2[U, error] {
	return func(yield func(U, error) bool) {
		var zero U
		if err != nil { // the build failed; the sequence is how the caller hears it
			yield(zero, err)
			return
		}
		rows, err := r.query(ctx, query, args)
		if err != nil {
			yield(zero, err)
			return
		}
		defer rows.Close()

		// The loop is written out rather than delegated to a scan.go helper the
		// way slice delegates to scanSlice. Handing yield across another closure
		// stops the compiler inlining it, and measured 16% slower over a
		// thousand rows.
		sc, err := newScanner[U](rows)
		if err != nil {
			yield(zero, err)
			return
		}
		for rows.Next() {
			v, err := sc.scan(rows)
			if !yield(v, err) || err != nil {
				return
			}
		}
		err = rows.Err()
		if err != nil {
			yield(zero, err)
		}
	}
}

// errNoStmtDriver is what probing a connection reports when its driver cannot
// run a named statement itself. Nothing was sent.
var errNoStmtDriver = errors.New("barm: no statement driver")

// viaDriver runs a named statement through the driver of the connection the
// query belongs to — the transaction's, the held connection's, or one borrowed
// from the pool — where it is prepared in the same round trip as it first runs.
// ok is false when that driver cannot, found out before anything is sent, and
// the caller goes through database/sql instead.
//
// It and its callers take the runner by value: a closure capturing the query's
// own runner would put the whole query on the heap on every call, prepared or
// not.
func viaDriver(ctx context.Context, rn runner, query string, run func(StmtDriver) error) (bool, error) {
	stmts := rn.h.sess().stmts
	if stmts == nil || stmts.noDriver.Load() || rn.buildOnly() {
		return false, nil
	}
	_, err := stmts.entry(rn.name, query)
	if err != nil {
		return true, err
	}
	var conn *sql.Conn
	switch h := rn.h.(type) {
	case *Tx:
		conn = h.Conn.Conn
	case *Conn:
		conn = h.Conn
	case *DB:
		conn, err = h.DB.Conn(ctx)
		if err != nil {
			return true, err
		}
		defer conn.Close()
	default:
		return false, nil
	}
	err = conn.Raw(func(dc any) error {
		b, _ := batcherFor(dc)
		d, ok := b.(StmtDriver)
		if !ok {
			return errNoStmtDriver
		}
		return run(d)
	})
	if errors.Is(err, errNoStmtDriver) {
		stmts.noDriver.Store(true)
		return false, nil
	}
	return true, err
}

// rowsViaDriver runs a named statement through the driver and hands read its
// rows. The hook event finishes once the statement has run, before the rows are
// read, as it does on the database/sql path.
func rowsViaDriver(ctx context.Context, rn runner, query string, args []any, read func(Rows) error) (bool, error) {
	return viaDriver(ctx, rn, query, func(d StmtDriver) error {
		ctx, ev := rn.start(ctx, query, args)
		finished := false
		err := d.QueryStmt(ctx, rn.name, query, args, func(rows Rows) error {
			rn.finish(ctx, ev, nil, nil)
			finished = true
			return read(rows)
		})
		if !finished {
			rn.finish(ctx, ev, nil, err)
		}
		return err
	})
}

// rowViaDriver takes the single row of a named statement run through the
// driver. The hooks see the scan's error, ErrNoRows included, as one's do.
func rowViaDriver(ctx context.Context, rn runner, query string, args []any, v any) (bool, error) {
	return viaDriver(ctx, rn, query, func(d StmtDriver) error {
		ctx, ev := rn.start(ctx, query, args)
		err := d.QueryStmt(ctx, rn.name, query, args, func(rows Rows) error {
			return scanRow(rows, v)
		})
		rn.finish(ctx, ev, nil, err)
		return err
	})
}

// execViaDriver runs a named write through the driver.
func execViaDriver(ctx context.Context, rn runner, query string, args []any) (sql.Result, bool, error) {
	var res sql.Result
	ok, err := viaDriver(ctx, rn, query, func(d StmtDriver) error {
		ctx, ev := rn.start(ctx, query, args)
		er, err := d.ExecStmt(ctx, rn.name, query, args)
		if err == nil {
			res = rowsAffected(er.RowsAffected)
		}
		rn.finish(ctx, ev, res, err)
		return err
	})
	return res, ok, err
}
