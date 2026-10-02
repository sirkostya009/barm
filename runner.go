// runner is what every builder ends in. It holds the handle a query runs on
// and its prepared statement name, and does the running: hooks around the
// call, the statement names, and the one/slice/seq terminals that scan into
// the result type.

package barm

import (
	"context"
	"database/sql"
	"errors"
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

func (r *runner) stmtName() string { return r.name }

// Dialect returns the dialect of the handle the query runs on.
func (r *runner) Dialect() Dialect { return r.h.Dialect() }

// ErrNoConn is returned by a DB that renders SQL but has nothing to run it on.
var ErrNoConn = errors.New("barm: this DB has no connection — it renders SQL only")

// executor is what the query runs on, or ErrNoConn for a DB from NewBuilder.
func (r *runner) executor() (Executor, error) {
	e := r.h.executor()
	if e == nil {
		return nil, ErrNoConn
	}
	return e, nil
}

// query runs a query and returns its rows, for the caller to read and close.
// The hooks finish once it has run, before the rows are read.
func (r *runner) query(ctx context.Context, query string, args []any) (Rows, error) {
	ctx, ev := r.start(ctx, query, args)
	rows, err := r.rows(ctx, query, args)
	r.finish(ctx, ev, nil, err)
	return rows, err
}

// rows runs a query, prepared when it was named, leaving the hooks to the
// caller.
func (r *runner) rows(ctx context.Context, query string, args []any) (Rows, error) {
	e, err := r.executor()
	if err != nil {
		return nil, err
	}
	if r.name == "" {
		return e.Query(ctx, query, args)
	}
	err = r.h.sess().names.bind(r.name, query)
	if err != nil {
		return nil, err
	}
	return e.QueryPrepared(ctx, r.name, query, args)
}

func (r *runner) exec(ctx context.Context, query string, args []any) (sql.Result, error) {
	ctx, ev := r.start(ctx, query, args)
	var res sql.Result
	e, err := r.executor()
	switch {
	case err != nil:
	case r.name == "":
		res, err = e.Exec(ctx, query, args)
	default:
		err = r.h.sess().names.bind(r.name, query)
		if err == nil {
			res, err = e.ExecPrepared(ctx, r.name, query, args)
		}
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
// ErrNoRows included.
func (r *runner) one[U any](ctx context.Context, query string, args []any) (U, error) {
	var v U
	ctx, ev := r.start(ctx, query, args)
	rows, err := r.rows(ctx, query, args)
	if err == nil {
		err = scanRow(rows, &v)
		cerr := rows.Close()
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
	rows, err := r.query(ctx, query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSlice[U](rows, hint)
}

// seq streams the rows. The query has already run by the time the first row
// arrives, so stopping early stops the reading, not the statement. A build
// error is carried in and reported through the sequence, which is the only
// channel an iterator has.
//
// The rows hold their connection until the loop ends, so a loop that queries
// the transaction it is reading from waits on itself on drivers that run one
// statement at a time per connection, pgx among them.
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
