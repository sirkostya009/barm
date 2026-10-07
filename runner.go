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

// reach is what the query runs on, its Prepare name bound to it, or why it
// cannot reach the database: ErrNoConn for a DB from NewBuilder, or a name
// already bound to other SQL.
func (r *runner) reach(query string) (Executor, error) {
	e := r.h.executor()
	if e == nil {
		return nil, ErrNoConn
	}
	if r.name != "" {
		err := r.h.sess().names.bind(r.name, query)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

// query runs a query and returns its rows, for the caller to read and close.
// With a hook registered they come wrapped, so the hooks finish once they are
// closed, counting what was read.
func (r *runner) query(ctx context.Context, t target, query string, args []any) (Rows, error) {
	ctx, ev, e, err := r.start(ctx, t, query, args)
	if err != nil {
		return nil, err
	}
	rows, err := r.rows(ctx, e, query, args)
	if ev == nil {
		return rows, err
	}
	if err != nil {
		r.finish(ctx, ev, nil, 0, err)
		return nil, err
	}
	ev.responded()
	return &hookedRows{Rows: rows, ctx: ctx, hooks: r.h.sess().hooks, ev: ev}, nil
}

// rows runs a query on what start reached, prepared when it was named, leaving
// the hooks to the caller.
func (r *runner) rows(ctx context.Context, e Executor, query string, args []any) (Rows, error) {
	if r.name == "" {
		return e.Query(ctx, query, args)
	}
	return e.QueryPrepared(ctx, r.name, query, args)
}

func (r *runner) exec(ctx context.Context, t target, query string, args []any) (sql.Result, error) {
	ctx, ev, e, err := r.start(ctx, t, query, args)
	if err != nil {
		return nil, err
	}
	var res sql.Result
	if r.name == "" {
		res, err = e.Exec(ctx, query, args)
	} else {
		res, err = e.ExecPrepared(ctx, r.name, query, args)
	}
	r.finish(ctx, ev, res, 0, err)
	return res, err
}

// Every builder ends here: it renders its own SQL and hands it over with what it
// knows about the result — the column names, and how many rows to expect. A
// write's rows need not be the type that was written, which is why the result
// type is the helper's own rather than the builder's.

// one takes the first row. It reads the columns' names off the result, as
// slice does, so any projection maps, and the hooks see the scan's error,
// ErrNoRows included.
func (r *runner) one[U any](ctx context.Context, t target, query string, args []any) (U, error) {
	var v U
	ctx, ev, e, err := r.start(ctx, t, query, args)
	if err != nil {
		return v, err
	}
	rows, err := r.rows(ctx, e, query, args)
	ev.responded()
	if err == nil {
		err = closeRows(rows, scanRow(rows, &v))
	}
	n := int64(0)
	if err == nil {
		n = 1
	}
	r.finish(ctx, ev, nil, n, err)
	return v, err
}

// slice collects every row. The hint is how many to expect — a LIMIT for a
// select, the row count for an insert, one for a write keyed by primary key.
func (r *runner) slice[U any](ctx context.Context, t target, query string, args []any, hint int64) ([]U, error) {
	ctx, ev, e, err := r.start(ctx, t, query, args)
	if err != nil {
		return nil, err
	}
	rows, err := r.rows(ctx, e, query, args)
	ev.responded()
	var out []U
	if err == nil {
		out, err = scanSlice[U](rows, hint)
		err = closeRows(rows, err)
	}
	r.finish(ctx, ev, nil, int64(len(out)), err)
	return out, err
}

// seq streams the rows. The query has already run by the time the first row
// arrives, so stopping early stops the reading, not the statement. A build
// error is carried in and reported through the sequence, which is the only
// channel an iterator has.
//
// The rows hold their connection until the loop ends, so a loop that queries
// the transaction it is reading from waits on itself on drivers that run one
// statement at a time per connection, pgx among them. The hooks finish then
// too, the loop's own work included.
func (r *runner) seq[U any](ctx context.Context, t target, query string, args []any, err error) iter.Seq2[U, error] {
	if err != nil { // the build failed; the sequence is how the caller hears it
		return func(yield func(U, error) bool) {
			var zero U
			yield(zero, err)
		}
	}
	// The sequence holding all of the target would push it past its size class.
	tp := keepTarget(r.h.sess().hooks, t)
	return func(yield func(U, error) bool) {
		var zero U
		ctx, ev, e, err := r.start(ctx, tp.get(), query, args)
		if err != nil {
			yield(zero, err)
			return
		}
		rows, err := r.rows(ctx, e, query, args)
		if err != nil {
			r.finish(ctx, ev, nil, 0, err)
			yield(zero, err)
			return
		}
		ev.responded()
		var n int64
		defer func() { r.finish(ctx, ev, nil, n, closeRows(rows, err)) }()

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
			var v U
			v, err = sc.scan(rows)
			n++
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
