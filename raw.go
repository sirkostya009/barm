// RawQuery is SQL written by hand, turned into a Query. Query is closed to
// barm's own types, so this is how a statement no builder produces gets
// queued in a batch, run with hooks, or nested as a CTE or union branch. Its ?
// markers are rewritten to the dialect's.

package barm

import (
	"context"
	"database/sql"
)

// RawQuery is SQL written out by hand. Query is closed to barm's own types, so
// this is what carries a statement no builder produces: it is a Query like any
// other, and goes wherever one goes — queued in a batch, or nested as a CTE
// body or a union branch.
//
// It holds a dialect because that is what a bind marker needs. `?` takes the
// next argument, the same convention Where and Join use, and is rewritten to
// whatever the dialect binds with.
type RawQuery struct {
	runner
	f frag
}

func newRaw(r runner, query string, args []any) *RawQuery {
	return &RawQuery{runner: r, f: frag{sql: query, args: args}}
}

// Via returns a copy of the query that runs on h instead, leaving q bound to
// where it was started. See [SelectQuery.Via].
func (q *RawQuery) Via(h IDB) *RawQuery {
	c := *q
	c.h = h
	return &c
}

func (q *RawQuery) Build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

func (q *RawQuery) render(b *builder) error { //nolint:unparam // render's error is part of the Query interface
	b.hint(len(q.f.args))
	b.frag(q.f)
	return nil
}

func (q *RawQuery) argCount() int { return len(q.f.args) }

// Exec runs the statement.
func (q *RawQuery) Exec(ctx context.Context) (sql.Result, error) {
	query, args, err := q.Build()
	if err != nil {
		return nil, err
	}
	return q.exec(ctx, target{}, query, args)
}
