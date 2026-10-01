// The prepared statement cache. Prepare names a query, the name binds to one
// SQL text for good, and the statement is prepared once and reused. Reusing a
// name for different SQL is an error rather than a silent re-prepare.

package barm

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
)

// preparer is whatever a statement can be prepared on. A *sql.DB prepares on
// the pool, a *sql.Conn on its own connection — which is why a Conn keeps a
// cache of its own rather than sharing the DB's.
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// stmtCache holds named prepared statements for the lifetime of its preparer.
type stmtCache struct {
	p   preparer
	mu  sync.RWMutex
	all map[string]*namedStmt
	// noDriver says the connections behind this cache have no driver that runs
	// a named statement itself. It is found out on the first one, so the rest
	// go straight to database/sql.
	noDriver atomic.Bool
}

// namedStmt is prepared once it succeeds, not merely once: a failure is not
// kept, since it may belong to the caller that hit it — a cancelled context, a
// dropped connection, a table that did not exist yet — rather than to the name.
type namedStmt struct {
	query string
	mu    sync.Mutex // serializes attempts, so concurrent first uses prepare once
	stmt  atomic.Pointer[sql.Stmt]
}

// entry returns what a name is bound to, binding it to query on first use. A
// name pins one SQL text; reusing it for another query is a bug, not a silent
// re-prepare.
func (c *stmtCache) entry(name, query string) (*namedStmt, error) {
	c.mu.RLock()
	ns := c.all[name]
	c.mu.RUnlock()

	if ns == nil {
		c.mu.Lock()
		if ns = c.all[name]; ns == nil {
			ns = &namedStmt{query: query}
			if c.all == nil {
				c.all = map[string]*namedStmt{}
			}
			c.all[name] = ns
		}
		c.mu.Unlock()
	}
	if ns.query != query {
		return nil, fmt.Errorf("barm: prepared statement %q is already bound to a different query", name)
	}
	return ns, nil
}

// get returns the database/sql statement for a name, preparing it on first use.
func (c *stmtCache) get(ctx context.Context, name, query string) (*sql.Stmt, error) {
	ns, err := c.entry(name, query)
	if err != nil {
		return nil, err
	}
	if s := ns.stmt.Load(); s != nil {
		return s, nil
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if s := ns.stmt.Load(); s != nil {
		return s, nil
	}
	s, err := c.p.PrepareContext(ctx, ns.query) //nolint:sqlclosecheck // the cache owns it until close
	if err != nil {
		return nil, err
	}
	ns.stmt.Store(s)
	return s, nil
}

func (c *stmtCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	for _, ns := range c.all {
		if s := ns.stmt.Load(); s != nil {
			cerr := s.Close() //nolint:sqlclosecheck // closing every statement in a loop, not one in scope
			if cerr != nil && err == nil {
				err = cerr
			}
		}
	}
	clear(c.all)
	return err
}
