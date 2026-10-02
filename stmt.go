// Prepared statements. Prepare names a query, and the name binds to one SQL
// text for good, on every driver: reusing it for different SQL is an error
// rather than a silent re-prepare. Where the statement is prepared is the
// driver's business; database/sql's are cached here.

package barm

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
)

// names binds statement names to their SQL, one registry per DB.
type names struct {
	mu sync.RWMutex
	m  map[string]string
}

// bind binds name to query on first use, and checks it on every use after.
func (n *names) bind(name, query string) error {
	n.mu.RLock()
	bound, ok := n.m[name]
	n.mu.RUnlock()
	if !ok {
		n.mu.Lock()
		if bound, ok = n.m[name]; !ok {
			if n.m == nil {
				n.m = map[string]string{}
			}
			n.m[name], bound = query, query
		}
		n.mu.Unlock()
	}
	if bound != query {
		return fmt.Errorf("barm: prepared statement %q is already bound to a different query", name)
	}
	return nil
}

// preparer is whatever a database/sql statement can be prepared on. A *sql.DB
// prepares on the pool, a *sql.Conn on its own connection — which is why a held
// connection keeps a cache of its own rather than sharing the pool's.
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// stmtCache holds database/sql's prepared statements for the lifetime of its
// preparer, by name.
type stmtCache struct {
	p   preparer
	mu  sync.RWMutex
	all map[string]*namedStmt
}

// namedStmt is prepared once it succeeds, not merely once: a failure is not
// kept, since it may belong to the caller that hit it — a cancelled context, a
// dropped connection, a table that did not exist yet — rather than to the name.
type namedStmt struct {
	query string
	mu    sync.Mutex // serializes attempts, so concurrent first uses prepare once
	stmt  atomic.Pointer[sql.Stmt]
}

// get returns the statement for a name, preparing it on first use. The cache
// belongs to the pool, which several DBs may share, so the name is checked
// against the query here too: a DB's own registry cannot see the others'.
func (c *stmtCache) get(ctx context.Context, name, query string) (*sql.Stmt, error) {
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
	if s := ns.stmt.Load(); s != nil {
		return s, nil
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	if s := ns.stmt.Load(); s != nil {
		return s, nil
	}
	s, err := c.p.PrepareContext(ctx, query) //nolint:sqlclosecheck // the cache owns it until close
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
