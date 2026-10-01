// Package pgxdriver teaches barm to batch over pgx, and to run a prepared
// statement in one round trip, its Parse sent with its first run. Import it
// for its side effect and any DB opened with the pgx driver does both, with
// nothing to configure:
//
//	import (
//		_ "github.com/jackc/pgx/v5/stdlib"
//		_ "github.com/sirkostya009/barm/pgxdriver"
//	)
//
//	db := barm.New(sqldb, barm.Postgres)
//	b := db.Batch()
package pgxdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/sirkostya009/barm"
)

func init() {
	barm.RegisterDriver(func(driverConn any) (barm.Batcher, bool) {
		c, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return nil, false
		}
		return sender{c.Conn()}, true
	})
}

type sender struct{ s *pgx.Conn }

func (p sender) SendBatch(ctx context.Context, qs []barm.BatchQuery, read func(barm.BatchReader) error) error {
	var b pgx.Batch
	for _, q := range qs {
		b.Queue(q.Query, q.Args...)
	}
	br := p.s.SendBatch(ctx, &b)

	err := read(reader{br})
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	return err
}

// stmtsKey is where a connection keeps the statements barm has prepared on it:
// in the connection's own data, so they go when it does.
const stmtsKey = "barm.stmts"

// prepared returns the statements this connection has prepared, by name.
func prepared(pc *pgconn.PgConn) map[string]*pgconn.StatementDescription {
	data := pc.CustomData()
	m, _ := data[stmtsKey].(map[string]*pgconn.StatementDescription)
	if m == nil {
		m = map[string]*pgconn.StatementDescription{}
		data[stmtsKey] = m
	}
	return m
}

// QueryStmt runs a named statement and hands read its rows.
func (p sender) QueryStmt(ctx context.Context, name, query string, args []any, read func(barm.Rows) error) error {
	return p.stmt(ctx, name, query, args, func(rr *pgconn.ResultReader) error {
		rows := pgx.RowsFromResultReader(p.s.TypeMap(), rr)
		err := read(pgxRows{rows})
		if cerr := (pgxRows{rows}).Close(); err == nil {
			err = cerr
		}
		return err
	})
}

// ExecStmt runs a named write.
func (p sender) ExecStmt(ctx context.Context, name, query string, args []any) (barm.ExecResult, error) {
	var res barm.ExecResult
	err := p.stmt(ctx, name, query, args, func(rr *pgconn.ResultReader) error {
		tag, err := rr.Close()
		res.RowsAffected = tag.RowsAffected()
		return err
	})
	return res, err
}

// stmt runs a named statement in one round trip. On a connection that has not
// seen it, the Parse goes out in the same flush as the Bind and Execute, with
// the arguments as text of no declared type for the server to infer, as pgx's
// own exec mode sends them. After that it runs by name, the arguments encoded
// for the types the server settled on. use gets the result once it has run.
//
// A statement the server no longer has — DEALLOCATE, or a pooler resetting the
// session — fails at Bind, before any row, so it is prepared again on the spot.
func (p sender) stmt(ctx context.Context, name, query string, args []any, use func(*pgconn.ResultReader) error) error {
	known := prepared(p.s.PgConn())
	sd := known[name]
	if sd != nil && sd.SQL != query {
		return fmt.Errorf("barm: statement %q is prepared on this connection for another query", name)
	}
	err := p.stmtOnce(ctx, name, query, args, sd, use)
	var pgErr *pgconn.PgError
	if sd != nil && errors.As(err, &pgErr) && pgErr.Code == "26000" { // invalid_sql_statement_name
		delete(known, name)
		err = p.stmtOnce(ctx, name, query, args, nil, use)
	}
	return err
}

// stmtOnce sends the statement, prepared first when sd is nil, and hands use
// the result. An error from Bind comes back before use is called.
func (p sender) stmtOnce(
	ctx context.Context, name, query string, args []any, sd *pgconn.StatementDescription, use func(*pgconn.ResultReader) error,
) error {
	var eqb pgx.ExtendedQueryBuilder
	err := eqb.Build(p.s.TypeMap(), sd, args)
	if err != nil {
		return err
	}
	pc := p.s.PgConn()
	pipe := pc.StartPipeline(ctx)
	if sd == nil {
		pipe.SendPrepare(name, query, nil)
		pipe.SendQueryPrepared(name, eqb.ParamValues, eqb.ParamFormats, eqb.ResultFormats)
	} else {
		pipe.SendQueryStatement(sd, eqb.ParamValues, eqb.ParamFormats, eqb.ResultFormats)
	}
	err = pipe.Sync()
	if err == nil && sd == nil {
		var res any
		res, err = pipe.GetResults()
		if got, ok := res.(*pgconn.StatementDescription); ok && err == nil {
			got.Name, got.SQL = name, query // a pipeline's prepare reports only the types
			prepared(pc)[name] = got
		}
	}
	if err == nil {
		var res any
		res, err = pipe.GetResults()
		if rr, ok := res.(*pgconn.ResultReader); ok && err == nil {
			err = use(rr)
		}
	}
	if cerr := pipe.Close(); err == nil {
		err = cerr
	}
	return err
}

type reader struct{ br pgx.BatchResults }

func (r reader) Rows() (barm.Rows, error) {
	rows, err := r.br.Query()
	if err != nil {
		return nil, err
	}
	return pgxRows{rows}, nil
}

func (r reader) Exec() (barm.ExecResult, error) {
	tag, err := r.br.Exec()
	return barm.ExecResult{RowsAffected: tag.RowsAffected()}, err
}

// pgxRows presents pgx.Rows as barm.Rows.
type pgxRows struct{ pgx.Rows }

func (r pgxRows) Columns() ([]string, error) {
	fds := r.FieldDescriptions()
	names := make([]string, len(fds))
	for i, fd := range fds {
		names[i] = fd.Name
	}
	return names, nil
}

// Close reports the error pgx keeps on the rows, matching *sql.Rows.
func (r pgxRows) Close() error {
	r.Rows.Close()
	return r.Rows.Err()
}
