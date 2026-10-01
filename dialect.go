// Dialect is what differs between the databases barm talks to: bind markers,
// identifier quoting, and which bits of grammar exist (RETURNING, = ANY,
// DEFAULT in VALUES, OFFSET without LIMIT). Postgres, MySQL and SQLite are
// built in. Builders ask the dialect instead of branching on the database.

package barm

import "strconv"

// Dialect abstracts the SQL flavor differences barm needs to know about.
type Dialect interface {
	Name() string
	// AppendPlaceholder appends the bind marker for the n-th (1-based) argument.
	AppendPlaceholder(b []byte, n int) []byte
	// AppendIdent appends a quoted identifier. Dotted names are quoted per part.
	AppendIdent(b []byte, name string) []byte
	// HasReturning reports whether INSERT/UPDATE ... RETURNING is supported.
	HasReturning() bool
	// HasAnyArray reports whether a list of values can be one array parameter,
	// as `col = ANY($1)`. Where it can, a query over many keys keeps one SQL
	// text however many there are, so the driver can reuse the statement; where
	// it cannot, the values are spelled out as `col IN (...)` and the text
	// changes with their number.
	HasAnyArray() bool
	// HasDefaultKeyword reports whether DEFAULT may stand in for a value inside
	// a VALUES list. Where it can, a column left at its zero renders as DEFAULT,
	// which is the column's actual default rather than barm's idea of it; where
	// it cannot — sqlite — the `default:` expression is written instead.
	HasDefaultKeyword() bool
	// NumberedArgs reports whether placeholders carry a position ($1) and can
	// therefore be referenced more than once. Positional dialects (?) cannot.
	NumberedArgs() bool
	// NoLimit is the LIMIT that means none, for an OFFSET on its own: sqlite
	// and MySQL take OFFSET only as part of a LIMIT clause. Empty where OFFSET
	// stands alone.
	NoLimit() string
}

type postgres struct{}
type mysql struct{}
type sqlite struct{}

var (
	Postgres Dialect = postgres{}
	MySQL    Dialect = mysql{}
	SQLite   Dialect = sqlite{}
)

func (postgres) Name() string { return "postgres" }
func (mysql) Name() string    { return "mysql" }
func (sqlite) Name() string   { return "sqlite" }

func (postgres) NumberedArgs() bool { return true }
func (mysql) NumberedArgs() bool    { return false }
func (sqlite) NumberedArgs() bool   { return false }

func (postgres) HasReturning() bool { return true }
func (mysql) HasReturning() bool    { return false }
func (sqlite) HasReturning() bool   { return true }

func (postgres) HasAnyArray() bool { return true }
func (mysql) HasAnyArray() bool    { return false }
func (sqlite) HasAnyArray() bool   { return false }

func (postgres) NoLimit() string { return "" }
func (mysql) NoLimit() string    { return "18446744073709551615" } // the largest BIGINT UNSIGNED, as MySQL's manual spells it
func (sqlite) NoLimit() string   { return "-1" }

func (postgres) HasDefaultKeyword() bool { return true }
func (mysql) HasDefaultKeyword() bool    { return true }
func (sqlite) HasDefaultKeyword() bool   { return false }

func (postgres) AppendPlaceholder(b []byte, n int) []byte {
	return strconv.AppendInt(append(b, '$'), int64(n), 10)
}
func (mysql) AppendPlaceholder(b []byte, _ int) []byte  { return append(b, '?') }
func (sqlite) AppendPlaceholder(b []byte, _ int) []byte { return append(b, '?') }

func (postgres) AppendIdent(b []byte, name string) []byte { return appendIdent(b, name, '"') }
func (sqlite) AppendIdent(b []byte, name string) []byte   { return appendIdent(b, name, '"') }
func (mysql) AppendIdent(b []byte, name string) []byte    { return appendIdent(b, name, '`') }

func appendIdent(b []byte, name string, q byte) []byte {
	b = append(b, q)
	for i := range len(name) {
		c := name[i]
		switch c {
		case '.':
			b = append(b, q, '.', q)
		case q:
			b = append(b, q, q)
		default:
			b = append(b, c)
		}
	}
	return append(b, q)
}
