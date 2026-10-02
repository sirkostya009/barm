// ValuesQuery renders a slice of structs as a VALUES list, for a query to join
// against: as a CTE it names its columns, so UPDATE … FROM and DELETE … USING
// can match rows by them.

package barm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
)

// ValuesQuery is a VALUES list, one row per element, one column per field:
//
//	db.Update[Item]().With("data", db.Values(changes)).From("data").
//		Set("name = data.name").Where("items.id = data.id")
//
// As a CTE it renders `"data" ("id", "name") AS (VALUES ($1::bigint, $2::text),
// ($3, $4))`. Postgres reads an untyped parameter in a VALUES list as text, and
// the first row decides each column's type, so on Postgres that row is cast:
// to the `type:` tag's type, or else to the one the Go type maps to. A column
// of a type barm cannot map, and no tag, stays text.
type ValuesQuery[T any] struct {
	runner
	model *model
	rows  reflect.Value // the slice
	err   error
}

func newValues[T any](r runner, rows []T) *ValuesQuery[T] {
	m, err := modelOf[T]()
	return &ValuesQuery[T]{runner: r, model: m, rows: reflect.ValueOf(rows), err: err}
}

func (q *ValuesQuery[T]) Build() (string, []any, error) {
	b := q.builder()
	err := q.render(b)
	if err != nil {
		b.release()
		return "", nil, err
	}
	return b.done()
}

func (q *ValuesQuery[T]) argCount() int { return q.rows.Len() * len(q.model.fields) }

// render writes the VALUES list. A field's default is not applied: DEFAULT
// means something only in an INSERT.
func (q *ValuesQuery[T]) render(b *builder) error {
	if q.err != nil {
		return q.err
	}
	if q.rows.Len() == 0 {
		return errors.New("barm: Values has no rows")
	}
	if len(q.model.fields) == 0 {
		return errors.New("barm: Values has no columns")
	}
	b.hint(q.argCount())
	b.str("VALUES ")
	for i := range q.rows.Len() {
		if i > 0 {
			b.str(", ")
		}
		b.byte('(')
		snap := snapshot(q.rows.Index(i))
		for j := range q.model.fields {
			if j > 0 {
				b.str(", ")
			}
			f := &q.model.fields[j]
			b.placeholder(b.value(f, fieldValue(snap, f.index)))
			if i == 0 && f.cast != "" {
				b.b = b.d.AppendCast(b.b, f.cast)
			}
		}
		b.byte(')')
	}
	return nil
}

// withColumns writes the column list a CTE over the values is named with.
func (q *ValuesQuery[T]) withColumns(b *builder) {
	b.str(" (")
	for i := range q.model.fields {
		if i > 0 {
			b.str(", ")
		}
		b.ident(q.model.fields[i].name)
	}
	b.byte(')')
}

var (
	nullTypes = map[reflect.Type]string{
		reflect.TypeFor[sql.NullString]():  "text",
		reflect.TypeFor[sql.NullInt64]():   "bigint",
		reflect.TypeFor[sql.NullInt32]():   "integer",
		reflect.TypeFor[sql.NullInt16]():   "smallint",
		reflect.TypeFor[sql.NullByte]():    "smallint",
		reflect.TypeFor[sql.NullBool]():    "boolean",
		reflect.TypeFor[sql.NullFloat64](): "double precision",
		reflect.TypeFor[sql.NullTime]():    "timestamptz",
	}
	valuerType = reflect.TypeFor[driver.Valuer]()
)

// castOf is the SQL type a column of Go type t is cast to in a VALUES list:
// the `type:` tag's when there is one, otherwise the type t maps to, and empty
// for a type barm cannot map — a driver.Valuer of its own, a struct, a map.
func castOf(t reflect.Type, opts tagOpts) string {
	if opts.typ != "" {
		return opts.typ
	}
	if opts.json {
		return "jsonb"
	}
	return castOfType(deref(t))
}

func castOfType(t reflect.Type) string {
	if c, ok := nullTypes[t]; ok {
		return c
	}
	if t == timeType {
		return "timestamptz"
	}
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return "bytea"
	}
	if t.Implements(valuerType) || reflect.PointerTo(t).Implements(valuerType) {
		return "" // it decides what it is
	}
	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int8, reflect.Int16, reflect.Uint8:
		return "smallint"
	case reflect.Int32, reflect.Uint16:
		return "integer"
	case reflect.Int, reflect.Int64, reflect.Uint32:
		return "bigint"
	case reflect.Uint, reflect.Uint64:
		return "numeric" // past bigint's range
	case reflect.Float32:
		return "real"
	case reflect.Float64:
		return "double precision"
	case reflect.String:
		return "text"
	case reflect.Slice:
		// A Postgres array type has no dimensions of its own: bigint[] is a
		// [][]int64 as much as a []int64.
		elem := castOfType(deref(t.Elem()))
		if elem == "" || strings.HasSuffix(elem, "[]") {
			return elem
		}
		return elem + "[]"
	}
	return ""
}
