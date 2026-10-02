// Models: a struct's barm tags parsed into a table, columns, keys, defaults
// and relations. Embedded structs are flattened, and the result is cached per
// type, since every query on that type needs it.

package barm

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
)

// BaseModel carries the table metadata, and is how a struct names its table:
//
//	type User struct {
//		barm.BaseModel `barm:"table:users,alias:u"`
//	}
//
// Nothing is guessed from the Go type name. A struct without it has no table,
// and can only be used as a result type or with an explicit Table.
type BaseModel struct{}

var baseModelType = reflect.TypeFor[BaseModel]()

// field is a single mapped struct field.
type field struct {
	name  string // column name
	index []int
	// def is the column's default, from `default:expr`. A zero value in a
	// column that has one is taken to mean "let the database decide": the
	// column is left out of the INSERT so its own DEFAULT applies, and where
	// it cannot be left out the expression is written in place of the value.
	// An auto key has one implicitly, autoDefault.
	def  string
	pk   bool
	auto bool // filled by the database (identity / serial)
	// nullzero writes the zero value as NULL, and reads NULL back as the zero,
	// so a column that is nullable in the database needs no pointer in Go.
	nullzero bool
	// json stores the value encoded as JSON, and decodes it on the way back.
	json bool
	// array stores a slice as a Postgres array literal, and parses it back.
	array bool
	// scanonly is a column a query computes rather than one the table has.
	scanonly bool
	// skipupdate keeps the column out of an update built from a value, for
	// columns something else maintains. Naming it in Column still writes it.
	skipupdate bool
}

// relField is a field holding rows of another table rather than a column, from
// `rel:parent_col=child_col`. A slice field takes many rows, anything else one —
// the field's own type decides, when the rows are handed over.
//
// typ is the type the field holds — on a model that is the other model, which is
// where the child table comes from; on a projection it is the projection of it,
// which is where the child columns come from. The same split the query itself
// has, one level down.
type relField struct {
	name      string // the Go field name, which is what With takes
	index     []int
	typ       reflect.Type
	parentCol string
	childCol  string
}

// model is the cached mapping between a struct type and a table.
//
// The fields are stored inline; pks and byName point into them, which is what
// makes a field one object with three ways in rather than three copies. Nothing
// appends to fields once the model is built, so those pointers stay put.
type model struct {
	typ    reflect.Type
	table  string
	alias  string
	fields []field
	// read holds the scanonly fields: mapped when a result brings their column
	// back, and in no statement barm writes, reads by default or returns.
	read   []field
	pks    []*field
	byName map[string]*field
	names  []string
	rels   []relField
}

// rel returns the relation a Go field name declares.
func (m *model) rel(name string) (*relField, bool) {
	for i := range m.rels {
		if m.rels[i].name == name {
			return &m.rels[i], true
		}
	}
	return nil, false
}

// covers reports whether every column of other exists on m. A model with no
// fields of its own (a query started from a table name) vouches for anything.
func (m *model) covers(other *model) error {
	if len(m.fields) == 0 || m == other {
		return nil
	}
	for i := range other.fields {
		name := other.fields[i].name
		if _, ok := m.byName[name]; !ok {
			return fmt.Errorf("barm: %s has no column %q for %s", m.table, name, other.typ)
		}
	}
	return nil
}

// returningCols is a write's RETURNING clause for this model: what Returning was
// given, otherwise every column the model maps.
func (m *model) returningCols(d Dialect, given []frag) ([]frag, error) {
	if !d.HasReturning() {
		return nil, fmt.Errorf("barm: %s has no RETURNING", d.Name())
	}
	if len(given) > 0 {
		return given, nil
	}
	if len(m.fields) == 0 {
		return nil, fmt.Errorf("barm: %s names no columns to return — add Returning", m.typ)
	}
	out := make([]frag, len(m.fields))
	for i := range m.fields {
		out[i] = frag{sql: string(d.AppendIdent(nil, m.fields[i].name))}
	}
	return out, nil
}

// column returns the field an explicit column list on a write names.
func (m *model) column(name string) (*field, error) {
	f, ok := m.byName[name]
	switch {
	case !ok:
		return nil, errors.New("barm: unknown column " + name)
	case f.scanonly:
		return nil, fmt.Errorf("barm: column %q is scanonly, and never written", name)
	}
	return f, nil
}

// field returns the mapped field for a column name.
func (m *model) field(name string) (f *field, ok bool) {
	f, ok = m.byName[name]
	return
}

var modelCache sync.Map // reflect.Type -> *model

// modelOf returns the cached model for T, deriving it from struct tags. The
// model comes back even with an error, for a builder to carry to its terminal.
// Only a model without one is cached: a type that fails is a mistake in the
// code, fixed long before it matters how often it is derived again.
func modelOf[T any]() (*model, error) { return modelOfType(reflect.TypeFor[T]()) }

func modelOfType(t reflect.Type) (*model, error) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if v, ok := modelCache.Load(t); ok {
		return v.(*model), nil //nolint:forcetypeassert // modelCache only ever stores *model
	}
	m := &model{typ: t, byName: map[string]*field{}}
	if t.Kind() != reflect.Struct {
		return m, nil
	}
	m.collect(t, nil)
	var err error
	m.fields, err = promote(t, m.fields)
	if err != nil {
		return m, err
	}
	for _, f := range m.fields {
		if !f.array {
			continue
		}
		if f.json {
			return m, fmt.Errorf("barm: column %q cannot be both json and array", f.name)
		}
		if ft := deref(t.FieldByIndex(f.index).Type); ft.Kind() != reflect.Slice {
			return m, fmt.Errorf("barm: column %q is an array, but %s is not a slice", f.name, ft)
		}
	}
	if slices.ContainsFunc(m.fields, func(f field) bool { return f.scanonly }) {
		all := m.fields
		m.fields = make([]field, 0, len(all))
		for _, f := range all {
			if f.scanonly {
				m.read = append(m.read, f)
			} else {
				m.fields = append(m.fields, f)
			}
		}
		for i := range m.read {
			m.byName[m.read[i].name] = &m.read[i]
		}
	}
	m.names = make([]string, 0, len(m.fields))
	for i := range m.fields {
		f := &m.fields[i]
		m.byName[f.name] = f
		m.names = append(m.names, f.name)
		if f.pk {
			m.pks = append(m.pks, f)
		}
	}
	v, _ := modelCache.LoadOrStore(t, m)
	return v.(*model), nil //nolint:forcetypeassert // modelCache only ever stores *model
}

func (m *model) collect(t reflect.Type, index []int) {
	for i := range t.NumField() {
		sf := t.Field(i)
		tag, opts := parseTag(sf.Tag.Get("barm"))
		idx := append(append([]int(nil), index...), i)

		if sf.Type == baseModelType {
			m.table, m.alias = opts.table, opts.alias
			continue
		}
		if tag == "-" {
			continue // said out loud, and for an embedded struct as much as a column
		}
		// An embedded struct is flattened, but only when it is exported:
		// reflect cannot set a field reached through an unexported one, so
		// mapping those columns would panic at scan time.
		if sf.Anonymous && sf.IsExported() && deref(sf.Type).Kind() == reflect.Struct && !isScannable(sf.Type) {
			m.collect(deref(sf.Type), idx)
			continue
		}
		// A relation holds rows, not a value, so it is not a column — and the
		// columns it joins on are spelled out, like everything else here.
		if opts.rel != "" && sf.IsExported() {
			parent, child, ok := strings.Cut(opts.rel, "=")
			if !ok || parent == "" || child == "" {
				continue // `rel:parent_col=child_col`, or nothing
			}
			t := sf.Type
			if t.Kind() == reflect.Slice {
				t = t.Elem()
			}
			m.rels = append(m.rels, relField{
				name: sf.Name, index: idx, typ: deref(t),
				parentCol: parent, childCol: child,
			})
			continue
		}
		// A field is a column when the tag names one. Nothing is guessed from
		// the Go field name, so an untagged field is simply not mapped, and "-"
		// says the same thing out loud.
		if tag == "" || tag == "-" || !sf.IsExported() {
			continue
		}

		def := opts.def
		if opts.auto && def == "" {
			def = autoDefault
		}
		m.fields = append(m.fields, field{
			name: tag, index: idx, def: def, pk: opts.pk, auto: opts.auto, nullzero: opts.null, json: opts.json, array: opts.array, scanonly: opts.read, skipupdate: opts.keep,
		})
	}
}

// autoDefault is an auto key's default where a dialect cannot say DEFAULT:
// sqlite fills an integer primary key in for NULL.
const autoDefault = "NULL"

// promote keeps one field per column the way Go promotes embedded fields: the
// shallowest one wins, and two at the same depth are ambiguous. Mapping both
// would write the column twice and read it into whichever came last.
func promote(t reflect.Type, fields []field) ([]field, error) {
	out := make([]field, 0, len(fields))
	var err error
	for i := range fields {
		shadowed := false
		for j := range fields {
			if i == j || fields[j].name != fields[i].name {
				continue
			}
			switch {
			case len(fields[j].index) < len(fields[i].index):
				shadowed = true
			case len(fields[j].index) == len(fields[i].index) && err == nil:
				err = fmt.Errorf("barm: %s maps column %q twice at the same depth", t, fields[i].name)
			}
		}
		if !shadowed {
			out = append(out, fields[i])
		}
	}
	return out, err
}

// tagOpts is everything a barm tag can carry besides the column name. The set is
// closed and spelled out here, so an unknown option is ignored rather than
// silently meaning something.
type tagOpts struct {
	table string // BaseModel: the table name
	alias string // BaseModel: the alias to qualify columns with
	def   string // the column's default, written as SQL
	rel   string // `rel:parent_col=child_col`: rows of another table, not a column
	pk    bool
	auto  bool // filled by the database: autoincrement or identity
	null  bool // nullzero
	json  bool
	array bool
	read  bool // scanonly
	keep  bool // skipupdate
}

// parseTag splits `name,opt,opt:value`. The name is the leading part when it
// carries no value; everything after is an option.
func parseTag(tag string) (name string, opts tagOpts) {
	for part := range strings.SplitSeq(tag, ",") {
		if part == "" {
			continue
		}
		k, v, hasValue := strings.Cut(part, ":")
		if name == "" && !hasValue && opts == (tagOpts{}) {
			name = k
			continue
		}
		switch k {
		case "table":
			opts.table = v
		case "alias":
			opts.alias = v
		case "default":
			opts.def = v
		case "rel":
			opts.rel = v
		case "pk":
			opts.pk = true
		case "autoincrement", "identity":
			opts.auto = true
		case "nullzero":
			opts.null = true
		case "json":
			opts.json = true
		case "array":
			opts.array = true
		case "scanonly":
			opts.read = true
		case "skipupdate":
			opts.keep = true
		}
	}
	return name, opts
}

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}
