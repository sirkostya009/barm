// Relations load a model's child rows alongside the query that reads the
// parents: one query per relation, keyed by the parents' join column, then
// grouped back onto them. Nested relations load breadth-first, so each depth
// costs one round trip across every branch, batched when the driver can.

package barm

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"
)

// reader turns one child result into its rows, grouped by the key they join on.
// It also hands back what renders the rows' own relations, if they have any,
// for the next depth.
type reader func(context.Context, Rows) (setter, func() ([]*pending, error), error)

// setter stores the rows for key into field, a pointer to the parent's field.
type setter func(field, key any)

// relation is one loaded relation: where its rows come from, and how to fetch
// them. prepare is built by Relation, which knows the child type as a type parameter
// — so the rows are scanned as that type rather than through reflection, and
// only reading the parent key and setting the parent field need reflect at all.
//
// It renders rather than runs, so that the relations of one query can be sent
// together: they all wait on the same parent keys and on nothing else.
type relation struct {
	field     string // the Go field name on the result type
	childType reflect.Type
	parentCol string
	childCol  string
	prepare   func(r runner, schema string, keys []any, keyType reflect.Type) (string, []any, reader, error)
}

// Relation loads a relation alongside the query, as a second statement keyed on what
// the first returned — one query per relation, not one per row.
//
//	authors, err := db.Select[Author]().
//		Relation[bookLite]("Books", func(q *barm.SelectQuery[bookLite]) *barm.SelectQuery[bookLite] {
//			return q.Where("title LIKE ?", "%Go%")
//		}).
//		SliceAs[authorLite](ctx)
//
// U is the type the rows are read into, and the field named on the result type
// must hold it. The table they come from is the model's: Author.Books is a
// []Book, and Book names the table — so the projection needs no table of its
// own, exactly as the result type of the outer query needs none. A query with no
// model behind it has nowhere to take the table from, and there U must name it.
//
// The fns run against the child query before it is sent, for filtering and
// ordering. The key predicate is added afterwards, so nothing in them can
// displace it. Calling Relation inside one loads a relation of the relation, a
// round trip deeper.
//
// Relations need the whole parent set before the children can be fetched, which
// is the one thing a stream does not have: Seq reports them rather than falling
// back to a query per row.
func (q *SelectQuery[T]) Relation[U any](field string, fns ...func(*SelectQuery[U]) *SelectQuery[U]) *SelectQuery[T] {
	rel, ok := q.model.rel(field)
	if !ok {
		q.fail(fmt.Errorf("barm: %s has no relation %q — tag the field `barm:\"rel:parent_col=child_col\"`", q.model.typ, field))
		return q
	}

	// The model names the table and carries any relations of its own; U names
	// the columns. The same split the outer query has, one level down — which is
	// what lets a relation of a relation find its table too.
	child, err := modelOfType(rel.typ)
	u, uerr := modelOf[U]()
	if child.table == "" {
		child, err = u, uerr
	}
	if child.table == "" {
		q.fail(fmt.Errorf("barm: relation %q names no table — %s or %s needs a BaseModel table tag",
			field, rel.typ, reflect.TypeFor[U]()))
		return q
	}
	err = cmp.Or(err, uerr)
	if err == nil {
		err = child.covers(u)
	}
	if err != nil {
		q.fail(err)
		return q
	}

	childCol := rel.childCol
	q.rels = append(q.rels, relation{
		field:     field,
		childType: reflect.TypeFor[U](),
		parentCol: rel.parentCol,
		childCol:  childCol,
		prepare: func(r runner, schema string, keys []any, keyType reflect.Type) (string, []any, reader, error) {
			r.name = "" // the key list changes the SQL, so it is not the parent's statement
			c := newSelect[U](r)
			c.model = child // table and relations from the model, columns from U
			c.schema = schema
			for _, fn := range fns {
				if fn != nil {
					c = fn(c)
				}
			}
			return prepareRelation(c, childCol, keys, keyType)
		},
	})
	return q
}

// prepareRelation renders the child query and hands back the reader for it.
func prepareRelation[U any](
	c *SelectQuery[U], childCol string, keys []any, keyType reflect.Type,
) (string, []any, reader, error) {
	// The key has to come back with every row to group by, whether or not the
	// projection asked for it, so it goes in front of whatever was being
	// selected — after the caller's fns, which therefore cannot displace it.
	//
	// Its own columns are qualified with its table, as the parent's are, so a
	// table the caller's fns join in cannot make them ambiguous.
	if len(c.cols) == 0 {
		for _, name := range c.proj.names {
			c.cols = append(c.cols, c.qualified(name))
			c.colName = append(c.colName, name)
		}
	}
	key := c.qualified(childCol)
	// A projection that selects the key already gets it moved to the front rather
	// than sent twice, and the row keeps it.
	selected := slices.Index(c.colName, childCol)
	if selected >= 0 {
		c.cols = slices.Delete(c.cols, selected, selected+1)
		c.colName = slices.Delete(c.colName, selected, selected+1)
	}
	c.cols = append([]frag{key}, c.cols...)
	c.colName = append([]string{childCol}, c.colName...)
	c.Where(keyPredicate(c.Dialect(), key.sql, keys, keyType))

	query, args, err := c.build()
	if err != nil {
		return "", nil, nil, err
	}
	read := func(_ context.Context, rows Rows) (setter, func() ([]*pending, error), error) {
		return groupRelation(c, rows, keyType, len(keys), selected >= 0)
	}
	return query, args, read, nil
}

// keyPredicate matches the child rows against the keys. One array parameter
// keeps the SQL the same for any number of them, so a driver that caches
// statements keeps hitting the same one; a dialect without arrays spells the
// values out instead.
func keyPredicate(d Dialect, name string, keys []any, keyType reflect.Type) (string, any) {
	if !d.HasAnyArray() {
		return name + " IN (?)", In(keys)
	}
	// A typed slice binds as one value: the driver sees an array.
	arr := reflect.MakeSlice(reflect.SliceOf(keyType), len(keys), len(keys))
	for i, k := range keys {
		arr.Index(i).Set(reflect.ValueOf(k))
	}
	return name + " = ANY(?)", arr.Interface()
}

// groupRelation scans the child rows and gathers each key's into one contiguous
// run of a single array, so every parent is handed a window of it rather than a
// slice of its own. Rows of their own load first, before anyone holds a window.
func groupRelation[U any](
	c *SelectQuery[U], rows Rows, keyType reflect.Type, nkeys int, inRow bool,
) (setter, func() ([]*pending, error), error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	// The key is column 0. When the row selects it too, it scans into the row and
	// is read back from there; otherwise it scans into a holder of its own.
	off := 1
	if inRow {
		off = 0
	}
	p, err := planFor(reflect.TypeFor[U](), cols[off:])
	if err != nil {
		return nil, nil, err
	}
	if inRow && p.paths[0] == nil {
		return nil, nil, fmt.Errorf("barm: %s maps no field to %q", reflect.TypeFor[U](), cols[0])
	}

	var (
		flat  []U
		gid   []int32 // the group of each row, in the order the rows came
		group = make(map[any]int32, nkeys)
		dest  = make([]any, len(cols))
		sink  any
		key   = reflect.New(keyType) // the parent's own type, so the keys compare
		hs    = p.holders()
	)
	convert, deref := false, false
	if inRow {
		ft := reflect.TypeFor[U]().FieldByIndex(p.paths[0]).Type
		if ft.Kind() == reflect.Pointer {
			ft, deref = ft.Elem(), true // a nullable key; NULL matches no parent, so it never comes back
		}
		if ft != keyType {
			if !ft.ConvertibleTo(keyType) {
				return nil, nil, fmt.Errorf("barm: key %q is %s on the row but %s on the parent", cols[0], ft, keyType)
			}
			convert = true
		}
	} else {
		dest[0] = key.Interface()
	}
	for rows.Next() {
		var zero U
		flat = append(flat, zero)
		rv := reflect.ValueOf(&flat[len(flat)-1]).Elem()
		for i, path := range p.paths {
			if path == nil {
				dest[i+off] = &sink
				continue
			}
			dest[i+off] = p.at(rv, i, hs)
		}
		err = rows.Scan(dest...)
		if err != nil {
			return nil, nil, err
		}
		var k any
		if inRow {
			f := fieldValue(rv, p.paths[0])
			if deref {
				f = f.Elem()
			}
			if convert {
				f = f.Convert(keyType) // same value as the parent's key type, so they compare
			}
			k = f.Interface()
		} else {
			k = key.Elem().Interface()
		}
		k = hashable(k)
		g, ok := group[k]
		if !ok {
			g = int32(len(group)) //nolint:gosec // group would exhaust memory long before hitting 2^31 entries
			group[k] = g
		}
		gid = append(gid, g)
	}
	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	// start[g]..start[g+1] is group g's run.
	// Rows usually arrive grouped already — an index scan on the key hands them
	// back in key order — and then they are in place. Otherwise, a counting sort.
	start := make([]int32, len(group)+1)
	grouped := true
	for i, g := range gid {
		start[g+1]++
		if i > 0 && g < gid[i-1] {
			grouped = false
		}
	}
	for i := 1; i < len(start); i++ {
		start[i] += start[i-1]
	}
	sorted := flat
	if !grouped {
		sorted = make([]U, len(flat))
		at := slices.Clone(start[:len(group)])
		for i, g := range gid {
			sorted[at[g]] = flat[i]
			at[g]++
		}
	}

	var next func() ([]*pending, error)
	if len(c.rels) > 0 {
		next = func() ([]*pending, error) { return c.prepareRelations(sorted) }
	}

	var ptrs []*U // for []*U fields: pointers into sorted, windowed the same way
	set := func(field, k any) {
		g, ok := group[k]
		if !ok {
			// No children. A slice becomes empty rather than nil, to match what
			// a query with no rows returns; a has-one field stays zero, since
			// there is nothing else it could be.
			switch f := field.(type) {
			case *[]U:
				*f = []U{}
			case *[]*U:
				*f = []*U{}
			}
			return
		}
		lo, hi := start[g], start[g+1]
		switch f := field.(type) {
		case *[]U:
			*f = sorted[lo:hi:hi] // capped, so an append cannot overwrite the next run
		case *[]*U:
			if ptrs == nil {
				ptrs = make([]*U, len(sorted))
				for i := range sorted {
					ptrs[i] = &sorted[i]
				}
			}
			*f = ptrs[lo:hi:hi]
		case *U:
			*f = sorted[lo]
		case **U:
			*f = &sorted[lo]
		}
	}
	return set, next, nil
}

// pending is one relation with its query rendered and its parents lined up.
type pending struct {
	query string
	args  []any
	read  reader // nil when there were no keys to fetch by
	set   setter
	next  func() ([]*pending, error) // renders the rows' own relations, if they have any
	apply func()                     // hands the rows to the parents
}

// loadRelations fetches every relation asked for, and theirs, and hands the
// rows to the parents they belong to.
func (q *SelectQuery[T]) loadRelations(ctx context.Context, rows []T) error {
	level, err := q.prepareRelations(rows)
	if err != nil {
		return err
	}
	return loadLevels(ctx, level, q.sendRelations)
}

// loadLevels fetches relations a depth at a time. Everything at one depth goes
// out together, whichever parent or branch it hangs off, and the next depth is
// rendered only once it is read: from the rows that came back, and after the
// connection a batch held is free again. The rows go to their parents deepest
// first, since a has-one field takes a copy of its row and must copy it with its
// own relations in.
func loadLevels(ctx context.Context, level []*pending, send func(context.Context, []*pending) error) error {
	var levels [][]*pending
	for len(level) > 0 {
		err := send(ctx, level)
		if err != nil {
			return err
		}
		levels = append(levels, level)
		var next []*pending
		for _, w := range level {
			if w.next == nil {
				continue
			}
			ws, err := w.next()
			if err != nil {
				return err
			}
			next = append(next, ws...)
		}
		level = next
	}
	for _, level := range slices.Backward(levels) {
		for _, w := range level {
			w.apply()
		}
	}
	return nil
}

// prepareRelations renders the query's relations over rows, one pending apiece.
// Every one of them waits on the same parent keys and on nothing else, which is
// what lets them go out together.
func (q *SelectQuery[T]) prepareRelations(rows []T) ([]*pending, error) {
	if len(rows) == 0 || len(q.rels) == 0 {
		return nil, nil
	}
	m, err := modelOf[T]()
	if err != nil {
		return nil, err
	}
	level := make([]*pending, 0, len(q.rels))
	for i := range q.rels {
		rel := &q.rels[i]
		target, ok := m.rel(rel.field)
		if !ok {
			return nil, fmt.Errorf("barm: %s has no relation %q to load into", m.typ, rel.field)
		}
		if target.typ != rel.childType {
			return nil, fmt.Errorf("barm: relation %q holds %s, but Relation was given %s",
				rel.field, target.typ, rel.childType)
		}
		key, ok := m.field(rel.parentCol)
		if !ok {
			return nil, fmt.Errorf("barm: relation %q joins on %q, which %s does not select",
				rel.field, rel.parentCol, m.typ)
		}
		if !q.selects(rel.parentCol) {
			return nil, fmt.Errorf("barm: relation %q joins on %q, which the query's columns leave out — add it with Column",
				rel.field, rel.parentCol)
		}

		// Each parent's key, and the distinct ones for the IN list. A nullable
		// key is compared by what it points at, and a nil one belongs to nothing.
		keyType := m.typ.FieldByIndex(key.index).Type
		nullable := keyType.Kind() == reflect.Pointer
		if nullable {
			keyType = keyType.Elem()
		}
		byParent := make([]any, len(rows))
		seen := make(map[any]struct{}, len(rows))
		keys := make([]any, 0, len(rows))
		for j := range rows {
			kv := fieldValue(reflect.ValueOf(&rows[j]).Elem(), key.index)
			if nullable {
				if kv.IsNil() {
					continue
				}
				kv = kv.Elem()
			}
			k := kv.Interface()
			h := hashable(k)
			byParent[j] = h
			if _, dup := seen[h]; !dup {
				seen[h] = struct{}{}
				keys = append(keys, k) // bound as it is, compared as the database compares it
			}
		}

		w := &pending{set: noRows}
		index := target.index
		w.apply = func() {
			for j := range rows {
				w.set(fieldAt(reflect.ValueOf(&rows[j]).Elem(), index), byParent[j])
			}
		}
		level = append(level, w)
		if len(keys) == 0 {
			continue // nothing to fetch by, and `IN ()` is not SQL everywhere
		}
		w.query, w.args, w.read, err = rel.prepare(q.runner, q.schema, keys, keyType)
		if err != nil {
			return nil, fmt.Errorf("barm: relation %q: %w", rel.field, err)
		}
	}
	return level, nil
}

// qualified is a column of this query's own table, written as the projection
// writes it: under the table's alias or name, unless a custom table expression
// owns the naming.
func (q *SelectQuery[T]) qualified(col string) frag {
	ref := ""
	if q.table == nil {
		ref = q.model.ref()
	}
	d := q.Dialect()
	b := make([]byte, 0, len(ref)+len(col)+5) // both quoted, and the dot between
	if ref != "" {
		b = append(d.AppendIdent(b, ref), '.')
	}
	return frag{sql: string(d.AppendIdent(b, col))}
}

// hashable makes a key one a map can hold. A []byte cannot be one, so it goes
// by its bytes instead — what the database compared it by. The string shares
// the slice's memory rather than copying it: keys are compared only while the
// relation loads, and every scan hands back bytes of their own.
func hashable(k any) any {
	if b, ok := k.([]byte); ok {
		return unsafe.String(unsafe.SliceData(b), len(b))
	}
	if t := reflect.TypeOf(k); t != nil && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		b := reflect.ValueOf(k).Bytes() // a named byte slice, json.RawMessage say, is no map key either
		return unsafe.String(unsafe.SliceData(b), len(b))
	}
	return k
}

// selects reports whether the query reads col. Only an explicit Column list can
// leave it out; a raw expression might be it, which barm cannot tell, so that
// gets the benefit of the doubt.
func (q *SelectQuery[T]) selects(col string) bool {
	if len(q.cols) == 0 {
		return true
	}
	for _, n := range q.colName {
		if n == "" || n == col || strings.HasSuffix(n, "."+col) {
			return true
		}
	}
	return false
}

// noRows is the setter of a relation that had no keys to fetch by: slices
// come out empty, the same as from a query that matched nothing.
func noRows(field, _ any) {
	if v := reflect.ValueOf(field).Elem(); v.Kind() == reflect.Slice {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
}

// sendRelations runs one depth's queries, in one batch where the connection can
// pipeline them. A single query is never worth batching, and neither is a driver
// that cannot pipeline — both fall back to a query apiece, which differs only in
// round trips.
func (r *runner) sendRelations(ctx context.Context, level []*pending) error {
	n := 0
	for _, w := range level {
		if w.read != nil {
			n++
		}
	}
	if e := r.h.executor(); n > 1 && e != nil {
		// A driver that cannot batch says so before sending anything, so falling
		// back cannot run a query twice.
		err := batchRelations(ctx, newBatch(e, r.h.sess(), false, false), level)
		if !errors.Is(err, ErrNoBatcher) {
			return err
		}
	}
	c := *r
	c.name = "" // a relation's query is its own, not the statement its parent was prepared as
	for _, w := range level {
		if w.read == nil {
			continue
		}
		rows, err := c.query(ctx, w.query, w.args)
		if err != nil {
			return err
		}
		w.set, w.next, err = w.read(ctx, rows)
		rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// batchRelations queues one depth's queries on batch and runs it.
func batchRelations(ctx context.Context, batch *Batch, level []*pending) error {
	for _, w := range level {
		if w.read == nil {
			continue
		}
		batch.queue("", w.query, w.args, nil, item{
			fail: func(error) {},
			read: func(br BatchReader) (sql.Result, error) {
				rows, err := br.Rows()
				if err != nil {
					return nil, err
				}
				defer rows.Close()
				w.set, w.next, err = w.read(ctx, rows)
				return nil, err
			},
		})
	}
	return batch.Run(ctx)
}
