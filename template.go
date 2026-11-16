package barm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"time"
	"unsafe"
)

var (
	errNamedArg          = errors.New("barm: a Param binds only through a Template")
	errTemplateRelations = errors.New("barm: relations do not load through a Template")
)

// Param stands where a Template's argument goes, bound on every call to the
// field of the template's P its tag names. See Template.
func Param(name string) any { return param(name) }

type param string

// Template is a query rendered once, its SQL kept with where each of its
// Params and IdentSlots sits, and run again with each filled from the fields
// of a P, by the names their tags give:
//
//	type ByAuthor struct {
//		Schema string `barm:"schema"`
//		Author int64  `barm:"author"`
//	}
//
//	var postsBy = qb.Select[Post]().Schema(barm.IdentSlot("schema")).
//		Where("p.author_id = ?", barm.Param("author")).Template[ByAuthor]()
//
//	posts, err := postsBy.Slice(ctx, ByAuthor{Schema: tenant, Author: id})
//
// A field binds as a model's column does, its json, array and nullzero tags
// included, and fields no name takes are left alone. A name P has no field
// for, or an IdentSlot whose field is not a string, fails every call. An
// insert's template writes P as its row: each column it writes binds from the
// field of P that column names, so a P that is the insert's own T works, as
// does one embedding it beside the fields its Params and IdentSlots take.
//
// Nothing of the builder is kept, so a builder made only to be templated is
// garbage once Template returns. P is read only while a call binds it, and
// stays on the caller's stack.
//
// Template renders the query as written, and TemplateAs as BuildAs does: a
// select narrowed to U's columns, a write returning them. That is the only
// shape it runs in: its OneAs reads U off whatever columns the SQL returns,
// and its One adds no LIMIT. A query with a Param fails to build any way but
// these.
//
// An IdentSlot's identifier is spliced into the SQL on every call. Each value
// is SQL of its own, so a prepared template runs one, and Prepare gives each
// another name. A batch takes the SQL its last query from the same template
// spliced again while the identifiers repeat.
//
// Relations do not load through a template, which keeps the query's SQL and
// not the query, and Exec scans no RETURNING rows back, since there are no
// values of the query's own to scan them into. A template never changes, so
// it is safe for any number of goroutines at once.
type Template[P, R any] struct {
	h      IDB
	name   string
	t      target
	query  string
	args   []any       // as rendered, nil wherever a field of P goes
	binds  []fieldBind // where in args each field of P goes
	idents []fieldBind // where in query each IdentSlot sits, by byte offset, and the field filling it
	ptyp   reflect.Type
	err    error
}

// fieldBind is where a field of P goes. A field read through an offset is one
// of the predeclared types boxed as they are; anything else goes through
// reflect, as a model's column does.
type fieldBind struct {
	at   int
	off  uintptr
	kind bindKind
	f    *field
}

type bindKind uint8

const (
	bindReflect bindKind = iota
	bindString
	bindInt64
	bindInt
	bindInt32
	bindBool
	bindFloat64
	bindTime
	bindBytes
)

var bindKinds = map[reflect.Type]bindKind{
	reflect.TypeFor[string]():    bindString,
	reflect.TypeFor[int64]():     bindInt64,
	reflect.TypeFor[int]():       bindInt,
	reflect.TypeFor[int32]():     bindInt32,
	reflect.TypeFor[bool]():      bindBool,
	reflect.TypeFor[float64]():   bindFloat64,
	reflect.TypeFor[time.Time](): bindTime,
	reflect.TypeFor[[]byte]():    bindBytes,
}

// rowField marks where a column of an insert template's row binds.
type rowField struct{ f *field }

// newTemplate keeps a rendered query and maps its Params, IdentSlots and row
// columns to the fields of P.
func newTemplate[P, R any](r runner, t target, query string, args []any, err error) *Template[P, R] {
	var idents identSlotsError
	if errors.As(err, &idents) || errors.Is(err, errNamedArg) {
		err = nil
	}
	tp := &Template[P, R]{h: r.h, name: r.name, t: t, query: query, args: args, ptyp: reflect.TypeFor[P](), err: err}
	if err == nil {
		tp.err = tp.index(idents)
	}
	return tp
}

func (t *Template[P, R]) index(idents identSlotsError) error {
	var m *model
	fieldOf := func(name, what string) (*field, error) {
		if name == "" {
			return nil, fmt.Errorf("barm: a template's %s has no name", what)
		}
		if m == nil {
			var err error
			m, err = modelOfType(t.ptyp)
			if err != nil {
				return nil, err
			}
		}
		f, ok := m.field(name)
		if !ok {
			return nil, fmt.Errorf("barm: %s has no field for the template's %s %q", t.ptyp, what, name)
		}
		return f, nil
	}
	var params []string
	for at, v := range t.args {
		var f *field
		var err error
		switch a := v.(type) {
		case param:
			f, err = fieldOf(string(a), "Param")
			params = append(params, string(a))
		case rowField:
			f, err = fieldOf(a.f.name, "column")
		default:
			continue
		}
		if err != nil {
			return err
		}
		t.args[at] = nil
		t.binds = append(t.binds, t.bindOf(f, at))
	}
	for _, id := range idents {
		f, err := fieldOf(id.name, "IdentSlot")
		switch {
		case err != nil:
			return err
		case slices.Contains(params, id.name):
			return fmt.Errorf("barm: the template's %q is both a Param and an IdentSlot", id.name)
		case t.ptyp.FieldByIndex(f.index).Type.Kind() != reflect.String:
			return fmt.Errorf("barm: the template's IdentSlot %q takes a string, and %s's field is not one", id.name, t.ptyp)
		}
		b := t.bindOf(f, id.at)
		if b.off != unreachable {
			b.kind = bindString // any string kind reads as a string
		}
		t.idents = append(t.idents, b)
	}
	return nil
}

// unreachable is the offset of a field behind an embedded pointer.
const unreachable = ^uintptr(0)

// bindOf is how the field f of P binds at at.
func (t *Template[P, R]) bindOf(f *field, at int) fieldBind {
	b := fieldBind{at: at, f: f}
	ty := t.ptyp
	for _, i := range f.index {
		if ty.Kind() == reflect.Pointer {
			b.off = unreachable
			return b
		}
		sf := ty.Field(i)
		b.off += sf.Offset
		ty = sf.Type
	}
	if !f.json && !f.array && !f.nullzero {
		b.kind = bindKinds[ty]
	}
	return b
}

// Prepare returns the template running as the prepared statement name, or
// unprepared for "", leaving t as it was. A name binds to one SQL text, so a
// template with an IdentSlot takes a name of its own for each value it runs
// prepared:
//
//	postsBy.Prepare("posts_by_" + tenant).Slice(ctx, ByAuthor{Schema: tenant, ...})
func (t *Template[P, R]) Prepare(name string) *Template[P, R] {
	c := *t
	c.name = name
	return &c
}

// Via returns the template running on h. Without it a call runs on the
// handle in the ctx it is given, see WithIDB, or else where the template was
// started, which for one built on a NewBuilder DB is nowhere.
func (t *Template[P, R]) Via(h IDB) Run[P, R] { return Run[P, R]{t, h} }

// One reads the first row as R, or ErrNoRows.
func (t *Template[P, R]) One(ctx context.Context, args P) (R, error) {
	return Run[P, R]{t: t}.OneAs[R](ctx, args)
}

// Slice reads every row as R.
func (t *Template[P, R]) Slice(ctx context.Context, args P) ([]R, error) {
	return Run[P, R]{t: t}.SliceAs[R](ctx, args)
}

// Seq streams every row as R. See SelectQuery.Seq.
func (t *Template[P, R]) Seq(ctx context.Context, args P) iter.Seq2[R, error] {
	return Run[P, R]{t: t}.SeqAs[R](ctx, args)
}

// OneAs reads the first row as U, mapping the columns the query returns by
// name, or ErrNoRows.
func (t *Template[P, R]) OneAs[U any](ctx context.Context, args P) (U, error) {
	return Run[P, R]{t: t}.OneAs[U](ctx, args)
}

// SliceAs reads every row as U, mapping the columns the query returns by name.
func (t *Template[P, R]) SliceAs[U any](ctx context.Context, args P) ([]U, error) {
	return Run[P, R]{t: t}.SliceAs[U](ctx, args)
}

// SeqAs streams every row as U, mapping the columns the query returns by name.
func (t *Template[P, R]) SeqAs[U any](ctx context.Context, args P) iter.Seq2[U, error] {
	return Run[P, R]{t: t}.SeqAs[U](ctx, args)
}

// Exec runs the query for its row count.
func (t *Template[P, R]) Exec(ctx context.Context, args P) (sql.Result, error) {
	return Run[P, R]{t: t}.Exec(ctx, args)
}

// Build returns the template's SQL with args bound, encoded for the handle the
// template was started on: it takes no ctx to find another in.
func (t *Template[P, R]) Build(args P) (string, []any, error) {
	return t.bind(t.h.sess(), noescape(unsafe.Pointer(&args)), nil)
}

// Run is a Template running on the handle Via gave it.
type Run[P, R any] struct {
	t *Template[P, R]
	h IDB
}

// One reads the first row as R, or ErrNoRows.
func (r Run[P, R]) One(ctx context.Context, args P) (R, error) { return r.OneAs[R](ctx, args) }

// Slice reads every row as R.
func (r Run[P, R]) Slice(ctx context.Context, args P) ([]R, error) { return r.SliceAs[R](ctx, args) }

// Seq streams every row as R. See SelectQuery.Seq.
func (r Run[P, R]) Seq(ctx context.Context, args P) iter.Seq2[R, error] {
	return r.SeqAs[R](ctx, args)
}

// OneAs reads the first row as U, mapping the columns the query returns by
// name, or ErrNoRows.
func (r Run[P, R]) OneAs[U any](ctx context.Context, args P) (U, error) {
	h := r.on(ctx)
	query, out, err := r.t.bind(h.sess(), noescape(unsafe.Pointer(&args)), nil)
	if err != nil {
		var zero U
		return zero, err
	}
	rn := runner{h, r.t.name}
	return rn.one[U](ctx, r.t.t, query, out)
}

// SliceAs reads every row as U, mapping the columns the query returns by name.
func (r Run[P, R]) SliceAs[U any](ctx context.Context, args P) ([]U, error) {
	h := r.on(ctx)
	query, out, err := r.t.bind(h.sess(), noescape(unsafe.Pointer(&args)), nil)
	if err != nil {
		return nil, err
	}
	rn := runner{h, r.t.name}
	return rn.slice[U](ctx, r.t.t, query, out, 0)
}

// SeqAs streams every row as U, mapping the columns the query returns by name.
func (r Run[P, R]) SeqAs[U any](ctx context.Context, args P) iter.Seq2[U, error] {
	h := r.on(ctx)
	query, out, err := r.t.bind(h.sess(), noescape(unsafe.Pointer(&args)), nil)
	rn := runner{h, r.t.name}
	return rn.seq[U](ctx, r.t.t, query, out, err)
}

// Exec runs the query for its row count.
func (r Run[P, R]) Exec(ctx context.Context, args P) (sql.Result, error) {
	h := r.on(ctx)
	query, out, err := r.t.bind(h.sess(), noescape(unsafe.Pointer(&args)), nil)
	if err != nil {
		return nil, err
	}
	rn := runner{h, r.t.name}
	return rn.exec(ctx, r.t.t, query, out)
}

// on is the handle the query runs on: Via's, else the one in ctx, else the
// template's.
func (r Run[P, R]) on(ctx context.Context) IDB {
	if r.h != nil {
		return r.h
	}
	if h := IDBFrom(ctx); h != nil {
		return h
	}
	return r.t.h
}

// In returns the template queueing in b.
func (t *Template[P, R]) In(b *Batch) Queue[P, R] { return Queue[P, R]{t, b} }

// Queue is a Template queueing in the batch In gave it. Its arguments are
// encoded for the batch's handle, and args is read before a call returns.
type Queue[P, R any] struct {
	t *Template[P, R]
	b *Batch
}

// One queues the template with args and reads its first row as R, or ErrNoRows.
func (q Queue[P, R]) One(args P) *BatchResult[R] {
	query, out, err := q.t.bind(q.b.s, noescape(unsafe.Pointer(&args)), q.last())
	return queueRow[R](q.b, q.t.t, q.t.name, query, out, err)
}

// Slice queues the template with args and collects its rows as R.
func (q Queue[P, R]) Slice(args P) *BatchResult[[]R] {
	query, out, err := q.t.bind(q.b.s, noescape(unsafe.Pointer(&args)), q.last())
	return queueSlice[R](q.b, q.t.t, q.t.name, query, out, err, 0)
}

// Exec queues the template with args for its row count.
func (q Queue[P, R]) Exec(args P) *BatchResult[ExecResult] {
	query, out, err := q.t.bind(q.b.s, noescape(unsafe.Pointer(&args)), q.last())
	return q.b.queueExec(q.t.t, q.t.name, query, out, err)
}

// last is where the batch keeps the SQL the template's IdentSlots took last,
// for a template that has any.
func (q Queue[P, R]) last() *spliced {
	if len(q.t.idents) == 0 {
		return nil
	}
	if q.b.last == nil {
		q.b.last = new(spliced)
	}
	return q.b.last
}

// noescape hides p from escape analysis, so a P passed by value stays on its
// caller's stack: bind reads it only while it runs, and reflect copies what it
// boxes. It is the runtime's own trick, spelled so vet sees no uintptr turned
// back into a pointer.
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := *(*uintptr)(unsafe.Pointer(&p))
	return *(*unsafe.Pointer)(unsafe.Pointer(&x))
}

// bind fills the query's arguments from the P at p, encoded for the session
// they run in, and returns the SQL its IdentSlots take. A batch passes where it
// keeps the SQL its last query from a template spliced.
func (t *Template[P, R]) bind(s *session, p unsafe.Pointer, last *spliced) (string, []any, error) {
	if t.err != nil {
		return "", nil, t.err
	}
	query := t.query
	if len(t.idents) > 0 {
		var err error
		query, err = t.splice(p, last)
		if err != nil {
			return "", nil, err
		}
	}
	if len(t.binds) == 0 {
		return query, t.args, nil // nothing reads into args but bind
	}
	args := slices.Clone(t.args)
	for _, b := range t.binds {
		fp := unsafe.Add(p, b.off)
		switch b.kind {
		case bindString:
			args[b.at] = *(*string)(fp)
		case bindInt64:
			args[b.at] = *(*int64)(fp)
		case bindInt:
			args[b.at] = *(*int)(fp)
		case bindInt32:
			args[b.at] = *(*int32)(fp)
		case bindBool:
			args[b.at] = *(*bool)(fp)
		case bindFloat64:
			args[b.at] = *(*float64)(fp)
		case bindTime:
			args[b.at] = *(*time.Time)(fp)
		case bindBytes:
			args[b.at] = *(*[]byte)(fp)
		default:
			v, err := fieldArg(b.f, fieldValue(reflect.NewAt(t.ptyp, p).Elem(), b.f.index), s.nativeJSON, s.nativeArrays)
			if err != nil {
				return "", nil, err
			}
			args[b.at] = v
		}
	}
	return query, args, nil
}

// spliced is the SQL a batch's last query from a template took, kept while
// its identifiers repeat so the next one takes it rather than splice a copy.
type spliced struct {
	t     unsafe.Pointer
	vals  [4]string
	query string
}

// splice writes the identifiers the P at p gives into the SQL, its buffer
// sized up front, or takes last's SQL when it was spliced from the same ones.
func (t *Template[P, R]) splice(p unsafe.Pointer, last *spliced) (string, error) {
	var buf [4]string
	vals := buf[:0]
	if len(t.idents) > len(buf) {
		vals = make([]string, 0, len(t.idents))
		last = nil
	}
	n := len(t.query)
	for _, id := range t.idents {
		var v string
		if id.kind == bindString {
			v = *(*string)(unsafe.Add(p, id.off))
		} else {
			v = fieldValue(reflect.NewAt(t.ptyp, p).Elem(), id.f.index).String()
		}
		if v == "" {
			return "", fmt.Errorf("barm: the template's IdentSlot %q was given an empty name", id.f.name)
		}
		vals = append(vals, v)
		n += len(v) + 2 // quoted; a quote or dot inside grows it
	}
	if last != nil && last.t == unsafe.Pointer(t) && slices.Equal(last.vals[:len(vals)], vals) {
		return last.query, nil
	}
	d := t.h.Dialect() // what the rest of the SQL was rendered for
	b := make([]byte, 0, n)
	prev := 0
	for i, id := range t.idents {
		b = append(b, t.query[prev:id.at]...)
		b = d.AppendIdent(b, vals[i])
		prev = id.at
	}
	b = append(b, t.query[prev:]...)
	query := unsafe.String(unsafe.SliceData(b), len(b))
	if last != nil {
		last.t, last.query = unsafe.Pointer(t), query
		copy(last.vals[:], vals)
	}
	return query, nil
}
