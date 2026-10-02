# barm

A fast, typed query builder and ORM for Go, built on Go 1.27 generic methods and
iterators. It runs natively on pgx's pool, or on any `database/sql` driver.

```go
db := barm.New(pgxdriver.Pool(pool), barm.Postgres)

users, err := db.Select[User]().Where("age >= ?", 18).OrderBy("id DESC").Limit(20).Slice(ctx)
user, err  := db.Select[User]().Where("email = ?", email).One(ctx)
n, err     := db.Select[User]().Where("banned").Count(ctx)

for u, err := range db.Select[User]().Seq(ctx) { // streams, closes the rows on break
	...
}
```

You name the row type once, on the builder, and every terminal returns it. No type
arguments at the call site, and no `interface{}` in sight.

- **No dependencies.** The main module imports only the standard library. The pgx driver
  lives in a module of its own.
- **Native on Postgres.** On pgx's pool, every type pgx knows reads and writes as it is:
  arrays, JSON, UUIDs. `database/sql` stays available for SQLite, MySQL and anything else.
- **Values never enter the SQL text.** Every value is a bind argument, so there is nothing to
  escape.
- **Nothing is guessed.** Tables and columns come from struct tags, not from Go names.
- **Fewer round trips.** Relations load one query per level, and a batch is one round trip
  with pgx, prepared statements and transaction included.
- **pgx decides how a query runs.** barm hands it the SQL and the arguments, so its
  statement cache and exec mode work as you configured them.
- **Faster than bun**, 1.5–3× when building and less memory when reading. See
  [Against bun](#against-bun).

## Contents

- [Installing](#installing)
- [Example](#example)
- [Models](#models)
- [Reading](#reading)
- [Conditions and arguments](#conditions-and-arguments)
- [Writing](#writing)
- [Reusing and composing queries](#reusing-and-composing-queries)
- [Relations](#relations)
- [CTEs and unions](#ctes-and-unions)
- [Raw SQL](#raw-sql)
- [Transactions](#transactions)
- [Held connections](#held-connections)
- [Schemas](#schemas)
- [Prepared statements](#prepared-statements)
- [Batching](#batching)
- [Hooks](#hooks)
- [Drivers](#drivers)
- [Interfaces](#interfaces)
- [Dialects](#dialects)
- [Not yet](#not-yet)
- [Against bun](#against-bun)
- [Repository layout and tests](#repository-layout-and-tests)

## Installing

barm needs Go 1.27, which is the first release with generic methods.

```sh
go get github.com/sirkostya009/barm
```

barm runs on a pool, which you pick when creating the `DB`. For Postgres, use pgx's pool
through the driver module:

```sh
go get github.com/sirkostya009/barm/pgxdriver
```

```go
pool, err := pgxpool.New(ctx, dsn)
db := barm.New(pgxdriver.Pool(pool), barm.Postgres)
```

For SQLite, MySQL or any other `database/sql` driver, wrap the `*sql.DB`:

```go
sqldb, err := sql.Open("sqlite", dsn)
db := barm.New(barm.SQL(sqldb), barm.SQLite)
```

See [Drivers](#drivers) for what each one does differently.

## Example

```go
type User struct {
	barm.BaseModel `barm:"table:users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	Email     string    `barm:"email"`
	Age       int       `barm:"age"`
	CreatedAt time.Time `barm:"created_at,default:current_timestamp"`
}

pool, err := pgxpool.New(ctx, dsn)
if err != nil {
	return err
}
db := barm.New(pgxdriver.Pool(pool), barm.Postgres)
defer db.Close() // closes the pool

u := &User{Name: "Ada", Email: "ada@example.com", Age: 36}
_, err = db.Insert[User]().Values(u).Returning("id").Exec(ctx) // u.ID is filled in

u.Age++
_, err = db.Update[User]().Value(u).WherePK().Exec(ctx)

adults, err := db.Select[User]().Where("age >= ?", 18).Slice(ctx)

_, err = db.Delete[User]().Value(u).WherePK().Exec(ctx)
```

## Models

A model is a struct whose tags map it to a table:

```go
type User struct {
	barm.BaseModel `barm:"table:users,alias:u"`

	ID        int64     `barm:"id,pk,autoincrement"`
	Name      string    `barm:"name"`
	CreatedAt time.Time `barm:"created_at,default:current_timestamp"`
	Books     []Book    `barm:"rel:id=author_id"`
	Scratch   string    // no tag: not a column
}
```

| tag                          | where                    | meaning                                                                                              |
| ---------------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------- |
| `table:name`                 | `BaseModel`              | the table the model reads and writes                                                                 |
| `alias:a`                    | `BaseModel`              | the alias selects use, `FROM "users" AS "u"`                                                         |
| `column_name`                | field                    | the first tag part is the column name                                                                |
| `pk`                         | field                    | part of the primary key (tag several fields for a composite key)                                     |
| `autoincrement` / `identity` | field                    | the database fills the column in, so a zero value is left out of `INSERT`                            |
| `default:expr`               | field                    | the column has a database default, so a zero value means "use it" (see [Defaults](#defaults))        |
| `nullzero`                   | field                    | a zero value is written as `NULL`, and `NULL` reads back as the zero value                           |
| `json`                       | field                    | the value is stored encoded as JSON (see [JSON columns](#json-columns))                              |
| `scanonly`                   | field                    | a column a query computes: never written or selected by default (see below)                          |
| `skipupdate`                 | field                    | left out of an update built from `Value`, for columns maintained elsewhere; `Column` still writes it |
| `rel:parent_col=child_col`   | field                    | a relation, not a column (see [Relations](#relations))                                               |
| `-`                          | field or embedded struct | skip it                                                                                              |

**Nothing is inferred from Go names.** The table comes from `BaseModel`'s tag, and a field
is a column only when its tag names one. An untagged field is simply not mapped.

**Primary key.** The fields tagged `pk` are the key `WherePK` matches on. Nothing is
inferred, not even from a column named `id`: a model with no `pk` field has no key. Add
`autoincrement` for a key the database fills in, which `INSERT` leaves out while it is zero.

**Embedded structs are flattened.** Their columns become the model's own. When two fields
map the same column, the shallower one wins, and two at the same depth are an error.
`barm:"-"` on an embedded struct skips it entirely.

**Structs without `BaseModel`** name no table. They work as result types, or with an
explicit `Table`, but not as `Select[T]`'s own table.

**Scanning** follows `database/sql` rules. Use a pointer, `sql.Null[T]` or `nullzero` for a
nullable column. `nullzero` keeps the plain type, so an empty string, a zero time or a `0`
stands for `NULL` both ways, and nothing can tell the two apart. A type with its own `Scan` method is read as one value, even when it is a struct. A
struct that only inherits `Scan` from an embedded field is still mapped column by column. A
column with no matching field is read and discarded.

### Defaults

`default:expr` says the database fills the column in. A zero value there means "let it",
and the column is left out of the statement so the table's own `DEFAULT` applies:

```go
db.Insert[User]().Values(&User{Name: "signup", Email: "s@example.com"}).Exec(ctx)
// INSERT INTO "users" ("name", "email", "age") VALUES ($1, $2, $3)
```

Only columns with `default:` (and auto keys) are left out. A zero `age` is still written as
`0`. Set the field and it is written like any other column.

A multi-row insert where only some rows set the column cannot leave it out, so the rows that
did not set it say `DEFAULT` instead:

```go
db.Insert[User]().Values(&User{Name: "a", CreatedAt: t}, &User{Name: "b"}).Exec(ctx)
// ... VALUES ($1, $2, $3, $4), ($5, $6, $7, DEFAULT)
```

`DEFAULT` is the column's real default, so a tag that has drifted from the schema cannot
write the wrong value. SQLite does not accept `DEFAULT` inside `VALUES`, so there the tag's
expression is written instead:

```sql
INSERT INTO "users" (...) VALUES (?, ?, ?, ?), (?, ?, ?, current_timestamp)
```

Keep that expression in sync with the real column default if you use SQLite. It is raw SQL
from your own struct tag, never from a value. Tag options are split on commas, so the
expression cannot contain one. Defaults apply to `INSERT` only. An `UPDATE` writes exactly
what you give it.

### JSON columns

`json` stores a field encoded with `encoding/json`, and decodes it on the way back:

```go
type Settings struct {
	Theme string   `json:"theme"`
	Tags  []string `json:"tags"`
}

type Account struct {
	barm.BaseModel `barm:"table:accounts"`

	ID       int64          `barm:"id,pk"`
	Settings Settings       `barm:"settings,json"`
	Extra    map[string]any `barm:"extra,json"`
}
```

- **Any column that holds JSON text works:** `json` or `jsonb` in Postgres, `JSON` in
  MySQL, `TEXT` in SQLite. The value is bound as a string, and Postgres stores it as a
  proper JSON value, not a JSON string.
- **On pgx, the tag is optional:** pgx encodes and decodes `json`/`jsonb` itself, from the
  column types the server reports. It is needed for writes only where a query runs
  unprepared, under `QueryExecModeExec` or `QueryExecModeSimpleProtocol`, since pgx then
  does not know the parameter is JSON. A query named with `Prepare` is never one of those.
- **Nil is `NULL`.** A nil pointer, map or slice is written as SQL `NULL`, not as the JSON
  literal `null`. With `nullzero` as well, a zero struct is `NULL` too.
- **`NULL` reads back as the zero value.**
- **Decoding replaces.** The field is reset before decoding, so a map or struct that already
  held something does not keep stale keys.
- **Errors:** a value `encoding/json` cannot encode fails the build, naming the column.

## Reading

### Terminals

| terminal      | returns                | notes                                                            |
| ------------- | ---------------------- | ---------------------------------------------------------------- |
| `Slice(ctx)`  | `[]T`                  | all rows. A `Limit` preallocates the slice                       |
| `One(ctx)`    | `T`                    | the first row, or `barm.ErrNoRows` (see below)                   |
| `Seq(ctx)`    | `iter.Seq2[T, error]`  | streams rows. Breaking out closes them                           |
| `Count(ctx)`  | `int64`                | `SELECT count(*)` over the same filters                          |
| `Exists(ctx)` | `bool`                 | `SELECT EXISTS (SELECT 1 ... LIMIT 1)`, stops at the first match |
| `Rows(ctx)`   | `*sql.Rows`            | the raw rows, yours to close                                     |
| `Build()`     | `string, []any, error` | the SQL and arguments, without running anything                  |

`SliceAs[U]`, `OneAs[U]`, `SeqAs[U]` and `BuildAs[U]` do the same for another result type
`U`. The query itself is never changed by them.

### `One`

It always runs with `LIMIT 1`, whatever `Limit` said. An `Offset` is kept. The query you
built keeps its own limit, because `One` works on a copy. Like `Slice`, it maps columns by
the names the result reports, so any projection works, raw expressions included.

### Choosing columns

A narrower result type narrows the SQL to its columns:

```go
type nameAge struct {
	Name string `barm:"name"`
	Age  int    `barm:"age"`
}

rows, err := db.Select[User]().Where("age >= ?", 18).SliceAs[nameAge](ctx)
// SELECT "u"."name", "u"."age" FROM "users" AS "u" WHERE age >= $1
```

`Column` and `ColumnExpr` pick the projection by hand, and win over the result type. A
scalar result needs one:

```go
names, err := db.Select[User]().Column("name").SliceAs[string](ctx)
oldest, err := db.Select[int64]().Table("users").ColumnExpr("max(age)").One(ctx)
```

`Column` quotes the names you give it. `ColumnExpr` is raw SQL and may take arguments.

**Computed columns.** A `scanonly` field is a column the query computes rather than one
the table has. No insert, update or default select touches it, and a raw column with its
name fills it:

```go
type Folder struct {
	barm.BaseModel `barm:"table:folders,alias:f"`

	ID         int64  `barm:"id,pk"`
	Name       string `barm:"name"`
	TcaseCount int    `barm:"tcase_count,scanonly"`
}

db.Select[Folder]().
	ColumnExpr("f.*").
	ColumnExpr("(?) AS tcase_count", db.NewRaw("SELECT count(*) FROM tcases WHERE folder_id = f.id")).
	One(ctx)
// SELECT f.*, (SELECT count(*) ...) AS tcase_count FROM "folders" AS "f"
```

A query that does not select the column leaves the field zero. Naming a `scanonly` column
in an insert's or update's `Column` is an error.

### Clauses

```go
db.Select[User]().
	Distinct().
	Join("JOIN orders o ON o.user_id = u.id").
	Where("o.total > ?", 100).
	GroupBy("u.id").
	Having("count(*) > ?", 3).
	OrderBy("u.id DESC").
	Limit(20).
	Offset(40).
	For("UPDATE") // FOR UPDATE
```

A negative `Limit` or `Offset` is an error rather than no limit, so a page size taken from
input cannot ask for the whole table. `Offset` without `Limit` works on every dialect. SQLite and MySQL only take `OFFSET` as
part of a `LIMIT`, so barm writes the "no limit" value there (`LIMIT -1` on SQLite).

### Tables a model does not name

`Table` sets what a query reads from or writes to, so the model need not be the table, or
have one at all:

```go
u, err := db.Select[User]().Table("users_2024").One(ctx)

db.Insert[User]().Table("users_2024").Values(&u).Exec(ctx)   // User's columns, another table
db.Update[User]().Table("users_2024").Value(&u).WherePK().Exec(ctx)
db.Delete[User]().Table("users_2024").Where("age > ?", 90).Exec(ctx)
```

On a select, `Table` takes any table expression, which covers joins and ad-hoc results.
Name the row type up front and the terminal needs no type argument:

```go
rows, err := db.Select[struct {
	Name  string `barm:"name"`
	Total int64  `barm:"total"`
}]().
	Table("users u").
	Join("JOIN orders o ON o.user_id = u.id").
	Column("u.name").
	ColumnExpr("sum(o.cents) AS total").
	GroupBy("u.name").
	Slice(ctx)
// SELECT "u"."name", sum(o.cents) AS total FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.name
```

Every field of such a type still needs its tag. With a custom table expression, the model's
columns are written unqualified, since the expression owns its naming.

## Conditions and arguments

Fragments such as `Where`, `Join` and `OrderBy` are SQL that barm does not parse. Every `?`
in them becomes the dialect's placeholder, and its value goes into the arguments:

```go
Where("age >= ?", 18)                  // postgres: age >= $1      mysql, sqlite: age >= ?
Where("name = ?1 OR email = ?1", v)    // postgres: one argument, referenced twice
Where("id = ANY(?)", ids)              // id = ANY($1), a slice is one argument: an array
Where("id IN (?)", barm.In(ids))       // id IN ($1, $2, $3), spelled out
Where("data ?? 'key'")                 // ?? is a literal question mark
```

- **`?N`** refers to the N-th argument of its own fragment. On Postgres the value is bound
  once and the placeholder reused. On `?` dialects it is bound again, since a `?` cannot be
  referenced twice. `?N` does not move the bare `?` cursor, so the two forms count
  separately.
- **A slice is one argument.** It is never spelled out as `($1, $2, $3)`: the driver sends it
  as an array, so match against it with `= ANY(?)`, not `IN (?)`. The SQL is then the same for
  any number of values, which keeps prepared statements and plan caches reusable, and an
  empty slice simply matches nothing. Pass a typed slice (`[]int64`, `[]string`) so the
  driver knows the element type.
- **`barm.In` spells a slice out**, one placeholder per element, inside the parentheses you
  write: `IN (?)`. It is how to filter by a list on MySQL and SQLite, which have no arrays.
  An empty `In` is a build error, since `IN ()` is not SQL, so decide what an empty filter
  means before building. Reused through `?N`, the list is spelled out again.
- **A query is an argument too.** Any barm query, `NewRaw` included, renders in place, inside
  the parentheses you write, and continues the enclosing query's numbering:

    ```go
    counts := db.NewRaw("SELECT count(*) FROM books WHERE books.author_id = a.id")
    recent := db.Select[Book]().Column("author_id").Where("published > ?", cutoff)

    db.Select[Author]().
    	ColumnExpr("(?) AS books", counts).
    	Where("id IN (?)", recent)
    // SELECT (SELECT count(*) ...) AS books FROM "authors" AS "a"
    //   WHERE id IN (SELECT "author_id" FROM "books" WHERE published > $1)
    ```

    A subquery that fails to build fails the query around it. Reused through `?N`, it is
    rendered again.

- **Arguments are bound when the query is built.** The slice you pass to `Where` is kept, not
  copied, so changing it before the build changes the query. Struct values are read at build
  time too.

**Combining conditions.** Each `Where` joins with `AND`, each `WhereOr` with `OR`, at the
same level. When there is more than one, each condition is wrapped in parentheses, so an
`OR` inside one, in whatever spelling, cannot bind across the others:

```go
Where("a = 1 OR b = 2").Where("c = ?", 3).WhereOr("d = ?", 4)
// WHERE (a = 1 OR b = 2) AND (c = $1) OR (d = $2)
```

SQL binds `AND` tighter than `OR`, so that last line reads as `(... AND c) OR d`. To group an
`OR` with what comes before it, write the group in a single `Where`.

### Argument dedup

`barm.WithArgDedup()` binds a string or `[]byte` once when the same one (the same backing
memory, not merely equal contents) reaches several placeholders in one query. It saves
sending a large payload twice.

It is off by default, for two reasons:

- **The SQL depends on memory layout.** It changes with how the caller happened to share
  memory. That is also why it never applies to prepared queries.
- **Postgres gives a placeholder one type.** The same string compared to a `text` column and
  to an `integer` one fails once merged (`operator does not exist: integer = text`).

Prefer `?N`, which says the same thing in the query itself. Empty values are never merged,
since a nil `[]byte` is `NULL` to the driver and an empty one is not.

## Writing

```go
db.Insert[User]().Values(&u1, &u2).Exec(ctx)                 // multi-row VALUES
db.Insert[User]().Values(&u).Column("name", "email").Exec(ctx) // only these columns

db.Update[User]().Value(&u).WherePK().Exec(ctx)              // every column but the key
db.Update[User]().Value(&u).Column("age").WherePK().Exec(ctx) // just age
db.Update[User]().Set("age = age + ?", 1).Where("id = ?", id).Exec(ctx)

db.Delete[User]().Value(&u).WherePK().Exec(ctx)
db.Delete[User]().Where("created_at < ?", cutoff).Exec(ctx)
```

### Picking rows

An `UPDATE` or `DELETE` must say which rows it touches, with `Where` or `WherePK`. Without
one it is an error, never a full-table write. `Value` supplies the row's data and key, but
picks nothing on its own.

`WherePK` adds the value's primary key as a condition, in its place among the others. An
optimistic-lock guard is then one more call:

```go
db.Update[Doc]().Value(&d).WherePK().Where("version = ?", seen).Exec(ctx)
// UPDATE "docs" SET "title" = $1, "version" = $2 WHERE "id" = $3 AND version = $4
```

### Getting generated keys back

| dialect  | how the new key reaches your struct                                                               |
| -------- | ------------------------------------------------------------------------------------------------- |
| Postgres | add `Returning("id")`. Postgres has no `LastInsertId`, so without `RETURNING` the key stays unset |
| SQLite   | automatic for a single-row insert, via `LastInsertId`. Use `Returning` for several rows           |
| MySQL    | automatic for a single-row insert, via `LastInsertId`                                             |

`Exec` with `Returning` scans the returned columns back into the values you passed to
`Values`, in order:

```go
_, err := db.Insert[User]().Values(&a, &b).Returning("id", "created_at").Exec(ctx)
```

If fewer rows come back than values went in — a trigger dropped one, say — the rows after it
have landed on the wrong values, and `Exec` returns an error saying so.

### Upserts

`On` writes the conflict clause as you give it, and `Set` adds the assignments of a clause
that updates:

```go
db.Insert[User]().Values(&u).On("CONFLICT (email) DO NOTHING").Exec(ctx)

db.Insert[User]().Values(&u).On("CONFLICT (email) DO UPDATE").Set("name = excluded.name").Exec(ctx)
// ... ON CONFLICT (email) DO UPDATE SET name = excluded.name

db.Insert[User]().Values(&u).On("DUPLICATE KEY UPDATE").Set("name = VALUES(name)").Exec(ctx) // MySQL
```

An insert with an `On` clause may skip rows, so it can return fewer rows than it was given,
and barm does not read the clause to know which ones. So `Exec` writes the returned rows
back only when every value came back (as with `DO UPDATE`). Otherwise it writes none and
returns an error, rather than putting ids into the wrong structs. Use `Slice` to get the
returned rows themselves.

### Reading rows back from a write

Every write has the same terminals as a select, and reads its `RETURNING` rows:

```go
created, err := db.Insert[User]().Values(&u).One(ctx)
bumped, err  := db.Update[User]().Set("age = age + 1").Where("id = ?", id).One(ctx)
removed, err := db.Delete[User]().Where("age >= ?", 90).Slice(ctx)

for row, err := range db.Delete[User]().Where("stale").Seq(ctx) { ... } // too many to collect
```

- **`RETURNING` names exactly what gets scanned.** Without an explicit `Returning`, the
  clause lists the result type's own columns, never `*`. A scalar result has to name its
  column with `Returning`.
- **The result type can differ from what went in.** You can write a small type and read the
  full row back:

    ```go
    type signup struct {
    	Name  string `barm:"name"`
    	Email string `barm:"email"`
    }
    full, err := db.Insert[signup]().Table("users").Values(&signup{...}).OneAs[User](ctx)
    ```

- **Only `Exec` writes into your values.** The row terminals leave the values you passed
  alone and return new rows.
- **`Build` and `Exec` never add a `RETURNING`** that you did not ask for.
- **No `RETURNING` on the dialect** (MySQL) means an error instead of SQL that will not
  parse.
- **`Seq` on a write** streams the returned rows. The write has already happened when the
  first row arrives, so breaking early stops the reading, not the write.

## Reusing and composing queries

**Builders change in place.** Every method changes the builder and returns the same
pointer, not a copy. To branch two queries off one start, use `Clone`:

```go
base := db.Select[User]().Where("tenant_id = ?", t)
admins, err := base.Clone().Where("role = ?", "admin").Slice(ctx)
total, err := base.Count(ctx) // still every user of the tenant
```

Terminals never modify the query. `One`, `Count`, `Exists` and the `As` variants work on a
copy, so a query can be run again, or run differently.

**`Apply`** passes a query through functions, so filters can be named and reused. A nil
function is skipped, which makes an optional filter free at the call site:

```go
func adults(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
	return q.Where("age >= ?", 18)
}

var byTenant func(*barm.SelectQuery[User]) *barm.SelectQuery[User] // nil: no tenant filter

users, err := db.Select[User]().Apply(adults, byTenant).Slice(ctx)
```

**`Via`** runs a query on another handle: a DB, a transaction or a held connection. The
original stays bound to where it was built, so one query built once can serve all of them,
from any number of goroutines at once:

```go
var byEmail = db.Select[User]().Where("email = ?", email)

u, err := byEmail.Via(tx).One(ctx)
```

`Via` costs nothing over running the query in place: the copy shares the original's clauses
and stays on the stack. For that reason, keep building on a shared query only after
`Clone`. The handle should use the same dialect the query was built for, because `Column`
quotes names when it is called.

**Accessors.** Every query has `IDB()`, which returns the handle it runs on wrapped in a
`Handle`, and `Dialect()`. Code handed a query can start another one in the
same place, inside the same transaction:

```go
func withAudit(q *barm.SelectQuery[User]) *barm.SelectQuery[User] {
	q.IDB().Insert[Audit]().Values(&entry).Exec(ctx)
	return q
}
```

## Relations

A relation is a field that holds rows of another table. You declare the column on each side
that joins them, `rel:parent_col=child_col`:

```go
type Author struct {
	barm.BaseModel `barm:"table:authors,alias:a"`

	ID    int64  `barm:"id,pk,autoincrement"`
	Name  string `barm:"name"`
	Books []Book `barm:"rel:id=author_id"`
}

type Book struct {
	barm.BaseModel `barm:"table:books"`

	ID       int64  `barm:"id,pk,autoincrement"`
	Title    string `barm:"title"`
	AuthorID int64  `barm:"author_id"`
}
```

`Relation` loads one along with the query. The optional functions filter and order the
child query:

```go
authors, err := db.Select[Author]().
	Where("born > ?", 1900).
	Relation[Book]("Books", func(q *barm.SelectQuery[Book]) *barm.SelectQuery[Book] {
		return q.Where("title LIKE ?", "%Go%")
	}).
	Slice(ctx)
```

That is two queries however many authors come back. There is one query per relation, never
one per row:

```sql
SELECT "a"."id", "a"."name" FROM "authors" AS "a" WHERE born > $1
SELECT "books"."author_id", "books"."id", "books"."title" FROM "books"
  WHERE title LIKE $1 AND "books"."author_id" = ANY($2)
```

**Narrow projections work on both sides.** The model supplies the table and the result type
supplies the columns, one level down as well:

```go
type authorLite struct {
	ID    int64      `barm:"id,pk"`
	Name  string     `barm:"name"`
	Books []bookLite `barm:"rel:id=author_id"`
}

type bookLite struct {
	Title string `barm:"title"`
}

authors, err := db.Select[Author]().Relation[bookLite]("Books").SliceAs[authorLite](ctx)
```

`Author.Books` is a `[]Book`, so `Book` says which table the rows come from, and
`authorLite.Books` is a `[]bookLite`, so `bookLite` says which columns to read.
`bookLite` needs no `AuthorID`: the join key is always selected, so rows can be grouped,
and it is added after your functions run so nothing can remove it.

**Nested relations.** Calling `Relation` inside the function loads a relation of the
relation:

```go
db.Select[Author]().
	Relation[Book]("Books", func(q *barm.SelectQuery[Book]) *barm.SelectQuery[Book] {
		return q.Relation[Review]("Reviews")
	}).
	Relation[Tag]("Tags").
	Slice(ctx)
```

Relations load **breadth-first**: all relations at one depth go out together, pipelined in
one round trip where the driver supports it (pgx). The query above is three round trips,
one per depth: authors, then books and tags together, then reviews. A driver that cannot
pipeline sends them one at a time, with the same rows and more round trips.

Good to know:

- **Keys go as one array parameter** where the dialect supports it (`= ANY($1)` on
  Postgres), so the SQL stays the same for any number of parents and the driver can reuse
  the statement. Elsewhere they are spelled out as `IN (?, ?, ?)`. Keys are deduplicated.
- **Key types can differ.** Child keys are read as the parent field's type, so a parent
  `ID int` and a driver returning `int64` still match.
- **Field shapes:** a slice field takes many rows, `[]U` or `[]*U`, and any other field
  takes one. A parent with no children keeps a nil slice.
- **Errors instead of guesses:** the field must be a declared relation, the result field
  must hold exactly the type `Relation` was given, and the parent projection must select the
  join column.
- **`Schema`** on the parent query is inherited by its relations.
- **A query with no model** behind it, such as `Select[authorLite]()` without an `As`, has
  nowhere to take the child table from. There the child type must carry its own
  `BaseModel`.
- **`Seq` cannot load relations**, since they need the whole result first. It returns an
  error instead of falling back to a query per row. Use `Slice` or `One`.

## CTEs and unions

`With` adds a common table expression, and you read from it by name. Every builder has it,
so a CTE can feed a write as easily as a read:

```go
db.Select[User]().
	With("adults", db.Select[User]().Column("id").Where("age >= ?", 18)).
	Table("adults").
	Slice(ctx)
// WITH "adults" AS (SELECT "id" FROM "users" AS "u" WHERE age >= $1) SELECT ... FROM adults
```

A CTE body renders into the query it belongs to, so both share one argument numbering. With
two bodies, the second continues at `$2` instead of restarting at `$1`. That is why nesting
takes a barm `Query` rather than a SQL string.

`Union` and `UnionAll` take a `Query` too, and a union is what a recursive CTE hangs off:

```go
db.Select[node]().Table("tree").
	WithRecursive("tree", db.Select[node]().Where("parent_id IS NULL").
		UnionAll(db.Select[node]().Join("JOIN tree t ON t.id = nodes.parent_id"))).
	Slice(ctx)
```

- **`RECURSIVE` marks the whole `WITH` clause.** One recursive CTE makes the clause
  `WITH RECURSIVE`, and the others in it stay ordinary. Postgres requires the keyword, and
  SQLite treats it as optional.
- **A union goes before `ORDER BY`.** A trailing `ORDER BY`, `LIMIT` or `OFFSET` then applies
  to the whole union, so put them on the outer query, not on a branch.
- **`WithExpr` takes a whole CTE item as raw SQL**, for what a name and a body cannot say,
  such as a column list or `MATERIALIZED`:

    ```go
    q.WithExpr(`recent(id, seen) AS MATERIALIZED (SELECT id, seen FROM hits WHERE seen > ?)`, cutoff)
    ```

## Raw SQL

`NewRaw` turns hand-written SQL into a `Query`. `?` markers work as in `Where`:

```go
db.NewRaw("DELETE FROM sessions WHERE seen < ?", cutoff).Exec(ctx)
```

It goes wherever a `Query` goes: into a batch, or in as a CTE body or a union branch. It
exists on `DB`, `Tx`, `Conn` and `Handle`. For reading rows, use `Select` with `Table` and
`ColumnExpr`, which take raw SQL too and give you typed rows.

`Exec(ctx, sql, args...)` and `Query(ctx, sql, args...)` on any handle run SQL exactly as
written, in the driver's own placeholders (`$1` on Postgres), for statements like DDL or
`SET`. `Query` returns `barm.Rows`, which the caller closes. Hooks see both.

## Transactions

`Begin()` and `BeginTx(ctx, opts)` take `database/sql`'s `*sql.TxOptions` on every driver and
return a `*barm.Tx` that builds queries:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback() // a no-op after Commit

if _, err := tx.Insert[User]().Values(&u).Exec(ctx); err != nil {
	return err
}
return tx.Commit()
```

There is deliberately no `RunInTx(func)` helper. `defer tx.Rollback()` does the same job
without taking over your control flow. Rolling back or committing a finished transaction
returns `sql.ErrTxDone` and does nothing else.

**Nesting.** Beginning a transaction on a transaction nests it, with the same calls:

```go
inner, err := tx.BeginTx(ctx, nil)
defer inner.Rollback()   // undoes only the inner work, tx carries on
...
inner.Commit()           // keeps it, tx is still open
return tx.Commit()       // ends the transaction for real
```

Under the hood a nested transaction is a savepoint, with no names for you to manage, and it
nests as deep as you like. A savepoint cannot have its own isolation level, so a nested
`BeginTx` needs nil options. A nested `Rollback` rewinds to the savepoint and releases it, so
failed nested transactions in a loop do not pile up open savepoints, and a second `Rollback`
returns `sql.ErrTxDone`.

**Connections.** A transaction holds its connection from begin until commit or rollback,
so one nobody finishes holds it for good: always `defer tx.Rollback()`. On `database/sql`,
the driver also rolls back when the context you began with ends; pgx does not.

## Held connections

`db.Conn(ctx)` takes one connection out of the pool and keeps it until `Close`. It builds
everything a `DB` does, and every query built on it runs on that connection. Session state
needs this: temporary tables, advisory locks, `search_path`, anything lost on another
connection.

```go
c, err := db.Conn(ctx)
if err != nil {
	return err
}
defer c.Close() // the only thing that gives it back

tx, err := c.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback()

if _, err := tx.Exec(ctx, "SET LOCAL search_path = tenant_7"); err != nil {
	return err
}
users, err := tx.Select[User]().Slice(ctx)
...
return tx.Commit()
```

- **Put `SET LOCAL` inside a transaction.** Outside one, Postgres accepts it and silently
  does nothing, so you would read the wrong schema with no error. A plain `SET` outlives
  `Close` and passes to the next user of the connection, so reset it yourself if you use it.
- **A transaction begun on a `Conn` borrows it.** The connection stays held after commit or
  rollback.
- **Always `Close` a `Conn`.** One you forget is never given back to the pool.
- **`Driver()`** on a `Conn` or a `Tx` returns the driver's own connection or transaction.

## Schemas

`Schema` qualifies the model's table, and relations inherit it:

```go
db.Select[User]().Schema("tenant_7").Relation[Order]("Orders").Slice(ctx)
// FROM "tenant_7"."users" AS "u" ..., then FROM "tenant_7"."orders" ...

db.Insert[User]().Schema("tenant_7").Values(&u).Exec(ctx)
```

On a select, a custom `Table` expression overrides it. The schema is part of the SQL text,
so each schema is its own prepared statement.

**It qualifies only the model's table.** Postgres resolves every table reference on its own,
so tables inside `Join`, `Where` or `ColumnExpr` are not qualified:

```sql
SELECT o.tag FROM tenant_7.users u JOIN orders o ON o.user_id = u.id
-- reads public.orders, with no error
```

barm does not parse or rewrite your fragments, because it cannot tell a table name from a
CTE, a temporary table or an alias. Qualify those tables yourself, or set `search_path` in a
transaction as shown above.

## Prepared statements

Every builder has `Prepare(name)`:

```go
u, err := db.Select[User]().Where("id = ?", id).Prepare("user_by_id").One(ctx)
_, err = db.Insert[User]().Values(&u).Prepare("insert_user").Exec(ctx)
```

- **A name is bound to one SQL text.** Using it for different SQL is an error, not a silent
  re-prepare. Anything that changes the text needs a name of its own:
    - `One` adds `LIMIT 1`, so the same query prepared for `One` and for `Slice` needs two
      names.
    - An insert's text depends on its row count, and on which default columns each row leaves
      zero.
    - A different schema changes the text.
- **Argument dedup is never applied**, since it would make the text depend on memory
  layout.

**A query without `Prepare` runs however the driver runs queries.** barm hands pgx the SQL
and its arguments, so the pool's `DefaultQueryExecMode` decides. By default that is pgx's
statement cache, which prepares each query text once per connection and runs it by name
after. In a batch, barm's driver keeps the same statements the same way itself, up to the
same `StatementCacheCapacity`. Set the mode to `QueryExecModeExec` to prepare nothing, at
the price of Postgres parsing and planning every query, about 12 µs each on a local server:

```go
cfg, _ := pgxpool.ParseConfig(dsn)
cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
pool, _ := pgxpool.NewWithConfig(ctx, cfg)
```

**On pgx**, a named query is prepared whatever the pool's mode, and runs by name after, one
round trip each time with no Parse. Its arguments are encoded for the types the server gives
its parameters, as pgx's statement cache encodes them: encoded without knowing them, a
`time.Time` bound for a `timestamp` column would store a different value, and a map bound for
`jsonb` would not encode at all. The pool keeps those types for all its connections:

- **New to the pool**, a statement with arguments is described first, a round trip of its
  own, once.
- **New to a connection** but known to the pool, it is parsed in the same flush that runs it,
  so it is still one round trip.
- **A statement with no arguments** never needs describing.

The name you give `Prepare` is barm's key, bound to one SQL text; the statement on the server
is named from a hash of the name and the SQL. If the statement fails — the server forgot it
(`DEALLOCATE`, a pooler resetting the session), or a schema change altered it — it is
forgotten, by the connection and the pool, that call returns the error, and the next one
describes and prepares it again.

**On `database/sql`**, statements go through `database/sql`'s `Prepare`. That is a separate
round trip, and on the pool it takes its own connection. Inside a transaction the statement
is then rebound to it. A held `Conn` keeps its own statements, prepared on its connection,
and a sequential batch on the pool prepares its named queries on the connection it borrows,
for that batch. `db.Close()` closes the cached statements, then the database.

**`Seq` holds its connection** until the loop ends, so a loop that queries the same
transaction it is reading from waits on itself on pgx, which runs one statement at a time
per connection.

## Batching

A batch sends many queries in **one round trip**, on pgx's pool. A statement it keeps is
parsed in that same round trip the first time a connection runs it; only one with arguments
that the pool has never seen costs a round trip before, to learn its types (see
[Prepared statements](#prepared-statements)):

```go
b := db.Batch()

adults := b.Slice(db.Select[User]().Where("age >= ?", 18))   // *BatchResult[[]User]
oldest := b.One(db.Select[User]().OrderBy("age DESC"))       // *BatchResult[User]
total  := b.Count(db.Select[User]())                         // *BatchResult[int64]
known  := b.Exists(db.Select[User]().Where("email = ?", e))  // *BatchResult[bool]
ins    := b.Exec(db.Insert[User]().Values(&u))               // *BatchResult[ExecResult]

if err := b.Run(ctx); err != nil {
	return err
}
for _, u := range adults.Value() { ... } // []User
```

Each queued query returns a result typed to what it produces, so one batch mixes any number
of row types, and a loop can queue as many as it likes.

- **Reading results:** `Value()` returns the result, `Err()` its error, and `Get()` both.
  Until `Run` fills a result, it holds the zero value and `barm.ErrNotRun`, so reading too
  early is obvious. Queries behind a failure never run, and keep `ErrNotRun`.
- **When a query fails,** `Run` returns the first failure, naming the query. It then empties
  the batch, so it cannot run twice.
- **Implicit transaction:** a batch runs in one unless it is already inside a transaction,
  so a failure rolls the whole batch back.
- **Inserts queued with `Exec`** scan their `RETURNING` columns back into their values, the
  same as their own `Exec`.
- **`b.One` adds `LIMIT 1`**, like `One`.
- **Relations** on queued selects load after the batch, breadth-first across all its
  queries.
- **Queries render when queued,** so later changes to a struct do not affect the batch.

`database/sql` cannot send statements together, so a batch on `barm.SQL` fails with
`ErrNoBatcher` rather than quietly running one query at a time. To run it one statement at a
time anyway, on one connection, say so when creating the pool:

```go
db := barm.New(barm.SQL(sqldb, barm.SequentialBatches()), barm.SQLite)
```

20 lookups on a local Postgres, batched versus one at a time. Over a real network the gap
grows with latency:

```
batched      171 µs/op
sequential   547 µs/op     3.2× slower
```

### Batches in transactions

A batch on a transaction is part of it, and rolls back with it:

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

b := tx.Batch()
b.Exec(tx.Insert[User]().Values(&u))
b.Exec(tx.Insert[User]().Values(&v))
err := b.Run(ctx) // both statements are inside the transaction
```

A batch can also carry its own transaction. `Begin` and `Commit` are queued like any other
statement, so the transaction costs no extra round trip. Postgres runs the batch in order, so
`Begin` is done before anything after it can fail, and `Rollback` always has something to
return to. When the batch first describes a statement inside a transaction, it does so under
a savepoint of its own, so a statement that cannot be described fails in its place in the
batch rather than taking the transaction with it:

```go
b := db.Batch()
b.Begin()
for _, r := range rows {
	b.Exec(db.Insert[Row]().Values(r))
}
b.Commit()
err := b.Run(ctx)
```

What those statements are depends on where the batch runs:

| batch on                    | `Begin`                | `Commit`                       | `Rollback`                         |
| --------------------------- | ---------------------- | ------------------------------ | ---------------------------------- |
| a pool or a held connection | `BEGIN`                | `COMMIT`                       | `ROLLBACK`                         |
| a transaction               | `SAVEPOINT barm_batch` | `RELEASE SAVEPOINT barm_batch` | `ROLLBACK TO SAVEPOINT barm_batch` |

Inside a transaction a savepoint is the only correct choice. A `COMMIT` there would end the
transaction you were already in, taking unrelated work with it. So a batch undoes only
itself, and the transaction around it survives:

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()

tx.Insert[Audit]().Values(&entry).Exec(ctx) // must survive whatever follows

b := tx.Batch()
b.Begin()                                   // SAVEPOINT
for _, r := range rows {
	b.Exec(tx.Insert[Row]().Values(r))
}
b.Commit()                                  // RELEASE SAVEPOINT

if err := b.Run(ctx); err != nil {
	b.Rollback(ctx)                         // ROLLBACK TO SAVEPOINT, right away
}
err := tx.Commit()                          // the audit entry lands either way
```

**Why `Rollback` runs immediately instead of being queued.** When a query fails, Postgres
discards everything queued behind it and refuses further commands with "current transaction
is aborted". A queued rollback would be skipped exactly when it is needed. That is also what
`ErrNotRun` on the `Commit` result means. `ROLLBACK TO SAVEPOINT` is one of the few
statements Postgres accepts in that state, so `Rollback` sends it as a second round trip.

- **`Rollback` needs a held connection.** It goes to the connection the batch ran on, so the
  batch must be on a transaction or a held connection. A batch on the pool has already
  returned its connection, and the pool discards a connection left in a failed transaction.
- **To throw a batch away on purpose** (a dry run against real data), queue a rollback with
  `NewRaw` as the last statement of a batch you expect to succeed.
- **The savepoint name is fixed**, `barm.BatchSavepoint`. Nesting still behaves, because
  `ROLLBACK TO` and `RELEASE` act on the most recent savepoint of that name.

## Hooks

### Query hooks

```go
db := barm.New(pgxdriver.Pool(pool), barm.Postgres, barm.WithHook(barm.QueryHook{
	AfterQuery: func(_ context.Context, ev *barm.QueryEvent) {
		if ev.Duration > time.Second {
			log.Printf("slow %s (%s): %s", ev.Op, ev.Duration, ev.Query)
		}
	},
}))
```

A hook is a struct of optional functions, `BeforeQuery` and `AfterQuery`, so you set only
the ones you need. Hooks run in registration order before a query and in reverse after it,
unwinding like defers. The context `BeforeQuery` returns goes to the database call and on to
`AfterQuery`, which is where a tracing hook keeps its span.

`QueryEvent` has:

- **Before the query:** `Op` (`SELECT`, `INSERT`, …), `Query`, `Args`, `Prepared` (the
  `Prepare` name) and `StartedAt`. `Args` is a copy, so a hook that masks a value in place
  changes what it reports, not what is written.
- **After it:** `Err`, `Duration` and, for execs, `Result`.

When events finish:

- **Single-row reads** (`One`, `Count`, `Exists`) finish after the row is scanned, so a
  no-rows error is reported.
- **`Seq`** reports the query itself, not the iteration that follows.
- **In a batch,** each query gets its own event. All of them start when the batch is sent,
  and each finishes when its result is read. A query an earlier failure kept from running
  finishes with `ErrNotRun`. A batched exec reports its row count in `Result`, but no insert
  id.

With no hook registered, hooks cost nothing: no timestamp, no allocation.

`WithHook` also exists on a `Tx` and a `Conn`, adding hooks on top of the DB's for just that
handle.

### Transaction hooks

```go
db := barm.New(pgxdriver.Pool(pool), barm.Postgres, barm.WithTxHook(barm.TxHook{
	AfterCommit: func(_ context.Context, ev *barm.TxEvent) {
		log.Printf("commit after %s: %v", ev.Duration, ev.Err)
	},
}))
```

`TxHook` has `BeforeCommit`, `AfterCommit`, `BeforeRollback` and `AfterRollback`, all
optional. `TxEvent` carries `StartedAt`, and afterwards `Err` and `Duration`.

- **`BeforeCommit` runs work that must land inside the transaction**, such as draining an
  outbox or writing an audit row. If it returns an error, the commit does not happen. `Commit`
  returns that error and the transaction stays open for your deferred `Rollback`. The failing
  hook gets no `AfterCommit`, and later hooks do not run. A rollback cannot be refused, so
  `BeforeRollback` returns no error.
- **Hooks fire once per transaction**, when the outermost one ends. A nested commit is only a
  `RELEASE SAVEPOINT`, which the outer transaction can still roll back, so it fires nothing.
  That makes transaction hooks safe for cache invalidation: they run once, when the writes
  are durable.

Register hooks on one transaction to report what that unit of work did:

```go
tx, err := db.BeginTx(ctx, nil)
defer tx.Rollback()

if _, err := tx.Update[User]().Value(&u).WherePK().Exec(ctx); err != nil {
	return err
}
tx.WithTxHook(barm.TxHook{
	AfterCommit: func(context.Context, *barm.TxEvent) { cache.Evict(u.CacheKey()) },
})
return tx.Commit()
```

A hook registered inside a nested transaction still fires when the outermost transaction
commits. If that nested transaction rolls back, its hooks are dropped with its work, so you
never invalidate for rows that were never written. `WithTxHook` on a `Conn` applies to the
transactions it begins.

## Drivers

barm talks to the database through a small interface of its own, `barm.Pool`, and ships
two:

|                | `pgxdriver.Pool(pool)`                                     | `barm.SQL(sqldb)`                                      |
| -------------- | ---------------------------------------------------------- | ------------------------------------------------------ |
| databases      | Postgres                                                   | anything with a `database/sql` driver                  |
| column types   | everything pgx encodes and decodes: arrays, JSON, UUIDs, … | what `database/sql` converts; `json` tag for JSON      |
| batches        | pipelined, one round trip                                  | `ErrNoBatcher`, or one by one with `SequentialBatches` |
| plain queries  | as the pool's `DefaultQueryExecMode` says                  | as the driver runs them                                |
| last insert id | none; use `RETURNING`                                      | where the driver reports one                           |

- **JSON on pgx** needs no tag under pgx's default mode, reading or writing. See
  [JSON columns](#json-columns) for the modes where writing does.
- **Reaching the driver:** `db.Pool()` returns the pool, `*barm.SQLPool` has `DB()` for the
  `*sql.DB`, and `Driver()` on a `Conn` or `Tx` returns the driver's own connection or
  transaction.
- **Writing your own:** implement `barm.Pool`. It is an `Executor` (query, exec, the prepared
  forms, and batches) that can also hand out a held connection and begin a transaction,
  each of which is an `Executor` too.

## Interfaces

**`IDB`** is anything barm builds on: `*DB`, `*Tx` or `*Conn`. Go does not allow generic
methods in interfaces yet, so an `IDB` starts queries through `Handle`, a small wrapper:

```go
func adults(ctx context.Context, h barm.IDB) ([]User, error) {
	return barm.Handle{h}.Select[User]().Where("age >= ?", 18).Slice(ctx)
}
```

One function then serves a pool, a transaction and a held connection alike. `IDB` also
has `Exec` and `Query` for raw SQL, and `Begin` and `BeginTx`, which do the right thing for whatever it holds: a `DB` starts a
transaction on a connection of its own, a `Conn` lends its connection, and a `Tx` opens a
savepoint. So a transaction helper written once over `IDB` nests correctly:

```go
func inTx(ctx context.Context, h barm.IDB, fn func(*barm.Tx) error) error {
	tx, err := h.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
```

`Handle` is a stopgap, and `IDB` takes over its methods once Go allows generic methods in
interfaces. A query that is already built runs on any handle through
[`Via`](#reusing-and-composing-queries).

**`Query`** is any built query: every builder, and `NewRaw`. Only barm's own types can
implement it, because a nested query renders into the enclosing query to share its argument
numbering.

**`TypedQuery[T]`** adds the terminals, so a helper can take any query that returns users,
whether it selects, inserts or deletes them:

```go
func newest(ctx context.Context, q barm.TypedQuery[User]) (User, error) {
	return q.One(ctx)
}

newest(ctx, db.Select[User]().OrderBy("id DESC"))
newest(ctx, db.Insert[User]().Values(&u))
```

## Dialects

Three dialects are built in: `barm.Postgres`, `barm.MySQL` and `barm.SQLite`.

|                           | Postgres       | MySQL                | SQLite                   |
| ------------------------- | -------------- | -------------------- | ------------------------ |
| placeholders              | `$1`, reusable | `?`                  | `?`                      |
| identifier quotes         | `"name"`       | `` `name` ``         | `"name"`                 |
| `RETURNING`               | yes            | no                   | yes                      |
| relation keys             | `= ANY($1)`    | `IN (?, ?, ...)`     | `IN (?, ?, ...)`         |
| `DEFAULT` inside `VALUES` | yes            | yes                  | no, the tag's expression |
| `OFFSET` without `LIMIT`  | as is          | adds the max `LIMIT` | adds `LIMIT -1`          |

`Dialect` is an interface, so you can supply your own.

## Not yet

What barm does not do yet, for anyone coming from bun:

- **Postgres array columns on `database/sql`.** On pgx's pool they read and write natively.
  Through `database/sql`, the driver hands an array over as text, which a `[]int64` field
  cannot take.
- **Statement shapes:** `UPDATE … FROM`, `DELETE … USING`, `INSERT … SELECT`, a `VALUES` list
  built from structs, multi-row update and delete by key, `DISTINCT ON`.
- **Grouped conditions.** `WhereOr` joins at the top level, so a parenthesized `OR` group
  goes in a single `Where` string.
- **Identifier and raw-SQL arguments**, bun's `Ident` and `Safe`.
- **Relations:** composite join keys, many-to-many, and belongs-to loaded by a `JOIN` in
  the same query rather than a query of its own.

## Against bun

`bench/` runs barm and bun over the same table, the same SQL and the same SQLite database,
barm through `barm.SQL`. It is its own module, so bun stays out of barm's `go.mod`. Medians
of 6 runs, measured 2026-10-01:

|                                         |    barm |     bun |           |  barm B/op | bun B/op | barm allocs | bun allocs |
| --------------------------------------- | ------: | ------: | --------: | ---------: | -------: | ----------: | ---------: |
| build select                            |   363ns |   556ns | **1.53×** |        577 |     1024 |           5 |         10 |
| build select, 3 wheres + order + paging |   670ns |   1.4µs | **2.06×** |       1042 |     1728 |          10 |         17 |
| build insert, 1 row                     |   444ns |   1.2µs | **2.79×** |        520 |     1400 |           6 |         14 |
| build insert, 100 rows                  |  14.3µs |  38.3µs | **2.69×** |      25581 |    30472 |         112 |        220 |
| build update by pk                      |   418ns |   930ns | **2.23×** |        561 |     1224 |           5 |         10 |
| build delete by pk                      |   261ns |   357ns |     1.36× |        328 |      680 |           5 |          6 |
| select 1 row                            |  14.4µs |  15.4µs |     1.07× |       2006 |     6329 |          40 |         44 |
| select 100 rows                         | 165.0µs | 184.5µs |     1.12× |      36970 |    43580 |         642 |        745 |
| select 1000 rows                        |  1.51ms |  1.69ms |     1.12× |     318892 |   346381 |        6790 |       7794 |
| `One`                                   |  15.1µs |  16.0µs |     1.06× |       2022 |     6265 |          44 |         44 |
| stream 1000 rows (`Seq` vs a slice)     |  1.47ms |  1.69ms |     1.15× | **144402** |   346382 |        6784 |       7794 |

- **Building is where the difference is.** Running a query is mostly SQLite and
  `database/sql`, so 1.05–1.15× is the honest number there.
- **Memory is the wider gap.** A `One` holds a third of what bun does, and `Seq` streams
  1000 rows in 144 KB against the 346 KB bun needs for the slice it always builds.
- **The two are not doing quite the same thing.** bun formats every argument into the SQL
  text and runs the query with no arguments. barm sends placeholders and an argument list.
  That is part of why barm builds faster, and it is why bun cannot give the driver a
  reusable statement.

On Postgres, barm runs on pgx's pool and bun on `database/sql` over pgx, each as it is
meant to be used. Loading 100 authors with their relations, each relation 10 rows per
author:

|             |    barm |     bun |           |
| ----------- | ------: | ------: | --------: |
| 1 relation  | 498.1µs | 711.0µs | **1.43×** |
| 2 relations | 795.9µs |  1.34ms | **1.68×** |
| 3 relations |  1.11ms |  1.95ms | **1.76×** |

**Why a row costs one allocation, not one per column.** `reflect` copies an addressable
value on its way into an `any`, so binding N columns straight off your struct would cost N
allocations. barm reads them off one unaddressable copy of the row instead, which costs one
allocation however wide the row is. That took a 100-row insert from 412 allocations to 112,
and made it 28% faster. That copy also freezes the values at build time.

A primary key is usually one column, where copying the whole row costs more than boxing one
value. So `WherePK` boxes its key directly, and writes `"id" = $1` straight into the query
without allocating a fragment for it.

## Repository layout and tests

The main module has no dependencies. Everything that needs a driver is its own module, so
none of it reaches anyone importing barm:

```
barm/             the library, standard library only
barm/pgxdriver    the pgx pool driver
barm/integration  round trips against SQLite, through barm.SQL
barm/bench        the bun comparison, and prepared and batched reads on Postgres
```

```sh
go test ./...                     # SQL shapes
(cd pgxdriver && go test ./...)   # against Postgres
(cd integration && go test ./...) # against SQLite
(cd bench && go test -bench . ./...)
```

The pgx tests connect to `PGDSN`, which defaults to `postgres://postgres@localhost/postgres`.
