# barm

A query builder and ORM built on Go 1.27 generic methods and iterators, running
on pgx's pool or on `database/sql`. It is meant to replace bun in our own
services.

## Ground rules

**The main module has zero dependencies.** Nothing outside the standard library
may enter `go.mod` at the repo root. Drivers, benchmarks and integration tests
live in their own modules (`pgxdriver/`, `bench/`, `integration/`) and may depend
on whatever they need. If something seems to need a dependency in the core, it
belongs in one of those instead.

**Generic methods cannot go in interfaces.** `interface method must have no type
parameters` is a hard compiler rule, so anything generic is a method on a
concrete type or a free function taking the receiver. `Handle`, `IDB`, `Querier`
and `TypedQuery[T]` exist to give callers the non-generic surface.

**Performance is the point.** Allocations per query, round trips per operation
and statement reuse are the numbers that matter. Measure before claiming an
improvement, and use a real profiler rather than reasoning about where the
allocations are — that reasoning has been wrong here before.

## Layout

| file                                            | what it holds                                                     |
| ----------------------------------------------- | ----------------------------------------------------------------- |
| `db.go`                                         | `DB`, `Handle`, the shared `session`, the exported interfaces     |
| `driver.go`                                     | `Executor`, `Pool`, `DriverConn`, `DriverTx`                      |
| `sqldriver.go`                                  | `SQL` — the `Pool` over `database/sql`                            |
| `conn.go`                                       | `Conn` — one connection held out of the pool                      |
| `tx.go`                                         | `Tx`, nested transactions as savepoints                           |
| `select.go` `insert.go` `update.go` `delete.go` | the builders, one per file with its private constructor           |
| `raw.go`                                        | `RawQuery` — hand-written SQL as a `Query`                        |
| `runner.go`                                     | `runner` — what every builder ends in, with `one`/`slice`/`seq`   |
| `build.go`                                      | SQL text assembly, the `WITH` clause                              |
| `scan.go`                                       | reflect-based row scanning                                        |
| `schema.go`                                     | model cache, struct tag parsing                                   |
| `relation.go`                                   | `Relation[U]`, key predicates, grouping                           |
| `batch.go`                                      | `Batch`, its results, and the statements a batch transaction uses |
| `hook.go`                                       | `QueryHook`, `TxHook`                                             |
| `dialect.go`                                    | `Dialect` and the three built-ins                                 |
| `array.go`                                      | Postgres array literals, for the `array` tag                      |
| `values.go`                                     | `ValuesQuery` — a VALUES list from structs, and its casts         |
| `stmt.go`                                       | statement names, and `database/sql`'s prepared statement cache    |

A method starting a query on a handle (`Select`, `NewRaw`, `Batch`, …) lives in
that handle's file.

`pgxdriver/` is the `Pool` over pgx's own pool: pgx's codecs, and batches and
named statements sent through pgconn pipelines of its own, one round trip each.

## Conventions

Builders are pointers changed in place: every call hands back the same builder,
rather than a copy of it. Building two queries off one partial query takes
`Clone`. A builder that fails records the error and carries it to the terminal
method rather than panicking.

Terminals come in pairs: the plain one returns the builder's own type, the `As`
one takes a type parameter for a different result shape. Both go through
`Build`/`BuildAs`.

barm reaches the database only through the `driver.go` interfaces: a `DB` runs
on a `Pool`, a `Conn` on a `DriverConn`, a `Tx` on a `DriverTx`. Nothing in the
core calls `database/sql` except `sqldriver.go`, so batching, prepared statements
and column types are each driver's to get right. Nested transactions are
savepoints, sent on the driver transaction.

barm hands a driver the SQL and its arguments, and for a `Prepare`d query the
name too, and lets the driver decide the rest. On pgx that means the pool's
`DefaultQueryExecMode` — pgx's statement cache unless configured otherwise — is
how a plain query runs; barm never overrides it.

While a hook watches a call on a DB opened `WithCallStats`, the ctx barm passes
carries somewhere to report on it, `CallStatsFrom(ctx)`, which is a type check
on that ctx: a driver asks with the ctx it was given, and does nothing extra
when it gets nil.

Batches and named queries on pgx are the driver's own pipelines, because pgx's
batch spends a round trip describing statements and another on a savepoint that
must exist before anything can fail. Every batch is one round trip: Postgres runs
the messages in order, so a statement's Parse goes right before its first run,
after the batch's SAVEPOINT. Arguments are always encoded for the parameter types
the server reports, never blind: a blind first run stores a different
`time.Time` in a `timestamp` or `date` column than every later run, and cannot
encode a map for `jsonb` at all. So a statement with arguments needs its types
before its first Bind, and the only source is a describe. The pool shares the
types across connections by SQL, so that describe happens once per statement per
pool, inside a transaction under a savepoint of its own; a connection meeting a
known statement parses it with those types in the same flush that runs it. A
statement that fails is forgotten by the connection and the pool, so stale types
cost one failed call.

Transaction hooks fire once per transaction and never on a savepoint, because
their reason for existing is cache invalidation and a cache invalidates when the
outermost commit lands.

Nothing is inferred. A column is a field whose tag names it, a primary key is a
field tagged `pk` (a column named `id` is not one), and `UPDATE`/`DELETE` pick
rows only through `Where` or `WherePK`. Prefer an explicit call or tag over a
convention barm would have to guess at.

A slice is one bind argument, which pgx sends as a Postgres array — match it
with `= ANY(?)`. `In(slice)` is the opt-in that spells it out inside the
parentheses the SQL wrote. A `Query` passed as an argument renders in place and
shares the enclosing query's placeholder numbering.

Every read goes through the driver's `Rows` and maps columns by the names the
result reports; `*sql.Row` is not used anywhere. A `nullzero`, `json` or `array`
column reads through a `holder` from the scan plan, and a plan without one pays
nothing.

The `json` and `array` tags exist for `database/sql`, which has neither type.
A pool and rows reporting `NativeJSON` or `NativeArrays` get those values as
they are — pgx's codecs do better — except that a nil one is still written as
NULL. They are separate so a driver can take one and not the other. pgx's pool
reports both only in modes that type every argument: under exec mode or the
simple protocol a plain query's arguments go untyped, and pgx can neither tell a
map is JSON nor encode a slice of slices.

A `VALUES` list casts its first row on Postgres, to the field's `type:` tag or
else the type its Go type maps to: an untyped parameter there is `text`, which
no join to a non-text column survives, and which pgx refuses to encode an
`int64` for. The mapping is computed once per field, with the model.

`From` and `Using` tables ride in the update's `sets` and the delete's `wheres`,
flagged in `frag.kind`, rather than in slices of their own that would push the
builders past their size class.

An insert's ON clause and its `Select` source share `insertExtra`, behind the
one pointer the ON clause had, sized to stay in the 80-byte class it was in.

Struct size is part of performance: a builder that grows past an allocator size
class costs B/op on every query, and a B/op regression is a reason to rework or
revert a change. Check `unsafe.Sizeof` before adding a field to a builder. A
closure that captures a query's runner puts the whole query on the heap, so
helpers that need one take the runner by value.

Comments explain why, not what. Do not leave a comment describing something that
is no longer in the code.

## Testing

Unset `GTK_IM_MODULE_FILE` before running Go tests.

```sh
unset GTK_IM_MODULE_FILE
go test -race ./... && (cd pgxdriver && go test -race ./...) && (cd integration && go test -race ./...)
```

`integration/` runs against sqlite by default. The Postgres tests and the
Postgres benchmarks read `PGDSN` and skip when it is unset. Run them against a
throwaway database rather than one that holds anything:

```sh
createdb -h localhost -U postgres barm_verify
(cd pgxdriver && PGDSN=postgres://postgres@localhost/barm_verify go test -race ./...)
dropdb -h localhost -U postgres barm_verify
```

A test has to be able to fail. After writing one that passes, break the thing it
covers and confirm it goes red — several tests here passed either way until that
check caught them. For anything about what reaches the database, watch the wire
(`log_statement=all`, or a `QueryHook`) rather than trusting the builder's
output.

Benchmark differences of a few percent are below this machine's run-to-run
noise, and code layout alone moves them further: adding code no benchmark calls
has made a select build 8% slower. Before blaming a change for a few percent,
benchmark the old version with only the new code added and nothing calling it. Compare two versions by building a test binary of each and interleaving
their runs, then reading the result with `benchstat`; separate runs have shown
10% swings that were not there.

Run `gofmt -l`, `go vet`, `go fix` and `golangci-lint run ./...` before calling a
change done.
