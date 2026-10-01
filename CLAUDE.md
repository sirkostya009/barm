# barm

A query builder and ORM for `database/sql`, built on Go 1.27 generic methods and
iterators. It is meant to replace bun in our own services.

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

| file                                            | what it holds                                                   |
| ----------------------------------------------- | --------------------------------------------------------------- |
| `db.go`                                         | `DB`, `Handle`, the shared `session`, the exported interfaces   |
| `conn.go`                                       | `Conn` — one connection held out of the pool                    |
| `tx.go`                                         | `Tx`, nested transactions as savepoints                         |
| `select.go` `insert.go` `update.go` `delete.go` | the builders, one per file with its private constructor         |
| `raw.go`                                        | `RawQuery` — hand-written SQL as a `Query`                      |
| `runner.go`                                     | `runner` — what every builder ends in, with `one`/`slice`/`seq` |
| `build.go`                                      | SQL text assembly, the `WITH` clause                            |
| `scan.go`                                       | reflect-based row scanning                                      |
| `schema.go`                                     | model cache, struct tag parsing                                 |
| `relation.go`                                   | `Relation[U]`, key predicates, grouping                         |
| `batch.go`                                      | `Batch`, `Batcher`, transaction statements, driver registration |
| `hook.go`                                       | `QueryHook`, `TxHook`                                           |
| `dialect.go`                                    | `Dialect` and the three built-ins                               |
| `stmt.go`                                       | prepared statement cache                                        |

A method starting a query on a handle (`Select`, `NewRaw`, `Batch`, …) lives in
that handle's file.

`pgxdriver/` registers pgx via a side-effect import, as a `Batcher` and as a
`StmtDriver` that prepares a named statement in the same round trip as its first
run.

## Conventions

Builders are pointers changed in place: every call hands back the same builder,
rather than a copy of it. Building two queries off one partial query takes
`Clone`. A builder that fails records the error and carries it to the terminal
method rather than panicking.

Terminals come in pairs: the plain one returns the builder's own type, the `As`
one takes a type parameter for a different result shape. Both go through
`Build`/`BuildAs`.

A `Tx` holds its own `*sql.Conn` from begin until commit or rollback. This is
not incidental — a `*sql.Tx` has no `Raw`, so without the connection a
transaction cannot reach the driver and cannot batch. Nested transactions are
savepoints on that same connection.

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

Every read goes through `*sql.Rows` and maps columns by the names the result
reports; `*sql.Row` is not used anywhere. A `nullzero` or `json` column reads
through a `holder` from the scan plan, and a plan without one pays nothing.

Struct size is part of performance: a builder that grows past an allocator size
class costs B/op on every query, and a B/op regression is a reason to rework or
revert a change. Check `unsafe.Sizeof` before adding a field to a builder.

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
noise. Compare two versions by building a test binary of each and interleaving
their runs, then reading the result with `benchstat`; separate runs have shown
10% swings that were not there.

Run `gofmt -l`, `go vet`, `go fix` and `golangci-lint run ./...` before calling a
change done.

## Direction

The open design question is a backend abstraction: a `database/sql`
implementation in the core and a native `pgxpool` one in `pgxdriver`, with
`Rows`, `ExecResult`, `Batcher` and `StmtDriver` promoted from what the `Raw`
tunnel reaches to the front door. It would make arrays and JSON work natively
on Postgres, but `DB`, `Tx` and `Conn` stop embedding `*sql.DB`/`*sql.Tx`/
`*sql.Conn`, so it needs a written design before code. Until then, Postgres
array columns cannot be read through the `database/sql` path.
