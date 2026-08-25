# vgi-trino

A Trino connector for [VGI](https://github.com/Query-farm/vgi) (Vector Gateway
Interface) workers — the same out-of-process, Arrow-IPC-based worker protocol
the `vgi` DuckDB extension attaches. One Trino catalog maps to one VGI
`ATTACH`, the same granularity DuckDB uses.

```sql
-- etc/catalog/vgi_example.properties:
--   connector.name=vgi
--   vgi.location=uv run --project ~/Development/vgi-python vgi-fixture-worker
--   vgi.catalog-name=example

SELECT * FROM vgi_example.data.numbers;
```

## Status

This is a working v1: catalog/schema/table discovery, callable VGI table
functions via Trino's `TABLE(catalog.schema.fn(args))` syntax, real
multi-split parallel scans via `table_function_plan` (for both declarative
tables and table functions), and projection pushdown are implemented and
verified end to end (`./gradlew :plugin:test`) against the reference Python
fixture worker and an in-process split-capable worker. Read-only. See
**Scope** below for what's deliberately not here yet.

```sql
-- A callable VGI table function:
SELECT count(*) FROM TABLE(vgi_example.main.split_sequence(n => 200, splits => 12));
```

## Configuration

Set per catalog in `etc/catalog/<name>.properties`:

| Property | Required | Description |
|---|---|---|
| `connector.name` | yes | `vgi` |
| `vgi.location` | yes | The worker to attach: a bare shell command (subprocess transport), `unix:///path/to.sock`, or `tcp://host:port`. `http(s)://` is not yet implemented. |
| `vgi.catalog-name` | yes | The VGI-side catalog to attach — one of the names the worker's `catalog_catalogs()` advertises (e.g. `example` for the reference fixture worker). **Not** the Trino catalog name (the properties filename) — that's a purely local alias the worker never sees, the same split DuckDB's own `ATTACH 'name' AS alias` makes. |
| `vgi.connections` | no (default 4) | Size of the pooled-connection set. VGI's RPC is lockstep per connection — one call in flight at a time — so this also caps how many splits can be redeemed concurrently against this catalog. |
| `vgi.target-split-size-bytes` | no | Passed to `table_function_plan` as `target_split_bytes`. |
| `vgi.min-splits` | no | Passed as `min_splits`. |
| `vgi.max-splits-per-response` | no (default 1000) | Pagination cap per `table_function_plan` call — Trino's own `getNextBatch(maxSize)` further clamps this per call. |

## Running it

**Tests** (needs `~/Development/vgi-python` checked out for the live
fixture-worker test; the in-process split test needs nothing external):

```bash
./gradlew :plugin:test
```

**Sqllogictest conformance** (`plugin/src/test/.../conformance/`): runs the
*actual* `.test` files from `~/Development/vgi/test/sql/integration/` — parsed
by a small in-repo sqllogictest reader, not hand-ported equivalents — with
their `ATTACH` rewritten to this connector's catalog and their real expected
output compared against a real query result. Only `table/rowid.test` is
ported so far — 8 of its 16 records are portable declarative-table `SELECT`s;
the rest are skipped with a reported reason (`DESCRIBE`, struct-typed rowid
access needing `ROW` type support, the `rowid_sequence(...)` calls, which now
that table functions exist could be ported too — using DuckDB's `:=` named-arg
syntax rather than Trino's `=>`, so the harness's naive `example.` rewrite
alone won't carry them over unmodified). Running the file against a real
worker is exactly what caught a real bug: this connector was resolving a
table's backing scan function in the table's OWN schema, but VGI doesn't
guarantee that — the fixture's `data.rowid_first` scans via
`main.rowid_sequence`, a different schema entirely — fixed by passing no
schema hint and letting the worker's own dispatcher search by name, matching
vgi-python's own schema-less `Client` fallback.

Most of the wider 327-file suite still can't run against Trino verbatim: ~119
files use DuckDB-only introspection (`duckdb_tables()`, `duckdb_databases()`,
`CALL enable_logging`, DuckDB's own `EXPLAIN (FORMAT JSON)` shape) with no
Trino equivalent, ever. The ~161 files using table-function CALL syntax are a
different story now that `ConnectorTableFunction` support exists (see *Table
functions* below) — porting those needs a DuckDB-`:=`-to-Trino-`=>` syntax
translation this harness doesn't do yet, not new connector functionality.

**Local Trino, fast dev loop** (downloads/caches the Trino server tarball once):

```bash
VGI_LOCATION="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
VGI_CATALOG_NAME=example \
    dev/run-local-trino.sh
```

**Docker Compose** (Trino + the connector + a Python fixture-worker sidecar,
over `tcp://` since subprocess transport can't cross container boundaries):
see `docker/docker-compose.yml`. Not yet exercised end to end as part of this
change — written to the same conventions as the tested pieces, but treat it
as a starting point.

**Packaging**: `./gradlew :plugin:assemblePluginDir` assembles the connector's
jar plus its runtime dependencies into `plugin/build/plugin/vgi/` — the
flat-directory-of-jars layout Trino's plugin loader expects, deliberately
without `trino-spi` (Trino serves that from its own shared classloader; every
plugin's jars get an isolated classloader built from exactly this directory).
`docker/Dockerfile` bakes that directory into a `trinodb/trino:483` image.

## Architecture

```
VgiPlugin -> VgiConnectorFactory -> VgiConnector
                                       |-- VgiMetadata            (catalog/schema/table discovery)
                                       |-- VgiSplitManager/Source  (table_function_plan, paginated)
                                       |-- VgiPageSourceProvider/  (init(), Arrow -> Trino Block)
                                       |   VgiPageSource
                                       \-- VgiFunctionProvider ->  (Trino's PTF SPI hook — see Table
                                           VgiTableFunction*        functions below)
      \-- VgiWorkerClient: a pool of independently catalog_attach'd connections
```

Depends on `farm.query:vgi` (the VGI Java client SDK) built from source via a
Gradle composite build against a sibling `../../vgi-java` checkout, since this
connector needs client-side additions (`TableFunctionPlanRequest`, the
`max_splits_per_response` plumbing) that repo only just gained. Falls back to
the published Maven Central artifact once a release with them ships.

### Splits

`table_function_plan`'s `max_splits_per_response` is modeled directly on
Trino's `ConnectorSplitSource.getNextBatch(maxSize)` — the protocol's own docs
say so — so `VgiSplitSource` paginates by calling `getNextBatch`'s `maxSize`
straight through as that field, following `next_cursors` until the plan is
exhausted. Each returned split's `location_ids`/`PlanResponse.locations` map
to `ConnectorSplit.getAddresses()`; `estimated_bytes` (relative to
`target_split_bytes`) maps to `SplitWeight`. A worker that never opted into
splitting returns the framework's own "whole scan" sentinel (a single split
with an empty token) — the split source recognizes it and the page source
falls back to a plain, non-split `init()`.

### Type mapping

`VgiTypeMapping` covers what VGI's own type helpers and most declarative
columns actually produce: signed/unsigned integers (unsigned widened to the
next signed Trino width; `UInt64` has no exact match and maps to `BIGINT`
with a documented wraparound caveat above `Long.MAX_VALUE`), both float
widths, UTF-8 strings, binary, booleans, dates, timestamps *without* a time
zone, and 128-bit decimals. Anything else (half-precision floats, timestamps
*with* a time zone, duration, list/struct/map nesting) throws
`UnsupportedOperationException` naming the column and type rather than
silently mis-typing it.

### Table functions

VGI's callable table functions (`example.sequence(count)`,
`example.split_sequence(n, splits)`) are reachable from Trino's Polymorphic
Table Function SPI (`ConnectorTableFunction`, wired in via
`Connector.getFunctionProvider()` — there's no direct hook on `Connector`
itself for the split-reading side, `TableFunctionProcessorProvider` /
`FunctionProvider` is it). `VgiTableFunctions.discover` lists every
`TABLE_FUNCTION` at connector-creation time and decodes each one's
`FunctionInfo.arguments` schema into `ScalarArgumentSpecification`s.
`VgiTableFunction#analyze` calls the real `bind()` for the invocation's actual
arguments (a function's output columns can depend on them —
`constant_columns(n, *values)`'s types follow `values` — so the return type is
declared `GenericTable` and the real `Descriptor` comes from `analyze`, not a
static declaration), and the resulting handle feeds the exact same
`VgiSplitSource`/`VgiPageSource` machinery a declarative table scan uses (a new
`ConnectorSplitManager.getSplits(..., ConnectorTableFunctionHandle)` overload,
and `VgiTableFunctionSplitProcessor`, the `TableFunctionSplitProcessor`
counterpart of `VgiPageSource`).

Three real wire-format lessons surfaced only by testing this against the real
worker, all fixed:

- Trino's PTF binder upper-cases an unquoted call-site argument name before
  matching it against the declared spec — `ScalarArgumentSpecification` names
  are registered upper-case, mapped back to VGI's own (case-sensitive) name
  when building the bind call.
- A `ScalarArgumentSpecification`'s default value (and `ScalarArgument.getValue()`
  at call time) must be in the TYPE's own native/internal representation
  (`Slice` for VARCHAR, raw int bits as a `long` for REAL) — not the plain Java
  value `ArgumentsEncoder` infers an Arrow type from.
- VGI's wire shape distinguishes POSITIONAL from NAMED arguments independently
  of how a Trino caller wrote the call: `sequence`'s sole argument is named
  `count` but is positional on the wire, so calling it as `sequence(count =>
  10)` must still reach the worker as a positional value (`ArgumentsEncoder.
  positional(...)`, in declared order) — sending it named throws `IndexError:
  Argument 0: index out of range`.

**v1 scope**: only plain-scalar-argument functions register at all — a
function with a varargs, `any`-typed, or TABLE-input argument (`VgiArgSpec.
decode` returns `null`) is skipped entirely rather than registered with a
wrong/partial signature, and a name VGI itself overloads (multiple
`FunctionInfo` entries under one (schema, name) — Trino's PTF model allows only
one registration per name, unlike VGI's own arity/type-resolved dispatch) is
skipped too rather than guessing which overload a caller meant.

## Scope — what v1 does not do yet

- **Predicate pushdown** (`applyFilter`). Deliberately deferred, not just
  unscheduled: getting the value-type translation (Trino's native
  per-type representation → VGI's Arrow-typed filter AST) wrong is not merely
  a missed optimization the way an imperfect projection would be — a worker
  function with `auto_apply_filters` on could silently drop rows a wrong
  filter excluded, and Trino's own re-check can't recover rows the worker
  never returned. That risk deserves dedicated test coverage (ideally against
  a fixture function with `auto_apply_filters=true` to prove exactness)
  before landing, not a rushed pass.
- **`http(s)://` transport.** Subprocess, `unix://`, and `tcp://` are
  implemented (all thin wrappers over `RpcConnection` + an existing
  `RpcTransport`); HTTP needs its own client (state tokens, request/response
  framing) that isn't wrapped anywhere yet.
- **Per-column statistics.** Row-count estimates flow from
  `catalog_table_get`'s own `cardinality_estimate` (no extra RPC); per-column
  min/max/distinct stats (`table_function_statistics`,
  `catalog_table_column_statistics_get` — both already decodable client-side
  via `ColumnStatisticsDecoder`) aren't wired into `getTableStatistics` yet.
- **Dynamic filtering.** `ConnectorSplitSource.getNextBatch`'s
  `DynamicFilterSnapshot` is accepted but unused; VGI's `refined_filters`/
  `filters_complete` fields exist for exactly this and aren't threaded
  through.
- **Overloaded table functions**, and any function with a varargs/`any`-typed/
  TABLE-input argument — see *Table functions* above for why these are
  skipped rather than registered wrong.
- **Write support**, **multi-branch tables** (`catalog_table_scan_branches_get`),
  **time travel**, **transactions** — VGI supports all four; none are wired
  up here (write support matches vgi-java's own worker-SDK scope, which is
  read-only today).
