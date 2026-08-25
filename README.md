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
tables and table functions), and projection, predicate, *and dynamic-filter*
pushdown are implemented and verified end to end (`./gradlew :plugin:test`)
against the reference Python fixture worker and in-process split-capable
workers. Read-only. See **Scope** below for what's deliberately not here yet.

```sql
-- A callable VGI table function:
SELECT count(*) FROM TABLE(vgi_example.main.split_sequence(n => 200, splits => 12));
```

## Configuration

Set per catalog in `etc/catalog/<name>.properties`:

| Property | Required | Description |
|---|---|---|
| `connector.name` | yes | `vgi` |
| `vgi.location` | yes | The worker to attach: a bare shell command (subprocess transport), `unix:///path/to.sock`, `tcp://host:port`, or `http(s)://host:port/path` — an already-running HTTP server, unlike the other three schemes (which each spawn or connect to their own worker instance per pooled connection). |
| `vgi.catalog-name` | yes | The VGI-side catalog to attach — one of the names the worker's `catalog_catalogs()` advertises (e.g. `example` for the reference fixture worker). **Not** the Trino catalog name (the properties filename) — that's a purely local alias the worker never sees, the same split DuckDB's own `ATTACH 'name' AS alias` makes. |
| `vgi.connections` | no (default 4) | Size of the pooled-connection set. VGI's RPC is lockstep per connection — one call in flight at a time — so this also caps how many splits can be redeemed concurrently against this catalog. Trino may schedule more concurrent splits than this allows (e.g. a `LIMIT` it can satisfy from the first few splits still starts redeeming others in parallel before it notices) — the excess queues as pending futures, not blocked threads, so raising this changes throughput, not correctness; see *Connection acquisition* below. |
| `vgi.connection-acquire-timeout-millis` | no (default 30000) | How long the catalog/table-metadata and scan bind+plan calls (`VgiWorkerClient.borrow`, used once per query) wait for a pooled connection before failing. Does NOT bound split redemption itself — that's non-blocking (`borrowAsync`) and has no timeout of its own; see *Connection acquisition* below. |
| `vgi.max-plan-pages` | no (default 1024) | Bound on `table_function_plan` pagination — a worker that never stops cursoring makes `VgiSplitSource` throw once this many pages have been fetched, naming the cap, rather than follow it forever or silently truncate. Matches the C++ VGI extension's own `vgi_split_plan_max_pages` default. |
| `vgi.http-bearer-token` | no | Static bearer token sent as `Authorization: Bearer <token>` on every request, for an `http(s)://` `vgi.location` that requires one. Ignored for every other transport. |
| `vgi.target-split-size-bytes` | no | Passed to `table_function_plan` as `target_split_bytes`. |
| `vgi.min-splits` | no | Passed as `min_splits`. |
| `vgi.max-splits-per-response` | no (default 1000) | Pagination cap per `table_function_plan` call — Trino's own `getNextBatch(maxSize)` further clamps this per call. |
| `vgi.dynamic-filtering-wait-timeout-millis` | no (default 1000) | How long `VgiSplitSource` asks Trino to hold its first `getNextBatch` call so a join's dynamic filter can arrive before any split is planned. `0` disables waiting. |

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

**`splits/*.test` fixture conformance** (`VgiSplitsFixtureConformanceTest`):
13 of the 20 files in that category hand-ported (not parsed — DuckDB's `:=`
call syntax isn't valid Trino SQL) into `TABLE(...)` calls against the SAME
reference Python fixture worker functions every other SDK's conformance suite
runs against (`split_sequence`, `split_paginated`, `split_fail_at`, ...). The
class's own javadoc lists exactly which 7 files were left out and why (three
categories: no Trino-side equivalent exists at all — DuckDB extension
settings like `vgi_split_scans`/`vgi_split_token_min_ttl_seconds`, or the
result cache; PTF predicate pushdown is architecturally impossible per the
*Table functions*/*Predicate pushdown* sections above; or the file's DuckDB
transaction-scope premise doesn't translate to `trino-testing`'s
single-statement-per-`Session.execute()` harness).

Running these against real infrastructure — not the DuckDB dialect this time,
Trino's own split-scheduling behavior — found two real, load-bearing bugs in
`VgiWorkerClient`'s connection pool, both fixed:

- **The pool never self-healed.** A connection a call failed against was
  evicted and never replaced — fine for one bad connection, but N cumulative
  failures over a catalog's *lifetime* (not per-query) drained a
  `vgi.connections=N` pool to zero, after which every subsequent `borrow()`
  blocked forever. Found by porting `poisoned_conn.test`/`errors.test`, whose
  whole point is deliberately failing one split and then asserting the next
  query is unaffected. Fixed: `release` now mints a fresh replacement in
  place of an evicted connection, so the pool's *size* is stable across a
  transient failure.
- **`borrow()` had no timeout at all.** Trino may schedule more concurrent
  splits than `vgi.connections` allows — a `LIMIT` satisfiable from the very
  first split still starts redeeming others in parallel before the engine
  notices it has enough — and the excess legitimately queue behind whichever
  connections are already busy. But queuing forever, on the Trino engine's
  own thread, is a different thing: once enough threads are stuck the same
  way, nothing is left to notice the `LIMIT` was already satisfied and cancel
  the rest, wedging the whole query (and everything sharing that thread
  pool). Found by porting `cancel_midsplit.test`'s abandonment shape (adapted
  as `abandonedScanLeavesThePoolUsable`, since its actual assertions are all
  DuckDB result-cache-specific). Fixed with a bounded, configurable acquire
  timeout (`vgi.connection-acquire-timeout-millis`) at the time — later
  replaced by the fully correct fix, non-blocking future-based acquisition
  using the SPI's own async escape hatches; see *Connection acquisition*
  below. The bounded timeout still guards the OTHER calls that borrow a
  connection once per query (metadata, bind+plan) rather than once per
  split, where blocking briefly is normal and expected.

**Local Trino, fast dev loop** (downloads/caches the Trino server tarball once):

```bash
VGI_LOCATION="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
VGI_CATALOG_NAME=example \
    dev/run-local-trino.sh
```

**Docker Compose** (Trino + the connector + a Python fixture-worker sidecar,
over `tcp://` since subprocess transport can't cross container boundaries):

```bash
./gradlew :plugin:assemblePluginDir
docker compose -f docker/docker-compose.yml up --build
# then, from a Trino client connected to the exposed 8080 port:
#   SELECT * FROM vgi_example.data.numbers;
```

Verified end to end (real cross-container split scans, `TABLE(...)` calls,
and filters — not just "the coordinator started"). Getting there for the
first time found three real bugs, all fixed and explained in
`docker/docker-compose.yml`/`docker/Dockerfile`/`plugin/build.gradle.kts`:

- Arrow's off-heap memory access needs `--add-opens`/`--add-modules` JVM
  flags this connector's own jars can't add for itself — a whole-JVM
  concern, so the `Dockerfile` now appends them to the base image's own
  `etc/jvm.config`. `dev/run-local-trino.sh` already had the same flags for
  its own launcher; the Docker path just hadn't needed them yet because
  nobody had run it.
- Trino loads every catalog eagerly and fatally at startup — a `depends_on`
  with no healthcheck only waits for the worker container to *start*, not
  for `uv run` to finish syncing dependencies and actually bind its port, so
  Trino tried to `ATTACH` before anything was listening and exited
  immediately. `docker-compose.yml` now has a real TCP healthcheck.
- Trino's own inter-task wire protocol carries this connector's
  `ConnectorSplit`/etc. as polymorphic Jackson payloads and needs
  `jackson-module-blackbird` resolvable from *this plugin's own* isolated
  classloader to build an optimized deserializer for them — without it, the
  very first real split redemption crashed the coordinator with
  `NoClassDefFoundError: ...blackbird.deser.CreatorOptimizer`. Fixed by
  adding that dependency and forcing every Jackson artifact in the plugin
  onto Trino 483's own version (`plugin/build.gradle.kts`) — arrow-vector's
  own transitive Jackson is older, and blackbird's generated bytecode isn't
  compiled against it.

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

### Connection acquisition

`VgiWorkerClient` pools `vgi.connections` independent worker connections
(VGI's RPC is lockstep per connection, so redeeming splits concurrently needs
that many independent ones). Redeeming a split — `VgiPageSource`/
`VgiTableFunctionSplitProcessor` — reserves one via `borrowAsync()`, never
the bounded-blocking `borrow()` metadata/planning calls use: construction
never occupies a Trino engine thread waiting its turn, because Trino may
schedule more concurrent splits than `vgi.connections` allows (a `LIMIT`
satisfiable from the very first split still starts redeeming others in
parallel before the engine notices it has enough) and those excess
reservations need to queue as pending futures, not blocked threads — the SPI
has exactly this escape hatch (`ConnectorPageSource.isBlocked()`,
`TableFunctionProcessorState.Blocked`), and both classes use it. Redemption
itself (`init()`, once a connection is assigned) runs on `VgiWorkerClient`'s
own dedicated executor via `thenApplyAsync`, deliberately never a bare
`thenApply` — without an explicit executor the continuation could run
inline on whichever thread completes the connection future, which is
`release()` (some unrelated split's teardown, or the pool's own self-healing
replacement logic), and none of those should end up unexpectedly running a
different split's `init()` RPC.

Closing a page source/split processor that hasn't been assigned a connection
yet withdraws its wait (`cancelPendingBorrow`) rather than cancelling the
redemption future outright — a plain `CompletableFuture.cancel()` doesn't
stop an already-running `thenApplyAsync` stage, so cancelling while
redemption is in flight would let it finish anyway (opening a real
connection the future can no longer publish) and leak both it and its
stream. A `closeRequested` flag makes redemption itself hand back a
connection it received too late instead.

Verified end to end (`VgiNonBlockingAcquisitionTest`): 100 splits over only
2 connections complete correctly even with `vgi.connection-acquire-timeout-millis`
set to 1ms — a value that would fail the old, `borrow()`-based construction
almost immediately under this same contention, since ordinary queueing delay
alone (each split takes real, if brief, time to drain before its connection
frees up) far exceeds 1ms.

### Transports

Subprocess, `unix://`, and `tcp://` all wrap `RpcConnection` over an
`RpcTransport` (a duplex byte stream) — `openTransport` picks the right one
and `VgiWorkerClient` treats them identically from there. `http(s)://` has
no such byte stream to speak of (a stream over HTTP is a chain of
independent request/response pairs, continuity carried by a state token in
the response body rather than a socket staying open), so it gets its own
connection type, `HttpRpcConnection` — already implemented in `vgi-rpc-java`
and designed for exactly this drop-in swap (identical `proxy(Class)`
surface), which is why wiring it up needed no changes anywhere outside
`VgiWorkerClient` itself: every call site already goes through `a.service()`,
transport-agnostic from the start. `Attached.connection` is typed as the
plain `AutoCloseable` both connection types share — the only thing the pool
itself needs from it (every RPC goes through `service` instead).

Unlike the byte-stream transports, an `http(s)://` `vgi.location` names an
already-running, independently-managed server — this connector doesn't spawn
or own its lifecycle, only connects to it. `vgi.connections` still pools that
many separate `HttpRpcConnection` instances (each its own `catalog_attach`),
matching every other transport's model, even though a single instance is
itself safe to share across concurrent calls — `HttpClient` is thread-safe,
and only an individual in-flight stream (`HttpRpcStream`) isn't. A single
shared connection serving unlimited concurrent splits over HTTP, instead of
pooling N, is a real efficiency opportunity this v1 leaves on the table in
favor of reusing the exact acquire/release/self-heal machinery every other
transport already goes through.

Verified end to end (`VgiHttpTransportQueryRunnerTest`) against the reference
fixture worker running as a real HTTP server (`vgi-fixture-http`, a separate
process this test starts and tears down itself, since HTTP workers aren't
spawned per connection): a plain table scan, a filtered scan, a `TABLE(...)`
call, and a self-join proving multiple pooled connections all independently
attach successfully.

**Conformance coverage across transports.** The ported sqllogictest-derived
suites (`VgiSqlLogicTestConformanceTest`, `VgiSplitsFixtureConformanceTest`)
run their same assertions over all four transports above —
`VgiWorkerHarness` starts the real reference worker each transport's way
(subprocess unchanged; `unix://`/`tcp://` spawn it directly and block-read
its `UNIX:`/`TCP:` stdout discovery line; `http://` reuses the port-file
pattern) and four thin subclasses per suite supply it via one `startWorker()`
hook, so the same ~15 assertions per suite run for real over every transport
rather than only subprocess. `unix://`/`tcp://`'s pooled 16-connection
catalogs are, by construction, the first exercise anywhere in this test tree
of one real worker process serving many concurrent pooled clients — proven
sound, not merely assumed. `launch:` (the shared-warm-worker transport) has
no coverage here because it isn't implemented — see Scope.

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

### Statistics

`VgiMetadata.getTableStatistics` reports row count from `catalog_table_get`'s
`cardinality_estimate` (already fetched with the table itself — no extra
RPC) plus real per-column min/max/distinct-count/nulls, from
`catalog_table_column_statistics_get` decoded via the existing
`ColumnStatisticsDecoder`. The RPC is unconditional rather than gated on some
advertised capability: a worker with nothing to say answers empty bytes,
which the decoder reads as "no statistics" rather than an error, so a table
that never declared any degrades to row-count-only (or fully empty) exactly
as before this existed — verified against a real fixture table with no
`statistics=` block (`VgiTableStatisticsQueryRunnerTest`).

Only the numeric case populates a `DoubleRange`: VGI's min/max are boxed
Java values matching the column's own Arrow type (`Long` for an integer
column, `String` for UTF-8, WKB `byte[]` for geometry), and a non-`Number`
value correctly yields no range rather than a meaningless coercion —
`distinctValuesCount`/`nullsFraction` still populate regardless of type.
`nullsFraction` itself only ever resolves to exactly `0.0` or `1.0`: VGI's
wire shape carries `has_null`/`has_not_null` as two independent booleans,
which can say "some of both" but not what fraction — reporting a guessed
number there would mislead the cost model more than reporting unknown does.

Verified end to end against the reference fixture worker's `data.numbers`
table, whose statistics are extracted from a real DuckDB `ANALYZE`-style
pass (`statistics_from_duckdb()`), not hand-authored test numbers.

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

### Predicate pushdown

`VgiMetadata.applyFilter` intersects Trino's `Constraint.getSummary()`
(`TupleDomain<ColumnHandle>`) into the table handle's own carried
`TupleDomain<VgiColumnHandle>`, returning `Optional.empty()` once a fixed
point is reached (required — Trino calls `applyFilter` repeatedly until it
stops changing anything, and returning a "new" handle that's actually
unchanged loops forever). The constraint travels unevaluated inside the
handle from planning through both places it needs to reach the wire:
`VgiSplitManager` (into `table_function_plan`'s `pushdown_filters`, so
splits can be pruned/sized before any row is read) and
`VgiPageSourceProvider`/`VgiPageSource` (into `init`'s `pushdown_filters`,
per redeemed split) — both via the same `VgiFilterEncoding.encode(constraint,
fullSchema, projectedColumns)` call, so the two never drift apart on how a
column index is resolved.

`ConstraintApplicationResult`'s `remainingFilter` is always the constraint's
own full summary, never `TupleDomain.all()` — `VgiFilterTranslator` only
handles a deliberately narrow set of Arrow types (signed integers, UTF-8,
binary, boolean, and `DOUBLE`-width floats; nothing else), and a table's
`auto_apply_filters` flag is a worker-side promise, not something this
connector can verify column-by-column. Telling Trino the filter isn't
provably exact means Trino always re-checks it, which is safe (redundant
re-filtering on an already-correct row) in every case where the push either
did nothing or did exactly the right thing — the only case that would ever
matter is the one this design can't tell apart from those two, so it costs
nothing to always play it safe.

That safety net is why this was deferred past the rest of v1: a wrong
Trino-value → VGI-filter-AST translation, run against a worker that trusts
pushed filters enough to prune rows *before* Trino ever sees them
(`auto_apply_filters=true`), doesn't produce a wrong answer Trino's own
re-check would fix — it produces missing rows Trino never had a chance to
recheck, silently. `VgiFilterPushdownQueryRunnerTest` is written against
exactly that fixture (`example.data.filter_echo_table`), and every filtered
assertion also checks the table's own `pushed_filters` echo column isn't the
`"(none)"` sentinel — proving a filter genuinely reached and was evaluated by
the worker, not merely that the (correct) result happened to match without
one.

Building this test surfaced one real, pre-existing bug unrelated to filter
translation itself: `SELECT count(*) FROM filter_echo_table` returned `0`
rows instead of `100`. Trino resolves `count(*)` to a *zero-column*
projection, and both `VgiSplitManager` and `VgiPageSource` were encoding "no
desired columns" as an explicit `projection_ids=[]` — which this table's real
`projection_pushdown=true` scan function (correctly) reads as "return zero
columns," not "no restriction, return everything." Fixed in both places by
treating an empty desired-columns set as `projection_ids=null` instead.

### Dynamic filtering

A join's build-side values reach a VGI scan the same way a literal `WHERE`
does: `VgiSplitSource.getNextBatch` and `VgiPageSourceProvider.createPageSource`
each receive Trino's own `DynamicFilterSnapshot`/`DynamicFilter`, and both
intersect its `TupleDomain` with the scan's static `applyFilter` constraint
before running it through the very same `VgiFilterTranslator`/
`VgiFilterEncoding` a literal predicate uses. There's no separate encoding
path for "arrived from a join" versus "arrived from a `WHERE` clause" — Trino
already reduces a join's build side to a `TupleDomain` (a discrete value set
when small enough, otherwise a min/max range), which is exactly the shape the
translator already handles.

Two different moments need this, for two different reasons:

- **Plan time** (`VgiSplitSource`): the FIRST `getNextBatch` call (no cursor
  yet) sends everything currently known as plain `pushdown_filters`; a LATER
  page sends only what's narrowed further since the previous call, as
  `refined_filters` — which the protocol documents as narrowing future splits
  only, so a split already emitted under a looser filter is never invalidated
  by one. `getRequestedDynamicFilterWaitTimeoutMillis` (new `vgi.
  dynamic-filtering-wait-timeout-millis` config, default 1000ms) asks Trino to
  hold that first call until the filter completes or the timeout elapses —
  without it, a broadcast join's build side often hasn't finished before
  planning would otherwise start, and a split already emitted can't be
  narrowed retroactively.
- **Redemption time** (`VgiPageSourceProvider`): every split's `init()` gets a
  freshly merged filter at the moment it's actually redeemed, independent of
  which `table_function_plan` page produced it — this, not the plan-time
  bookkeeping above, is what actually guarantees a worker with
  `auto_apply_filters=true` prunes correctly in every split, including ones
  from later pages a slow-to-collect filter couldn't have reached in time to
  plan around.

`VgiDynamicFilteringQueryRunnerTest` proves both matter: an in-process
vgi-java fixture (split-capable, `auto_apply_filters=true`, since the
reference fixture workers' only comparable function — `split_dynamic_filter`
— is a callable table FUNCTION, and Trino's PTF SPI has no filter hook at
all; see *Table functions* below) is forced to paginate `table_function_plan`
across 6 pages (30 splits, 5 per page) while a real `JOIN` against a small
`VALUES` build side runs. Every surviving row, from every page, reports the
identical real filter — not just the first page's rows.

Landing this also surfaced a real, pre-existing gap in `vgi-java` itself:
`VgiServiceImpl.planRequestOf` decoded `pushdown_filters` but silently dropped
`refined_filters`/`filters_complete` from every `table_function_plan` request
— any JVM-hosted worker's `plan()` was simply never told about a
continuation's narrowing. Fixed upstream (`vgi-java`) by merging the two
decoded filter lists (both are top-level, implicit-AND lists on the wire, so
merging is concatenation) into the same `PlanRequest.pushdownFilters` a
fixture author already reads, and adding `PlanRequest.filtersComplete`.

### Scope

Dynamic filtering here is a plain-table-scan-only feature: `ConnectorTableFunctionHandle`
is a bare marker interface and `TableFunctionProcessorProvider.getSplitProcessor`
takes no filter of any kind, so a PTF-sourced split source is always built
with `TupleDomain.all()` — there is nothing to thread through even in
principle, not merely something left undone (see *Table functions* below for
the same boundary on static predicate pushdown).

## Scope — what v1 does not do yet

- **Predicate/dynamic-filter pushdown for table functions.** Not deferred —
  impossible via Trino's current PTF SPI; see *Table functions* above.
- **Overloaded table functions**, and any function with a varargs/`any`-typed/
  TABLE-input argument — see *Table functions* above for why these are
  skipped rather than registered wrong.
- **Write support**, **multi-branch tables** (`catalog_table_scan_branches_get`),
  **time travel**, **transactions**, **views** — VGI supports all five; none
  are wired up here (write support matches vgi-java's own worker-SDK scope,
  which is read-only today).
- **Custom ATTACH-time options.** `VgiWorkerClient.openAndAttach()` always
  calls `catalog_attach` with `null` options/init-opaque-data — a worker whose
  `catalog_attach` validates or depends on caller-supplied options (VGI's
  attach-options mechanism) has no way to receive any from this connector
  today. `vgi.catalog-name` is the only per-attach parameter this connector
  threads through.
- **`launch:` transport** (the shared-warm-worker/launcher protocol) — not implemented; `openTransport`
  doesn't recognize the scheme at all. A from-scratch implementation (canonical-JSON tuple hashing,
  `flock(2)`-based coordination, spawn-if-needed discovery) belongs in `vgi-rpc-java`, not here.
- **Per-query settings and secrets.** `table_function_plan`/`init()` always
  send `null`/`false` for `settings`/`secrets`/`resolved_secrets_provided` —
  settings-aware and secret-scoped worker functions have no path to receive
  either from this connector. Trino has no automatic SET-style pass-through,
  but its per-session/catalog properties could plausibly be wired to VGI
  settings; that's unbuilt, not merely untested.
