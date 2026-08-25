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
| `vgi.location` | yes | The worker to attach: a bare shell command (subprocess transport), `unix:///path/to.sock`, `tcp://host:port`, `http(s)://host:port/path` — an already-running HTTP server, unlike the other schemes (which each spawn or connect to their own worker instance per pooled connection) — or `launch:<argv>`, a shared warm worker every pooled connection (and any other process pointed at the identical `launch:` location) reuses rather than spawning its own. |
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
sound, not merely assumed.

**`launch:` (the shared-warm-worker transport).** `openTransport` recognizes
`launch:<argv>` — the first pooled connection to a given `(argv, cwd,
VGI_RPC_*-env)` tuple spawns a warm worker under a per-tuple `flock(2)`; every
other connection to the same tuple (this catalog's own remaining pooled
connections, or an entirely separate DuckDB process pointed at the identical
`launch:` location) reuses it. `<argv>` is tokenized with POSIX shell-quote
semantics (`LaunchLocationParser`), byte-for-byte matching the C++ extension's
own `ParseLaunchArgv` — this is what lets a Trino-launched worker be shared
with a DuckDB process: both sides only agree on which worker to reuse if they
tokenize the same location string into the same argv list, since that list is
exactly what gets hashed. The actual spawn/flock/discovery machinery lives in
`vgi-rpc-java`'s `farm.query.vgirpc.launcher` package (see that repo — this
connector only adds the `launch:` branch and the argv tokenizer), verified
there against a real spawned worker process, and here (`VgiLaunchTransportTest`)
against the real Python fixture worker: connectable, and a 4-connection pool
sharing one worker process rather than spawning four. Needs JDK 22+ (the
Foreign Function & Memory API — `flock(2)`, not `java.nio`'s `fcntl`-based
`FileChannel.lock()`, is the one syscall that actually interlocks with the
Python/C++ reference launchers); this connector requires JDK 25 regardless, so
that's never a real constraint here. Not covered by the transport-parameterized
conformance suites above — `launch:` resolves to a `unix://` connection under
the hood, so its distinguishing risk (hash stability, spawn-once-reuse-many,
idle-timeout self-shutdown) is orthogonal to query correctness, which is what
those suites actually exercise.

### Type mapping

`VgiTypeMapping` covers what VGI's own type helpers and most declarative
columns actually produce: signed/unsigned integers (unsigned widened to the
next signed Trino width; `UInt64` has no exact match and maps to `BIGINT`
with a documented wraparound caveat above `Long.MAX_VALUE`), both float
widths, UTF-8 strings, binary, booleans, dates, timestamps *without* a time
zone, 128-bit decimals, and arbitrarily nested `Struct`/`List`/`FixedSizeList`
(mapped to Trino `RowType`/`ArrayType`, recursively, in both the Arrow → Trino
read direction and — for scalar-function arguments and return values — the
reverse). Anything else (half-precision floats, timestamps *with* a time
zone, duration, `Map`) throws `UnsupportedOperationException` naming the
column and type rather than silently mis-typing it; the Trino → Arrow
direction additionally doesn't cover 128-bit decimals or writing a
`FixedSizeList` — see *Scalar functions*' own scope notes for why.

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

### Time travel

`FOR VERSION AS OF <expr>` / `FOR TIMESTAMP AS OF <expr>` on a plain declarative table works, backed
entirely by VGI's own `at_unit`/`at_value` wire fields — already present on `BindRequest` and on
`catalog_table_get`/`catalog_table_scan_function_get` before this connector ever used them.
`VgiMetadata.getTableHandle` converts Trino's `ConnectorTableVersion` (`VgiTimeTravel`) into that pair —
`PointerType.TARGET_ID` → `at_unit="VERSION"` (accepting any integer-family literal or a `VARCHAR` ref
name — Trino types a bare small-integer literal like `FOR VERSION AS OF 1` as `INTEGER`, not `BIGINT`,
confirmed by running it, not assumed) and `PointerType.TEMPORAL` → `at_unit="TIMESTAMP"` (accepting
`DATE`/`TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`, formatted as an ISO-8601 string) — modeled on Trino's own
Iceberg connector's real conversion logic (fetched from its source; this repo has no local Iceberg/Delta
connector to crib from directly) since there's no `getTableVersionType`-style declaration hook in this
Trino version to lean on instead.

Scoped narrower than Trino's SPI signature allows: only `endVersion` is supported (VGI's `at_unit`/
`at_value` is a single point, not a range — `startVersion` present is a clean `TrinoException`, not a
silently-wrong answer), and only plain table scans — `FOR ... AS OF` is a table-*reference* clause with no
syntax position on a `TABLE(catalog.schema.fn(...))` call, so table functions are unaffected.

Verified end to end (`VgiTimeTravelTest`) against the real reference fixture's own time-travel table,
`data.versioned_data` — not a fixture built for this connector, the same one every SDK's own conformance
suite already exercises — whose schema genuinely evolves across versions (`{id}` → `{id, name, score,
active}` → `{id, score}`), proving the resolved AT clause reaches both the schema-discovery RPCs and the
scan itself consistently, not just one or the other.

### Scalar functions

Connector-defined scalar functions (`ConnectorMetadata.getFunctions`/`Connector.getFunctionProvider`)
dispatch a real VGI worker's exchange-mode scalar call per row — `VgiScalarFunctions` discovers every
`SCALAR_FUNCTION` a catalog exposes (mirroring `VgiTableFunctions.discover`) and builds a real Trino
`Signature` per overload, rather than hardcoding one function.

**Row-at-a-time is a real Trino ceiling, not a shortcut taken here.** VGI's scalar protocol is
exchange-mode (client sends a batch, worker answers a batch 1:1), but Trino's scalar `MethodHandle`
calling convention has no batch-level hook anywhere — confirmed by reading the actual codegen path
(`CallColumnarFilterGenerator`'s `ForLoop` + per-row `invokeExact`, even on the "columnar" fast path) and
by two first-party open issues asking for one: [trinodb/trino#18758](https://github.com/trinodb/trino/issues/18758)
("Batch executed scalar UDFs", specifically about remote-service UDFs) and
[trinodb/trino#14237](https://github.com/trinodb/trino/issues/14237) ("Project Hummingbird", whose
batch-calling-convention task item is still unchecked). Trino's own strategy for cheap built-ins is a
tight, JIT-vectorizable per-row loop — it cannot help a genuinely remote worker, and there is nothing
this connector can do to change that without a batch convention Trino doesn't offer.

**Connection handling never holds a connection longer than one invocation** — the opposite of an
earlier spike (`193a7e4`) that opened one bind/init/exchange stream per `Driver` via Trino's
`instanceFactory` hook and kept it open for that instance's lifetime. That was a real bug, not a
shortcut: `instanceFactory` runs once per `Driver` (not once per query, confirmed by tracing
`PageFunctionCompiler` → `LocalExecutionPlanner` → `DriverFactory`), so N concurrent drivers times M call
sites can open far more connections than `vgi.connections` allows — and **Trino never calls any
lifecycle/close hook on the instance it produces**, so every held connection genuinely leaked until GC
eventually collected the whole operator graph. Every `invoke()` call here instead does one
`client.withConnection(...)` — borrow, `init()`, one `exchange()` turn, `session.close()`, release —
exactly mirroring the existing, already-tested table-scan pattern (`VgiSplitManager` binds once,
`VgiPageSource` redeems on a separately-borrowed connection per split). `instanceFactory` still produces
one object per `Driver` (an `Invoker`), but it holds no connection or open stream, so Trino never
cleaning it up is harmless.

The one thing worth caching: `bind()` itself, not the stream. `VgiScalarFunctions.BindCache` (bounded
LRU, catalog-scoped) keys on `(function, observed const-argument values)` and reuses a cached
`bind_call`/`opaque_data` across calls whose "constant" hasn't changed — Trino's `FunctionProvider` has
no channel to receive a constant argument's actual VALUE ahead of invocation (`BoundSignature` carries
only types), so a `vgi_const` argument arrives as just another value on the same row-at-a-time call; the
cache rebinds the moment the observed value changes and reuses the existing bind otherwise. A query
whose "constant" genuinely varies per row degrades to "no caching benefit" (a fresh `bind()` every row),
never wrong results. Net honest cost versus the old spike: one extra `init()` RPC per row (the bind RPC
is what gets amortized away, not `init()`+`exchange()`).

Also covered: overloads (`getFunctions` can return more than one `FunctionMetadata` per name — real
Trino overload resolution, not the table-function SPI's one-registration-per-name constraint), `any`-typed
arguments via `Signature.typeVariable` (each gets its own independent type variable — see below for why
this connector never ties one to the return type), null handling (`BOXED_NULLABLE`/`NULLABLE_RETURN`
declared honestly, with `ScalarFunctionAdapter.adapt` bridging to whatever convention Trino actually
requests at a given call site — the same pattern Iceberg/AI-functions use), nested `Struct`/`List`/
`FixedSizeList` arguments and return types (`RowType`/`ArrayType` ↔ Arrow, recursively — see *Type
mapping* above), and varargs (`Signature.variableArity()`, with the trailing `vgi_const`-ineligible
argument spec repeated to match each call site's actual resolved arity).

Verified end to end (`VgiScalarFunctionsTest`) against real fixture functions, not just one: `passthru`
(plain dispatch, null in/out), `multiply` (a `vgi_const` argument, including the bind-cache's
rebind-on-value-change behavior across two different const values), `any_mixed` (an `any`-typed argument
resolved against two real overloads), `type_info` (a plain, all-concrete-type 5-way overload set),
`null_handling` (confirms Trino calls through with the null rather than short-circuiting before ever
reaching the worker), `geo_distance_struct`/`geo_distance_list` (struct and list row arguments),
`binary_packet` (const binary and const struct arguments together), and `geo_centroid_struct` (varargs of
struct arguments, returning a struct — all three new capabilities at once).

**Deliberately out of scope, named rather than silently dropped:**

- **A dynamic (bind-time-computed) return type.** VGI's `on_bind` can compute a genuinely new output
  type from an argument's actual type (`double`'s int8→int64-style promotion, via `_promote_for_addition`)
  — but Trino resolves a function's return type from its static `Signature` alone, before any RPC ever
  happens, so this has no Trino representation at all. Detected at discovery time via the `vgi:any`
  output-field metadata key `ScalarFunction.catalog_output_schema` emits for it (note: a different key
  than the argument-side `vgi_type=any`) and skipped, not guessed at. This one isn't a scope choice —
  it's a ceiling of Trino's current function-resolution model, not something more engineering fixes. It
  quietly also excludes any function whose return type simply isn't *declared* statically even though
  it's fixed in practice (e.g. the reference fixture's own `sum_values`/`concat_values`, whose `on_bind`
  always returns the same concrete type but never says so via an explicit `Returns(type)` — indistinguishable
  from a genuinely dynamic one without executing `on_bind`, so treated the same, conservatively).
- **A colliding overload set is pruned, not fully exposed.** VGI's own Arrow type system distinguishes
  overloads (e.g. `int64` vs. `uint32`/`uint64`) that `VgiTypeMapping` widens onto the *same* Trino type
  (`BIGINT` — Trino has no unsigned integer type). Registering all of them would make every call
  ambiguous ("Could not choose a best candidate operator") — confirmed against the real fixture's 5-way
  `type_info` overload set, three of whose signatures collide this way. Only the first-discovered
  overload per distinct Trino `Signature` is registered; a colliding later one is unreachable from
  Trino, not silently wrong. Also a ceiling, not a scope choice.
- **128-bit decimal arguments/return**, in the Trino → Arrow (scalar) direction specifically. Trino's
  `BOXED_NULLABLE` representation for a decimal is a raw `Long` (short) or `Int128` (long) unscaled
  value, not a `BigDecimal` — bridging that correctly needs real, separately-verified `Int128` handling.
  `toTrinoType`'s read direction (declarative table columns) already supports it.

**Settings and secrets are supported, with one real, narrow gap.** `required_settings` maps onto Trino
session properties (`VgiConnector.getSessionProperties()` declares one nullable string property per
distinct setting name across every registered function; `SET SESSION <catalog>.<name> = '<value>'`
supplies it). `required_secrets` maps onto `ConnectorIdentity.getExtraCredentials()` — a flat,
client-supplied `Map<String, String>` (e.g. the JDBC/CLI `--extra-credential` flag) — via a
`vgi_secret.<secretKey>.<fieldName>=value` convention, letting one secret carry multiple fields
(confirmed necessary and working against the real fixture's `secret_field()`, whose `vgi_example` secret
needs both `port` and `secret_string`). Both are delivered into `VgiScalarFunctions.invoke` via a real
`ConnectorSession`, threaded through Trino's `supportsSession` invocation-convention flag — the same
mechanism Trino's own built-in `current_user()`/`current_path()` use, just reached from a
connector-defined function via `ScalarFunctionAdapter.adapt` instead of hand-written bytecode. A
function reading either is also marked non-deterministic (its result depends on session/identity state
outside its `Signature` arguments — discovered the hard way: without this, Trino's IR constant-folding
evaluated `multiply_by_setting(5)` at *plan* time, before a real per-catalog session existed).
Auth-context arguments (`whoami`) need no support at all — confirmed invisible on the wire entirely.

The one real gap: a settings/secrets-using function combined with a genuine per-row **column**
argument hits an actual Trino 483 columnar-bytecode-generation bug in the combination of
`supportsSession=true` with an adapted argument convention — confirmed by reading the generated
bytecode against the real fixture's `multiply_by_setting`/`scale_by_setting` (a missing unbox before an
`LSTORE` for a `BIGINT` return; a raw `Block`/`int` descriptor mismatch for a `DOUBLE` return). It does
**not** reproduce for a session-declaring function with zero regular arguments (`secret_field()`,
verified working end to end) or one whose argument comes from Trino's early constant-evaluation path
rather than a real column. Every function that doesn't itself need `supportsSession` is deliberately
kept off that path entirely (`VgiScalarFunctions.methodHandleNoSession`), so this bug cannot affect the
vast majority of functions — but `multiply_by_setting`/`scale_by_setting` called against a real column
are untested-and-known-broken today (`VgiScalarFunctionsTest`'s two `@Disabled` tests document exactly
this), pending either a Trino fix or a different bridging strategy this connector hasn't found yet.

- **A `FixedSizeList` argument, on write, IS supported** — `VgiTypeMapping#toArrowField`'s `hint`
  parameter carries the argument's original discovery-time Arrow field forward so a worker-declared
  `FixedSizeList` width survives round-tripping through Trino's width-erasing `ArrayType` (verified
  against the real fixture's `geo_distance_fixed`).
- **A colliding overload set is pruned, not fully exposed.** VGI's own Arrow type system distinguishes
  overloads (e.g. `int64` vs. `uint32`/`uint64`) that `VgiTypeMapping` widens onto the *same* Trino type
  (`BIGINT` — Trino has no unsigned integer type). Registering all of them would make every call
  ambiguous ("Could not choose a best candidate operator") — confirmed against the real fixture's 5-way
  `type_info` overload set, three of whose signatures collide this way. The registration prefers a
  lossless Arrow → Trino mapping over a lossy one when both collide (see *Scalar functions*' own
  discovery-time WARN logging above), falling back to first-discovered only when both are equally
  (non-)lossless.

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
- **Scalar functions with a dynamic (bind-time-computed) return type, a
  colliding overload set, or a 128-bit decimal argument/return** — see
  *Scalar functions* above for why each is skipped rather than registered
  wrong. Settings/secrets ARE supported now, except for the one narrow,
  real Trino-483-bytecode-bug gap documented there (a settings/secrets
  function called with a genuine per-row column argument). The
  `TABLE(...)`-argument/batch-path question for scalar functions (an
  entirely separate design, not this connector's row-at-a-time dispatch)
  is also still undecided.
- **Write support**, **multi-branch tables** (`catalog_table_scan_branches_get`),
  **transactions**, **views** — VGI supports all four; none are wired up here
  (write support matches vgi-java's own worker-SDK scope, which is read-only
  today). Time travel *was* on this list — see *Time travel* below for why it
  turned out to be a real, buildable feature rather than a genuine gap, once
  actually checked instead of assumed.
- **Custom ATTACH-time options.** `VgiWorkerClient.openAndAttach()` always
  calls `catalog_attach` with `null` options/init-opaque-data — a worker whose
  `catalog_attach` validates or depends on caller-supplied options (VGI's
  attach-options mechanism) has no way to receive any from this connector
  today. `vgi.catalog-name` is the only per-attach parameter this connector
  threads through.
- **Per-query settings and secrets for table functions.** `table_function_plan`/`init()`
  still always send `null`/`false` for `settings`/`secrets`/`resolved_secrets_provided` —
  this gap is now closed for *scalar* functions only (session properties / extra
  credentials — see *Scalar functions* above), not yet for table functions, which would
  need the equivalent wiring on the `plan()`/`init()` call sites instead.

## License

[Query Farm Source-Available License, Version 1.0](LICENSE) — the same license
[VGI](https://github.com/Query-farm/vgi) itself is released under. Free for
development, testing, and internal Production Use; see the license for the
narrow set of restricted uses (competing hosted offerings, commercial
marketplaces) that require a separate agreement with Query Farm.

Copyright 2026 Query Farm LLC - https://query.farm
