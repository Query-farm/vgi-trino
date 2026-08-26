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
by a small in-repo sqllogictest reader (`SqlLogicTestFile`/`SqlLogicTestRunner`),
not hand-ported equivalents — with their `ATTACH` rewritten to this connector's
catalog and their real expected output compared against a real query result.
Two files are pinned with exact skip/executed-count assertions as a stable
regression check (`table/rowid.test`, `catalog/window_self_join.test` —
see `VgiSqlLogicTestConformanceTest`); `VgiSqlLogicTestCensusTest` is a
looser, unasserted measurement pass across the *entire* 327-file corpus, run
on demand (`./gradlew :plugin:test --tests
"farm.query.vgitrino.conformance.VgiSqlLogicTestCensusTest"`) to see where
things actually stand.

Before executing each record, `SqlLogicTestRunner` rewrites four DuckDB-only
syntax forms into their Trino equivalents (in `rewriteDuckDbOnlySyntax`/
`CastRewriter`) — none of these are new connector functionality, just making
sure a parser failure means an actual gap rather than "the harness never
translated the SQL":
- A bare table-function call used as a table reference —
  `schema.function(args)` — becomes Trino's required
  `TABLE(schema.function(args))`.
- DuckDB's `name := value` named-argument syntax becomes Trino's
  `name => value`.
- DuckDB's builtin row generators, `range(...)`/`generate_series(...)`, used
  bare as a table reference, become Trino's `UNNEST(SEQUENCE(...))` idiom —
  Trino has no `range()` table function of its own, and the translation needs
  two corrections, not just a name change: DuckDB's `range` is exclusive of
  its stop bound (Python semantics) where `sequence` is inclusive, and
  Trino's 2-argument `sequence(start, stop)` silently auto-detects
  ascending-vs-descending when no step is given (`sequence(1, 0)` returns the
  descending `[1, 0]`, not zero rows) — so the rewrite always emits an
  explicit step to disable that guessing.
- DuckDB's `expr::TYPE` postfix cast syntax (Trino has no such operator at
  all) becomes `CAST(expr AS TYPE)`. Unlike the other three, a cast can
  appear anywhere in an expression, not just after a fixed keyword, and the
  type name itself sometimes needs translating too (`BLOB` → `VARBINARY`,
  `TIMESTAMPTZ` → `TIMESTAMP WITH TIME ZONE`) — hand-rolling that expression
  grammar isn't worth it when a maintained SQL-dialect library already gets
  it right, so `CastRewriter` shells out to Python's
  [sqlglot](https://github.com/tobymao/sqlglot) for this one. It runs
  *after* the other three rewrites, deliberately — sqlglot's own
  `duckdb -> trino` rule for bare `range()` calls is confirmed buggy (it
  drops the query's column alias and reuses the table alias as the column
  name instead, turning `SELECT i FROM range(10) t(i)` into an unresolvable
  column reference), so this harness never lets sqlglot see a bare `range()`
  call — by the time it runs, `SqlLogicTestRunner` has already turned it into
  `UNNEST(SEQUENCE(...))`, which sqlglot correctly leaves untouched. This is a
  best-effort, optional dependency, not a hard one: if sqlglot isn't
  available, `CastRewriter` falls back to unrewritten SQL (one warning, not a
  failure) — install it into a dedicated venv (kept out of the system Python
  deliberately, since Homebrew's Python refuses a bare `pip install` under
  PEP 668) with:
  ```bash
  python3 -m venv ~/.venvs/vgitrino-sqlglot
  ~/.venvs/vgitrino-sqlglot/bin/pip install sqlglot
  ```

Running the real files against a real worker is exactly what caught a real
bug along the way: this connector was resolving a table's backing scan
function in the table's OWN schema, but VGI doesn't guarantee that — the
fixture's `data.rowid_first` scans via `main.rowid_sequence`, a different
schema entirely — fixed by passing no schema hint and letting the worker's
own dispatcher search by name, matching vgi-python's own schema-less `Client`
fallback.

As of this writing, the full-corpus census (327 files) reports 803 records
executed, 1906 skipped as known-non-portable (DuckDB-only introspection —
`duckdb_tables()`/`vgi_catalogs()`/etc., a second unattached ATTACH-alias
per test file, the dynamic-code-injection feature, `CALL enable_logging`,
DuckDB's own `EXPLAIN` plan shape, `QUALIFY`, and the confirmed PTF
filter/order/column-pushdown SPI ceiling below — none with a Trino
equivalent, or a fixable-only-upstream one, at all), and 2765 still failing
— `PARSE_ERROR` (948), `UNSUPPORTED` (1074 — e.g. `echo`/`constant_columns`,
whose argument shapes this connector's table-function registration doesn't
support yet), `OTHER_RUNTIME_ERROR` (681, e.g. write-support-only features
like `COPY`), `QUERY_MISMATCH` (33), and `EXPECTED_ERROR_DIDNT_HAPPEN` (29).

**Trino-native test adaptations** (`VgiTrinoAdaptationsTest`,
`plugin/src/test/resources/trino-adaptations/`): hand-written, genuinely
Trino-native `.trino.test` files (same sqllogictest format, no DuckDB
syntax at all — no rewriting needed) for the specific real DuckDB tests
that structurally can't be reached by rewriting the original, but whose
underlying connector behavior IS already working and IS still testable a
different way. The clearest case: `filter_echo`/`order_echo` (and
siblings) verify pushdown by having the worker echo back what
filter/order/limit info it received — unreachable through Trino at all
(the PTF SPI ceiling above), but the underlying scan + Trino's own
engine-level filter/sort/limit still need to produce the *correct rows*
regardless of whether that information ever reached the worker, which is
exactly what these adaptations verify instead. Deliberately narrow scope
— see the class's own javadoc for when a hand-written adaptation is (and
isn't) worth adding; it gives up the "any new upstream test case is
covered for free" property the rewrite-based census leans on, so it's
reserved for cases that property can't reach anyway.

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

**Settings and secrets for declarative tables.** A DECLARATIVE table's backing scan function
(`Table(function=...)`, scanned via a bare `SELECT * FROM catalog.schema.name`, never a `TABLE(...)`
call) has its `required_settings`/`required_secrets` resolved into the FIRST `bind()` this
manager's plain-table `getSplits` issues — reusing `VgiScalarFunctions.BindCache#resolveSettings`/
`#resolveSecretFields`/`#encodeSecrets` verbatim, exactly like a scalar function's or classic
table-in-out's. The one real wrinkle: `catalog_table_scan_function_get` (the RPC that resolves a
table's backing function) carries no `required_settings`/`required_secrets` fields at all — only
`function_name`/`arguments`/`required_extensions` (confirmed against the real wire record,
`TableScanFunctionGetResponse.java`). Those two fields exist only on `FunctionInfo`, discoverable
through `catalog_schema_contents_functions(type=TABLE_FUNCTION)` — `VgiTableScanFunctions.discover`
makes one more pass over that same data once at catalog-attach time (no extra per-query RPC),
keyed by function name alone since `TableScanFunctionGetResponse` never says which schema its
function lives in (the reference fixture's `data.rowid_first` scans via `main.rowid_sequence`, a
different schema than the table's own — the same reason `BindRequest.schema_name` is left `null`
for the plain-table bind call below).

That static wiring alone is not enough for every real fixture, though — confirmed the hard way
against `secret_demo_table` (backed by `secret_demo`), whose `on_bind` resolves its secret fully
dynamically via `SecretsAccessor.get()`, with no static `Secret()`/`Meta.required_secrets`
declaration at all: `resolve_metadata(SecretDemoFunction).required_secrets == []`, live-checked
against the fixture. VGI's own bind wire protocol has a second, genuinely dynamic channel for
exactly this case: a first `bind()` whose `on_bind` couldn't resolve a needed secret returns a
**secret scope request** instead of a normal response — `BindResponse.lookup_secret_types`/
`lookup_scopes`/`lookup_names` non-empty, `output_schema` empty — and the caller must resolve
exactly those (never a static declaration) and retry with `resolved_secrets_provided=true`, the
same two-phase dance the real C++ extension implements in `vgi_bind_protocol.cpp`
(`TryParseBindSecretScopeResponse`/its retry loop). `getSplits` now does this too: on a non-empty
`lookup_secret_types`, it resolves those exact requests from the session's extra credentials
(the same `vgi_secret.<key>.<field>` convention) and reissues `bind()` once, resolved.

One more real wrinkle inside that retry, found only by tracing the actual failure past a bind
that looked correct: the secret this connector resolves and hands back must ALSO carry a
synthetic `"type"` field (value = the secret type, e.g. `vgi_example`) inside its own field dict —
`vgi-python`'s `ResolvedSecrets.of_type()` (what `initial_state()` calls to read the secret back at
process time — a different, later accessor than the `on_bind`-time one) filters on exactly that
field, which the real DuckDB C++ extension always attaches when it resolves a `CREATE SECRET` but
which `#resolveSecretFields`/`#encodeSecrets` (built for a *static* `Secret()`-annotated scalar
argument, always read by plain dict key, never by `of_type()`) never added on their own. Synthesized
here, scoped to this two-phase-retry code path only — never touching the shared `BindCache` helpers
themselves, so the already-working static path (`secret_field`/`return_secret_value`) is untouched.

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

### Table-in-out ("blended") functions — literal call shape

VGI's `RowTransformFunction` ("blended") kind serves three call shapes from one
registration: literal (constant arguments, e.g. `geo_encode(52.0, 13.0)`), per-row
column (`FROM t, geo_encode(t.lat, t.lon)`), and `LATERAL`. Only the literal shape is
implemented here. Verified directly against the real Trino table-function SPI
(`ScalarArgumentSpecification`/`TableArgumentSpecification`/`DescriptorArgumentSpecification`
— zero "correlated"/"lateral" hooks anywhere in `io.trino.spi.function.table`): every
argument is resolved exactly once, at `analyze()`/bind time, with no mechanism for "this
scalar argument's value comes from each row of an outer table" — so the column and
`LATERAL` shapes are a confirmed SPI ceiling, not a scope choice. Call it as `SELECT *
FROM TABLE(catalog.schema.name(args))` — Trino's grammar requires the explicit
`TABLE(...)` wrapper even though every argument is a plain scalar; a bare
`catalog.schema.name(args)` in a `FROM` clause is a parse error.

**Wire shape, and how it differs from a regular table function.** A blended literal call
shares the exact same `bind()`/`init()`/exchange primitives a regular VGI table function
and scalar function already use — there's no separate RPC verb — but the interaction
pattern is the opposite of a producer scan: the POSITIONAL arguments themselves ARE the
one input row (no `table_function_plan`, no pagination, no splits), sent as
`BindRequest.input_schema` (the row's declared shape, at bind time) plus a real one-row
Arrow batch written during `init(phase=INPUT)`'s single exchange turn. Because a blended
function is guaranteed to have no finalize phase (the wire itself rejects
`input_from_args=true` combined with `has_finalize=true` at `bind()`), that one exchange
turn is the whole answer — legally 0, 1, or many output rows (`VgiTableInOutSplitProcessor`
copies the answer out of the reader-owned batch, then closes the exchange session; it
never loops a tick the way a producer-mode scan does). Named arguments (VGI's
`vgi_arg=named` convention, e.g. `geo_encode`'s optional `precision`) stay on
`BindRequest.arguments` exactly like a regular table function's arguments — only
positional arguments become row-batch columns; VGI's own `resolve_metadata` already
rejects a positional `vgi_const` argument for exactly this reason (indistinguishable from
a real input column).

Because `ScalarArgument.getValue()` is only available at `analyze()`/bind time (the same
fact `VgiTableFunction#analyze` already relies on for a regular table function's bind-time
constants), the literal row's real values are resolved eagerly there and serialized
(schema AND data, via `ArrowSchemaCodec.serializeBatch`) into `VgiTableInOutFunctionHandle`
— a table function's bound handle is what actually survives the coordinator/worker
boundary, not any live Java object kept in the `ConnectorTableFunction` instance. Since
there's no split enumeration at all, `VgiSplitManager`'s table-function `getSplits`
overload hands a literal call's handle a trivial single-element `FixedSplitSource`
(`VgiTableInOutSplit`) instead of a paginated `VgiSplitSource`, and
`VgiFunctionProvider.getTableFunctionProcessorProvider` branches on the split's runtime
type to dispatch to `VgiTableInOutSplitProcessor` instead of the regular
`VgiTableFunctionSplitProcessor`.

**v1 scope**: mirrors `VgiTableFunctions`' own constraints — a varargs positional argument
(`row_sum(*values)`) or an `any`-typed one is skipped at discovery (the same pre-existing
gap regular table functions have, not a new one), an overloaded name (VGI resolves
`geo_encode`'s 2-arg and 3-arg registrations by arity; Trino's PTF model allows only one
registration per name) is skipped entirely rather than guessing, and a blended function
with zero positional arguments is rejected defensively (VGI's own `resolve_metadata`
already prevents registering one, so this should never be observed in practice).

### Table-in-out functions — CLASSIC (real `TABLE` argument)

VGI's OTHER table-in-out kind — `TableInOutGenerator`/`TableInOutFunction` — takes a real
`TableInput` argument instead of blended's positional-args-are-the-row shape: `SELECT * FROM
TABLE(cat.main.echo(TABLE(some_query)))`, `TABLE(cat.main.repeat_inputs(3, TABLE(some_query)))`
(the `TableInput` argument isn't always argument 0). Unlike blended's column/`LATERAL` shapes,
this one IS representable — verified directly against the real Trino engine
(`io.trino.operator.LocalExecutionPlanner.visitTableFunctionProcessor`) that a function
declaring any `TableArgumentSpecification` is *always* routed through
`TableFunctionDataProcessor` (page-driven), never `TableFunctionSplitProcessor` (split-driven)
— so `VgiSplitManager` is never involved for this kind of call at all, and VGI's own
per-substream model (one independent worker connection per execution) maps directly onto
"one `getDataProcessor` call per Trino partition, one borrowed connection each."

**Wire shape.** Same `bind()`/`init(phase=INPUT)`/exchange primitives as everywhere else, but
genuinely incremental this time: `VgiTableInOutTableFunction#analyze` binds once (its
`input_schema` built from the `TableArgument`'s real `RowType`, declared via
`TableFunctionAnalysis.requiredColumns` — always every column, since VGI's classic mode has no
partial-projection concept of its own), then `VgiTableInOutDataProcessor` — one instance per
partition, mirroring `VgiTableFunctionSplitProcessor`'s async connection-acquisition pattern —
drives a genuinely long-lived streaming session: `init(phase=INPUT)` once, then one
`exchange()` turn (write the page as a batch, read the matching output) per `process(List<Optional<Page>>)`
call, until Trino signals true end-of-input (`input == null`), at which point `session.close()`
signals EOS and releases the connection. `TableArgument` (verified against the real SPI) carries
only a `RowType` plus `PARTITION BY`/`ORDER BY` column names — no reference to the source
connector at all — so this connector cannot push anything besides column pruning into wherever
those rows actually come from; the engine delivers them later as plain, already-narrowed `Page`s.
The registered `TableArgumentSpecification` uses `.keepWhenEmpty()` (matching Trino's own
reference `IdentityFunction`/`RepeatFunction` test implementations) rather than
`.rowSemantics()` — no `PARTITION BY`/`ORDER BY` requirement on the caller, whole relation as one
partition when neither is specified, which is the closest honest match to VGI's own
no-partition-concept-at-all model.

**A genuine SPI gap, not a bug**: unlike `TableFunctionSplitProcessor`, the real
`TableFunctionDataProcessor` interface declares no `close()`/lifecycle hook at all — verified by
reading the SPI source directly. If Trino ever abandons a partition before this processor
returns `FINISHED` on its own (e.g. a `LIMIT` satisfied by an earlier partition), there is no
notification and the borrowed connection leaks until GC — a real ceiling of this Trino version's
SPI, not something `VgiTableInOutDataProcessor` can fix; every other connection this connector
borrows has a real release path, this is the one documented exception.

**Finalize phase.** A `has_finalize=true` function (`SubstreamPartialSumFunction`/
`MultiBatchFinishFunction`-style cross-batch accumulation) registers too. On true
end-of-input, `VgiTableInOutDataProcessor` closes the INPUT-phase stream, then issues a
SECOND, separate `init(phase=FINALIZE)` call on the SAME connection, then drains its answer
in producer mode (`tick()` in a loop) — `finish()` can legitimately return several separate
output batches (confirmed against the real fixture's `multi_batch_finish`, built specifically
to catch a broken multi-batch continuation), each needing its own `tick()` round trip, not
just multiple rows in one batch. The one genuinely load-bearing detail, confirmed by reading
the real reference client and worker directly rather than assumed: the FINALIZE call MUST
carry the INPUT phase's own `execution_id` (read off the INPUT-phase stream's header, a real
`GlobalInitResponse` — the service interface declares `@StreamHeader(GlobalInitResponse.class)`
on `init`) — VGI's server-side state store is keyed by `execution_id`, not by the connection or
worker process, so passing a fresh/`null` one would silently correlate to an empty accumulator
instead of throwing.

**Settings and secrets.** `required_settings`/`required_secrets` (the real fixture's
`filter_by_setting`/`secret_in_out`) resolve in `analyze()` exactly like a scalar function's —
reusing `VgiScalarFunctions.BindCache#resolveSettings`/`#resolveSecretFields` verbatim, since
`analyze()` already receives a real `ConnectorSession` directly (a scalar function's
`FunctionProvider` does not, which is the entire reason `BindCache` exists there — no
bind-cache equivalent is needed on this side at all). `secret_in_out` specifically is NOT
reachable even so: its `on_bind` resolves a secret fully dynamically, with no static
`Secret()`/`Meta.required_secrets` declaration, so `FunctionInfo.required_secrets` is
genuinely empty for it — this connector's secret-forwarding stays gated on `required_secrets`
being non-empty (a deliberate, security-relevant gate: never guess which credentials a
function needs), so nothing is forwarded to it even if the caller's `--extra-credential`
supplied it.

**v1 scope otherwise**: mirrors the other table-function classes' constraints (varargs/
`any`-typed non-table arguments skipped, overloaded names skipped, more than one `TableInput`
argument rejected defensively — VGI's own validation already prevents registering one).
`SumAllColumnsFunction`/`SumAllColumnsSimpleDistributed` stay out of reach regardless of
finalize support — they're really a `TableBufferingFunction`, a third, distinct VGI kind with
its own `TABLE_BUFFERING`/`TABLE_BUFFERING_FINALIZE` phases, not `INPUT`/`FINALIZE`.

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

### Aggregate functions

VGI aggregates (`vgi_sum`, `vgi_avg`, `vgi_count`, …) are dispatched via `ConnectorMetadata.
getAggregationFunctionMetadata` + `FunctionProvider.getAggregationImplementation`
(`VgiAggregateFunctions`/`VgiFunctionProvider`) — a genuinely different Trino SPI surface from
scalar functions, and a genuinely different VGI wire protocol too: aggregates use their own set of
plain unary RPCs (`aggregate_bind`/`update`/`combine`/`finalize`/`destructor`), never the
exchange-mode streaming session scalar functions use.

**Non-decomposable by design, not by omission.** Trino's distributed aggregation model wants to run
a *partial* aggregation stage per node, ship each node's intermediate state across the network, and
*combine* them on a final node — which needs a wire mechanism for a portable, execution-independent
state blob. VGI's `aggregate_combine` doesn't have one: its `merge_batch` carries only `source_group_id`/
`target_group_id` columns, no state payload at all — it merges two groups' state *within* one
already-bound `execution_id`, addressed only by group id, on whichever single worker bound that
execution. There's no RPC that hands the client a state blob a *different* accumulator (a different
node, a different `execution_id`) could later deserialize and merge. So every VGI aggregate is
registered with no `combineFunction` and no intermediate type — `AggregationFunctionMetadata.
isDecomposable()` is defined as "has an intermediate type," so declaring none disables it outright,
and Trino runs the whole aggregation as a single stage feeding one accumulator. The real cost: no
partial-aggregation parallelism across nodes for a VGI aggregate. The alternative — guessing at a
cross-node state-shipping mechanism the protocol doesn't actually provide — would be worse.

**Row-at-a-time `input()`, batched RPCs.** Trino's `AccumulatorCompiler` calls the `input`
`MethodHandle` once per ROW, with no vectorized calling convention at the implementation level.
Naively RPC-ing on every call would be a real regression versus scalar functions (called once per
*output* row; an aggregate's `input()` is called once per *input* row feeding a far smaller number
of groups). `VgiAggregateFunctions.AggregateState` instead buffers each row in-process into a local
Arrow batch tagged with a `__vgi_group_id` column — exactly the batched, group-tagged shape VGI's
own `aggregate_update` already expects — and only calls `aggregate_update` once the buffer crosses a
size threshold (4096 rows) or `output()` needs to flush whatever's pending before finalizing.

One state class (`AggregateState`, implementing `GroupedAccumulatorState`) serves both grouped
(`GROUP BY`) and ungrouped aggregation — `AccumulatorStateFactory.createSingleState()`/
`createGroupedState()` both return the same type by design; the ungrouped case just never calls
`setGroupId`, leaving every row tagged with the implicit group id 0. `FILTER (WHERE ...)`,
`DISTINCT`, and an explicit `ORDER BY` inside the aggregate call are all handled by Trino's own
engine before `input()` is ever called — nothing VGI-specific needed there.

**`OVER (...)` (windowed usage) — correct for some VGI aggregates, a genuine ceiling for others.**
No VGI aggregate declares a `combineFunction` or a hand-compiled `WindowAccumulator` override (see
above — the latter is only reachable via a real compiled Java class implementing `removeInput`,
never from a dynamically-discovered `AccumulatorStateFactory` like this connector's), so Trino falls
back to its own generic per-frame-recompute strategy: confirmed directly by reading
`AggregateWindowFunction`/`AggregationWindowFunctionSupplier` in `io.trino.operator.window`, every
frame that isn't a pure growing extension of the previous one discards and rebuilds the whole
accumulator — a fresh `aggregate_bind`, every buffered row re-sent via `aggregate_update`, a fresh
`aggregate_finalize`, once per *output* row.

That recompute-per-frame strategy is a legitimate, Trino-SPI-supported way to answer `OVER (...)`
for a non-decomposable aggregate, and it genuinely works today for any VGI aggregate whose
`update`/`finalize` implement real "compute the aggregate over whatever rows you're handed"
semantics — confirmed directly: `vgi_sum` produces correct results under both a sliding frame
(`ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`) and a growing one (`ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW`), see `VgiAggregateFunctionsTest`. It does **not**, and cannot, work for a VGI
aggregate whose only correct implementation lives behind VGI's *separate* windowed-aggregate RPC
family (`aggregate_window_init`/`aggregate_window`/`aggregate_window_batch`/
`aggregate_window_destructor` — ship the partition once, then evaluate each output row's frame via
explicit subframe ranges). The real `vgi_window_median` fixture is exactly this case: its
`update`/`finalize` are a deliberate no-op (mirroring how DuckDB's own C++ extension always prefers
the `window()` callback for `OVER (...)` and only falls back to `update`/`combine`/`finalize` for
non-window aggregation), so `example.main.vgi_window_median(x) OVER (...)` reliably returns `NULL`
for every row through this connector — never the real median.

This is a genuine ceiling, not an unfinished feature, for two independent reasons: (1) the
`aggregate_window_init`/`aggregate_window` RPC family is entirely absent from the `farm.query:vgi`
Java SDK this connector depends on — no `AggregateWindow*` protocol types, no `aggregate_window*`
method on `VgiService` — confirmed against both the published jar and a from-source `vgi-java`
checkout, so it's a real capability gap, not an unpublished-release lag. (2) Even with those RPCs
available, Trino's own engine (`LocalExecutionPlanner`) hardcodes every `FunctionKind.AGGREGATE`
function's `OVER (...)` usage to go through `AggregationImplementation` — `FunctionProvider.
getWindowFunctionSupplier` is never consulted for an aggregate-kind function, only for a genuinely
`FunctionKind.WINDOW`-registered one (`rank`, `lag`, and similar) — so there is no engine-level hook
by which this connector could route an aggregate's window usage through a different RPC family even
once the SDK gap above closes. `supports_window`/`streaming_partitioned` (real, if currently unread,
`FunctionInfo` metadata fields) couldn't disambiguate the two cases at discovery time either way —
`vgi_window_sum` (real fallback, correct today) and `vgi_window_median` (stub fallback, always
`NULL`) both set them identically.

**Const arguments bind lazily, from the first observed row.** `vgi_const` arguments (e.g. `vgi_percentile`'s
percentile) ARE supported — VGI's `AggregateBindRequest.arguments` is exactly the const-value
channel, the same shape scalar functions' `BindRequest.arguments` already uses — but `aggregate_bind`
has to happen before any row can be processed, and Trino's `AccumulatorStateFactory` methods take no
arguments at all, so the actual constant VALUE can't be known at state-creation time (Trino doesn't
distinguish "const" arguments at the SPI level either — one arrives as an ordinary per-row value on
every `input()` call, same as scalars). `AggregateState` defers `aggregate_bind` until the FIRST row
actually arrives, reads the constant off that row, and binds once for the accumulator's whole
lifetime — matching VGI's own bind-once-per-execution semantics exactly. A function with no const
arguments still binds eagerly at construction. One honest edge case: if an accumulator never
receives a single row (e.g. aggregating an empty table), the constant is never observed and `output()`
returns `NULL` directly rather than binding with a fabricated value — correct for every real fixture
seen, not a general proof for every conceivable aggregate.

**Varargs and `any`-typed arguments** are both supported too, via `Signature.variableArity()`/
`Signature.typeVariable` exactly mirroring the scalar-function precedent (`effectiveArgs` expands
the declared argument list to a call site's actual bound arity for the same reason: Arrow field
names must stay unique, and VGI dispatches a vararg group by column position, not name) — needed for
real fixtures like `vgi_sum_all`, whose vararg argument is itself any-typed.

**Null handling matches VGI's own default without extra work.** `vgi_sum`'s default null handling
skips a NULL-valued row entirely (its `update()` is never called for one) — declaring every argument
non-nullable makes Trino's own generated code skip calling `input()` for a null row in exactly the
same way, confirmed by reading `AccumulatorCompiler`'s `anyParametersAreNull` gating directly rather
than assumed.

**Scope — deferred, named explicitly, same discipline as *Scalar functions* above:**

- A dynamic (bind-time-computed) return type — same ceiling as scalars: Trino resolves a function's
  return type from its static `Signature` before any RPC happens.
- More than 4 arguments — `VgiAggregateFunctions` only has hand-written `input` `MethodHandle`s for
  0–4 arguments (Trino's compiler expects an exact, statically-typed `(State, ValueBlock, int, ...)`
  parameter list per argument, not a generically collectible shape) — this includes a vararg call
  site whose EXPANDED arity exceeds 4, even if the declared signature itself is small.
- `aggregate_destructor` is never called — a known gap, not a correctness risk in the current
  single-execution design (a finalized group's state is never needed again), but state isn't
  reclaimed from the worker proactively yet.
- Settings/secrets for aggregates — `AggregateBindRequest` carries the same `settings`/`secrets`
  fields scalar functions already use, but this connector doesn't resolve/forward them yet.

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
- **Aggregate functions.** Aggregates with up to 4 arguments — including const/varargs/`any`-typed
  ones — `vgi_sum`, `vgi_avg`, `vgi_count`, `vgi_percentile`, `vgi_sum_all`, and similar, ARE now
  supported, non-decomposable (see *Aggregate functions* above for why that's a real protocol
  ceiling, not a scope choice). `OVER (...)` usage is correct for a VGI aggregate whose
  `update`/`finalize` implement real arbitrary-subset semantics (e.g. `vgi_sum`), but is a genuine,
  unfixable-from-here ceiling for one whose only correct implementation lives behind VGI's separate
  windowed-aggregate RPC family (e.g. `vgi_window_median` — always returns `NULL`) — see *Aggregate
  functions* above for the full root cause. A dynamic return type, more than 4 arguments,
  settings/secrets, and `aggregate_destructor`-driven state cleanup are the named, deferred
  remainder — see that section's own scope list.
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
- **Per-query settings and secrets for callable `TABLE(...)` producer functions
  (`VgiTableFunction`).** `VgiTableFunction#analyze` still always sends `null`/`null`/`false` for
  `settings`/`secrets`/`resolved_secrets_provided` on its own `bind()` call — this gap is now
  closed for scalar functions, classic table-in-out functions, AND declarative tables' backing
  scan functions (session properties / extra credentials — see *Scalar functions*, *Table-in-out
  functions — CLASSIC*, and *Splits* above), but a function called directly as
  `TABLE(catalog.schema.fn(...))` still needs the equivalent wiring on `VgiTableFunction`'s own
  bind call site.

## License

[Query Farm Source-Available License, Version 1.0](LICENSE) — the same license
[VGI](https://github.com/Query-farm/vgi) itself is released under. Free for
development, testing, and internal Production Use; see the license for the
narrow set of restricted uses (competing hosted offerings, commercial
marketplaces) that require a separate agreement with Query Farm.

Copyright 2026 Query Farm LLC - https://query.farm
