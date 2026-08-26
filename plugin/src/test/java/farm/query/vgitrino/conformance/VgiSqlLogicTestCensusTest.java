// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
import farm.query.vgitrino.testing.VgiWorkerHarness;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A real, current CENSUS of how many of vgi's {@code .test} sqllogictest files' records this
 * connector can actually run today — not a pass/fail gate ({@link VgiSqlLogicTestConformanceTest}
 * already covers the two hand-curated files that are asserted stable), but a fresh measurement
 * across the ENTIRE {@code ~/Development/vgi/test/sql/integration/} corpus (327 files as of this
 * writing), replayed via the same {@link SqlLogicTestRunner} those two files already use.
 *
 * <p>Deliberately does NOT pre-filter DuckDB table-function CALL syntax ({@code
 * schema.function(args)}, used as a bare table reference — Trino requires the explicit {@code
 * TABLE(...)} wrapper and has no equivalent grammar) via a marker the way {@code ATTACH}/{@code
 * DESCRIBE}/DuckDB-only introspection functions are — that population is exactly what the parser
 * failures below measure, so the report distinguishes "known, structural, Trino-grammar gap"
 * (a parse error) from "this connector genuinely computed the wrong thing" (a query mismatch) —
 * the second bucket is the one actually worth fixing.
 *
 * <p>Run standalone: {@code ./gradlew :plugin:test --tests
 * "farm.query.vgitrino.conformance.VgiSqlLogicTestCensusTest"} (subprocess transport, one shared
 * worker for the whole corpus — this is a measurement pass, not a per-transport conformance
 * check).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiSqlLogicTestCensusTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final Path INTEGRATION_ROOT =
            Path.of(System.getProperty("user.home"), "Development/vgi/test/sql/integration");

    private static final String TRINO_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    /** The categories {@code VgiSqlLogicTestConformanceTest}'s two curated files already established
     *  as genuinely non-portable — no Trino/connector equivalent exists at all, so skipping these
     *  (rather than letting them fail and pollute the interesting buckets) is itself accurate, not
     *  a shortcut. CALL syntax is deliberately NOT in this list — see the class javadoc. */
    private static final List<String> NON_PORTABLE_MARKERS = List.of(
            "ATTACH ", "DETACH", "DESCRIBE",
            "duckdb_tables(", "duckdb_databases(", "duckdb_constraints(", "duckdb_logs(",
            "duckdb_functions(", "duckdb_schemas(", "vgi_result_cache(", "vgi_result_cache_flush(",
            "enable_logging", "QUALIFY",
            // DuckDB's EXPLAIN plan format (node names like EMPTY_RESULT/VGI_TABLE_SCAN) has zero
            // correspondence to Trino's own EXPLAIN output shape — confirmed by a real sample
            // (table/column_statistics.test) comparing a DuckDB plan-node regex against Trino's
            // actual physical-plan text, which obviously never matches. Not a connector bug to fix;
            // a structurally different feature between the two engines, matching the "EXPLAIN
            // (FORMAT JSON)" case this broader marker now subsumes.
            "EXPLAIN ",
            // A confirmed, documented Trino SPI ceiling (see the README's "Predicate pushdown"
            // scope note), not a bug: a PTF-sourced split source is always built with
            // TupleDomain.all() — ConnectorTableFunctionHandle is a bare marker interface and
            // TableFunctionProcessorProvider.getSplitProcessor takes no filter of any kind, so
            // there is nothing to thread through even in principle. Every one of these functions
            // exists specifically to echo back what filter it received, so "(none)" is the only
            // possible correct answer today — confirmed by sampling real QUERY_MISMATCH failures,
            // not assumed. Fixing this needs an upstream Trino SPI change, which is out of scope.
            "filter_echo(", "filter_echo_partitioned(", "split_echo_filters(", "split_dynamic_filter(",
            "order_echo(", "value_prune(", "filtered_columns_echo(",
            // DuckDB's own catalog name (e.g. "example", "v1", "accumulate" from its own ATTACH
            // statements) is a plain STRING VALUE inside these queries, not an identifier prefix —
            // the harness's catalog rewrite only ever touches "example." (dot-suffixed, used as a
            // schema-qualifier), so a bare catalog-name-as-data comparison against
            // information_schema can never match the renamed Trino catalog ("vgi_example"). A
            // harness catalog-naming artifact, not a connector information_schema bug.
            "information_schema",
            // The SAME multi-catalog-alias limitation, a different real shape: accumulate/*.test
            // files ATTACH the same worker under two SEPARATE catalog names ("accumulate",
            // "accumulate2") to test cross-attach behavior — this harness only ever attaches ONE
            // Trino catalog ("vgi_example", from the canonical "example." reference), so a call
            // qualified by either secondary alias has no matching Trino catalog to route to at
            // all (confirmed the dominant remaining PARSE_ERROR driver by sampling real failures —
            // ~60+ of the corpus's "mismatched input '('" parses trace here). Fixing this for real
            // would mean the harness discovering and attaching a Trino catalog per distinct ATTACH
            // alias per file, a genuine architecture change, not a textual rewrite.
            "accumulate(", "accumulate_read(", "accumulate_clear(",
            // attach/attach_options_echo.test's own version of the same thing: "ao"/"ao_defaults"/
            // "ao_other" are separate ATTACH aliases this harness never creates a Trino catalog
            // for either.
            "echo_attach_options(",
            // A DuckDB/VGI-extension-native introspection function (lists attached catalogs),
            // unrelated to any worker-registered function — no Trino equivalent, same category as
            // duckdb_tables()/etc. above. Lowercase: matches the real corpus casing (confirmed by
            // reading the source directly — an earlier, uppercase "VGI_CATALOGS(" marker silently
            // never matched anything, the same class of case-sensitivity mistake already fixed
            // once for accumulate(/accumulate_read(/accumulate_clear( above).
            "vgi_catalogs(",
            // DuckDB's own runtime-tuning PRAGMA-like statements (no SESSION keyword, no such
            // session property in Trino at all) — harmless, unrelated to anything this connector
            // does, same category as duckdb_tables()/etc. The vgi_result_cache_* family (dir/
            // max_bytes/max_entry_bytes/disk_max_bytes — confirmed real corpus spellings via
            // cache/spill_correctness.test etc.) is DuckDB/VGI's own native result-cache tuning,
            // same non-portable category.
            "SET threads", "SET vgi_streaming_window", "SET vgi_result_cache",
            // Trino has no CREATE TEMP/TEMPORARY TABLE at all (matches the existing "no write
            // support" scope gap, not a new one) — "TEMP", not "TEMPORARY", is the real corpus
            // spelling.
            "CREATE TEMP ",
            // Trino's grammar has no COPY statement at all (confirmed directly against the real
            // ANTLR grammar, SqlBase.g4 — zero references to the keyword) — a hard parser-level
            // ceiling, not a connector gap; this connector's own copy_from/copy_to BindRequest
            // wire fields exist for a DIFFERENT reason (a function-backed table's own scan bind
            // can carry COPY context when DuckDB itself drives one), not because Trino SQL has a
            // COPY statement for this connector to implement.
            "COPY ",
            // Trino has no CREATE SECRET/DROP SECRET DDL — VGI secrets reach this connector via
            // ConnectorIdentity.getExtraCredentials() (see the README's "Settings and secrets"
            // sections), never a DuckDB-style secret-management statement.
            "CREATE SECRET ", "DROP SECRET ",
            // A DuckDB/VGI-extension-native debug procedure (disables internal request logging) —
            // no Trino Procedure SPI implementation exists for it, nor should one: it's a pure
            // test/debug utility with no data-visible effect, same category as duckdb_tables()/etc.
            "CALL disable_logging(",
            // A worker whose behavior is defined by a literal source-code-string argument at
            // runtime — gated behind require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE and explicitly
            // excluded from this connector's static-registration model (a function's shape must be
            // known at catalog-discovery time; there's no such thing as "the behavior of THIS
            // specific call" for a statically-registered Trino function) — not a bug, a
            // fundamentally different feature this connector's architecture doesn't support.
            "vgi_dynamic_agg(", "vgi_dynamic_ml_agg(");

    private VgiWorkerHarness.Handle worker;
    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest census");
        Assumptions.assumeTrue(Files.isDirectory(INTEGRATION_ROOT),
                "~/Development/vgi/test/sql/integration not present — skipping sqllogictest census");
        worker = VgiWorkerHarness.subprocess(VGI_PYTHON);

        session = TestingSession.testSessionBuilder().setCatalog(TRINO_CATALOG).setSchema("data").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(TRINO_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", worker.location(),
                "vgi.catalog-name", VGI_CATALOG_NAME,
                // 4 was too small for a single long-lived catalog replaying the WHOLE corpus,
                // including several deliberate combine/finalize-crash TableBufferingFunction
                // fixtures (buffered_combine_crash.test/buffered_finalize_crash.test) — each crash
                // is expected to evict+replace one pool slot (VgiWorkerClient.release's own
                // self-heal), but a replacement mint can itself transiently fail under this whole
                // Gradle test JVM's heavy concurrent subprocess-spawn load, "honestly losing" that
                // slot for the rest of the census's lifetime (see VgiWorkerClient.release's own
                // javadoc for why that trade-off is deliberate, not a bug). With only 4 slots, a
                // handful of such honest losses over a 328-file run is enough to reach TRUE zero
                // available connections — confirmed via a live jstack showing VgiWorkerClient.borrow()
                // permanently blocked in VgiTableBufferingFunction.analyze() for a later, unrelated
                // query, degrading the rest of the run to one 30s connection-acquire timeout per
                // remaining table-in-out-family statement (invisible in the worker's own RPC log,
                // since it never reaches a real RPC call). 16 gives enough headroom to absorb the
                // small number of crash fixtures this corpus actually contains without ever
                // reaching real exhaustion in one run.
                "vgi.connections", "16"));
    }

    @AfterAll
    void stop() throws Exception {
        if (runner != null) runner.close();
        if (worker != null) worker.teardown().close();
    }

    @Test
    @Timeout(3600)
    void census() throws IOException {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(INTEGRATION_ROOT)) {
            files = walk.filter(p -> p.toString().endsWith(".test")).sorted().toList();
        }

        int totalExecuted = 0;
        int totalSkipped = 0;
        int totalFailed = 0;
        int filesFullyClean = 0; // every non-skipped record executed with no failures
        Map<String, Integer> failureBuckets = new TreeMap<>();
        Map<String, Integer> reasonCounts = new HashMap<>();
        List<String> worstFiles = new ArrayList<>();
        List<String> queryMismatchSamples = new ArrayList<>();
        List<String> parseErrorSamples = new ArrayList<>();
        List<String> otherRuntimeErrorSamples = new ArrayList<>();

        int filesUsingADetectedAlias = 0;
        for (Path file : files) {
            SqlLogicTestRunner.Result result;
            try {
                String vgiCatalogRef = detectExampleCatalogAlias(file) + ".";
                if (!vgiCatalogRef.equals("example.")) filesUsingADetectedAlias++;
                result = SqlLogicTestRunner.run(runner, session, file, vgiCatalogRef, TRINO_CATALOG, NON_PORTABLE_MARKERS);
            } catch (RuntimeException | IOException e) {
                // The file itself didn't even parse/replay — count every one of its failures as one bucket entry.
                failureBuckets.merge("FILE-LEVEL ERROR: " + e.getClass().getSimpleName(), 1, Integer::sum);
                continue;
            }
            totalExecuted += result.executed();
            totalSkipped += result.skipped();
            totalFailed += result.failures().size();
            if (result.failures().isEmpty()) {
                filesFullyClean++;
            } else {
                worstFiles.add(INTEGRATION_ROOT.relativize(file) + " (" + result.failures().size() + " failures)");
                for (String failure : result.failures()) {
                    failureBuckets.merge(classify(failure), 1, Integer::sum);
                    reasonCounts.merge(reasonKey(failure), 1, Integer::sum);
                    if (failure.startsWith("QUERY mismatch") && queryMismatchSamples.size() < 200) {
                        queryMismatchSamples.add(INTEGRATION_ROOT.relativize(file) + ":\n" + failure);
                    }
                    if (classify(failure).startsWith("PARSE_ERROR") && parseErrorSamples.size() < 60) {
                        parseErrorSamples.add(INTEGRATION_ROOT.relativize(file) + ":\n" + failure);
                    }
                    if (classify(failure).equals("OTHER_RUNTIME_ERROR") && otherRuntimeErrorSamples.size() < 80) {
                        otherRuntimeErrorSamples.add(INTEGRATION_ROOT.relativize(file) + ":\n" + failure);
                    }
                }
            }
        }

        worstFiles.sort(Comparator.comparing(s -> s));
        StringBuilder report = new StringBuilder();
        report.append("\n=== vgi sqllogictest census (" + files.size() + " files) ===\n");
        report.append("records executed: ").append(totalExecuted).append('\n');
        report.append("records skipped (known non-portable): ").append(totalSkipped).append('\n');
        report.append("records FAILED: ").append(totalFailed).append('\n');
        report.append("files with zero failures: ").append(filesFullyClean).append(" / ").append(files.size()).append('\n');
        report.append("files using a detected non-'example' catalog alias: ").append(filesUsingADetectedAlias).append('\n');
        report.append("--- failure buckets ---\n");
        failureBuckets.forEach((bucket, count) -> report.append(String.format("%6d  %s%n", count, bucket)));
        report.append("--- top 30 distinct failure reasons (across all buckets) ---\n");
        reasonCounts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(30)
                .forEach(e -> report.append(String.format("%6d  %s%n", e.getValue(), e.getKey())));
        report.append("--- QUERY_MISMATCH samples (").append(queryMismatchSamples.size()).append(") ---\n");
        queryMismatchSamples.forEach(s -> report.append(s).append("\n---\n"));
        report.append("--- PARSE_ERROR samples (").append(parseErrorSamples.size()).append(") ---\n");
        parseErrorSamples.forEach(s -> report.append(s).append("\n---\n"));
        report.append("--- OTHER_RUNTIME_ERROR samples (").append(otherRuntimeErrorSamples.size()).append(") ---\n");
        otherRuntimeErrorSamples.forEach(s -> report.append(s).append("\n---\n"));
        report.append("--- files with failures (").append(worstFiles.size()).append(") ---\n");
        worstFiles.forEach(f -> report.append(f).append('\n'));
        System.out.println(report);
    }

    /** Matches {@code ATTACH '<catalog-name>[?query-string]' AS <alias>} — the {@code
     *  query-string} suffix (e.g. {@code ?location=...&pool=false}) is real corpus syntax (see
     *  {@code connection_string.test}) that must not leak into the captured catalog name. */
    private static final Pattern ATTACH_PATTERN =
            Pattern.compile("ATTACH\\s+'([^'?]*)[^']*'\\s+AS\\s+(\\w+)");

    /**
     * If {@code file} attaches VGI's real {@code 'example'} catalog under exactly ONE alias, and
     * attaches no OTHER catalog at all, returns that alias (so the file's own queries — which use
     * that alias throughout, e.g. {@code ex.some_function(...)} — get correctly rewritten to the
     * Trino catalog name). Falls back to {@code "example"} otherwise (today's long-standing
     * default), which is always safe: a file using the literal {@code example} alias already
     * matches it, and a file attaching more than one catalog is an already-documented,
     * out-of-scope multi-catalog-alias case regardless (see {@code accumulate(}/{@code
     * echo_attach_options(}'s own {@link #NON_PORTABLE_MARKERS} entries) — this detection doesn't
     * change anything about how those are handled, it only widens what a SINGLE-alias file needs.
     *
     * <p>Confirmed against the real corpus this genuinely matters: 74 files (as of this writing)
     * attach {@code 'example'} under a non-{@code "example"} alias — mostly {@code ex} (62 files),
     * some {@code acme}/{@code badenum}/{@code svc} — with no other catalog in the same file, and
     * every one of them was previously unreachable (every query in the file failed with
     * {@code Catalog '<alias>' not found}, since the hardcoded {@code "example."} rewrite target
     * never matched their SQL at all).
     */
    private static String detectExampleCatalogAlias(Path file) throws IOException {
        String text = Files.readString(file);
        Matcher m = ATTACH_PATTERN.matcher(text);
        Set<String> aliasesForExample = new HashSet<>();
        Set<String> otherCatalogNames = new HashSet<>();
        while (m.find()) {
            String catalogName = m.group(1);
            String alias = m.group(2);
            if (catalogName.equals("example")) aliasesForExample.add(alias);
            else otherCatalogNames.add(catalogName);
        }
        if (aliasesForExample.size() == 1 && otherCatalogNames.isEmpty()) {
            return aliasesForExample.iterator().next();
        }
        return "example";
    }

    /**
     * A short, deduplicatable "reason" for one failure — the last non-blank line of the message
     * (where the actual exception text lives; earlier lines are the record's own SQL/expected-vs-
     * actual dump), truncated so two failures differing only in a literal value/position still
     * collapse to the same bucket.
     */
    private static String reasonKey(String failure) {
        String[] lines = failure.split("\n");
        String last = "";
        for (int i = lines.length - 1; i >= 0; i--) {
            if (!lines[i].isBlank()) { last = lines[i].strip(); break; }
        }
        return last.length() > 140 ? last.substring(0, 140) : last;
    }

    /**
     * Buckets one failure message from {@link SqlLogicTestRunner.Result#failures()} by likely
     * cause. Checks the most SPECIFIC signatures first deliberately: an earlier version of this
     * method checked a bare {@code "line 1:"} substring as its parse-error signal — which matches
     * almost every Trino exception message, since nearly all of them carry a {@code line:column}
     * position regardless of category — and so silently swallowed genuine {@code UNSUPPORTED}
     * classifications (a "Function 'X' not registered" message also contains "line 1:") into
     * {@code PARSE_ERROR} ahead of the {@code not registered} check. Confirmed by direct comparison
     * of two census runs where only connector functionality changed (aggregate-function support
     * added, no syntax-rewrite change at all) and {@code PARSE_ERROR} still dropped sharply —
     * impossible if the bucketing were actually parse-error-specific.
     */
    private static String classify(String failure) {
        if (failure.startsWith("QUERY mismatch")) return "QUERY_MISMATCH (ran, wrong data — worth investigating)";
        if (failure.startsWith("expected an error for")) return "EXPECTED_ERROR_DIDNT_HAPPEN";
        if (failure.contains("not registered") || failure.contains("does not exist")
                || failure.contains("Unsupported")) {
            return "UNSUPPORTED (function/feature not registered or implemented)";
        }
        if (failure.contains("mismatched input") || failure.contains("Non-query expression encountered")
                || failure.contains("SyntaxException") || failure.contains("extraneous input")) {
            return "PARSE_ERROR (likely DuckDB-only SQL syntax, e.g. bare table-function CALL syntax)";
        }
        return "OTHER_RUNTIME_ERROR";
    }
}
