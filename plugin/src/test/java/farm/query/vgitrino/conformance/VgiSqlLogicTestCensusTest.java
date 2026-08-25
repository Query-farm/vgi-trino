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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
            "filter_echo(", "split_echo_filters(", "split_dynamic_filter(",
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
            // does, same category as duckdb_tables()/etc.
            "SET threads", "SET vgi_streaming_window",
            // Trino has no CREATE TEMP/TEMPORARY TABLE at all (matches the existing "no write
            // support" scope gap, not a new one) — "TEMP", not "TEMPORARY", is the real corpus
            // spelling.
            "CREATE TEMP ",
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
                "vgi.connections", "4"));
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

        for (Path file : files) {
            SqlLogicTestRunner.Result result;
            try {
                result = SqlLogicTestRunner.run(runner, session, file, "example.", TRINO_CATALOG, NON_PORTABLE_MARKERS);
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
        report.append("--- files with failures (").append(worstFiles.size()).append(") ---\n");
        worstFiles.forEach(f -> report.append(f).append('\n'));
        System.out.println(report);
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
