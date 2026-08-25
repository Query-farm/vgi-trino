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
            "duckdb_functions(", "enable_logging", "EXPLAIN (FORMAT JSON)", "QUALIFY");

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
        List<String> worstFiles = new ArrayList<>();

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
        report.append("--- files with failures (").append(worstFiles.size()).append(") ---\n");
        worstFiles.forEach(f -> report.append(f).append('\n'));
        System.out.println(report);
    }

    /** Buckets one failure message from {@link SqlLogicTestRunner.Result#failures()} by likely cause. */
    private static String classify(String failure) {
        if (failure.startsWith("QUERY mismatch")) return "QUERY_MISMATCH (ran, wrong data — worth investigating)";
        if (failure.contains("mismatched input") || failure.contains("Non-query expression encountered")
                || failure.contains("line 1:") || failure.contains("SyntaxException")) {
            return "PARSE_ERROR (likely DuckDB-only SQL syntax, e.g. bare table-function CALL syntax)";
        }
        if (failure.startsWith("expected an error for")) return "EXPECTED_ERROR_DIDNT_HAPPEN";
        if (failure.contains("not registered") || failure.contains("does not exist")
                || failure.contains("Unsupported")) {
            return "UNSUPPORTED (function/feature not registered or implemented)";
        }
        return "OTHER_RUNTIME_ERROR";
    }
}
