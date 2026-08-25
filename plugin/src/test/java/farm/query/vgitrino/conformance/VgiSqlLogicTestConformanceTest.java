// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the ACTUAL {@code .test} sqllogictest files from
 * {@code ~/Development/vgi/test/sql/integration/} against this connector —
 * not hand-ported equivalents, the real files, parsed by
 * {@link SqlLogicTestFile}, with their real ATTACH-derived catalog reference
 * rewritten to this Trino catalog's name and their real expected output
 * compared against a real Trino query result.
 *
 * <p>Most of the 327-file suite cannot run against Trino at all, for two
 * independent reasons neither of which this connector can fix by itself:
 * <ul>
 *   <li>DuckDB-only introspection surface — {@code duckdb_tables()},
 *       {@code duckdb_databases()}, {@code duckdb_constraints()},
 *       {@code duckdb_logs()}, {@code CALL enable_logging}, DuckDB's own
 *       {@code EXPLAIN (FORMAT JSON)} plan shape. These have no Trino
 *       equivalent, full stop — 119 of the 327 files reference at least one.</li>
 *   <li>DuckDB table-function CALL syntax ({@code schema.function(args)}).
 *       Trino has an equivalent SPI (polymorphic table functions,
 *       {@code TABLE(catalog.schema.fn(args))}) but this connector doesn't
 *       implement it yet — a separate, large piece of work. 161 of the 327
 *       files use call syntax somewhere.</li>
 * </ul>
 *
 * <p>{@code rowid.test} is mostly the portable remainder: plain
 * {@code SELECT}/{@code WHERE}/{@code ORDER BY} against declarative tables.
 * Its {@code DESCRIBE} line (DuckDB's 6-column shape has no Trino
 * equivalent), its struct-typed-rowid case (needs {@code ROW} type support
 * this connector's type mapper doesn't have — see {@code VgiTypeMapping}),
 * and its trailing {@code rowid_sequence(...)} calls (table-function syntax)
 * are skipped with a reported reason, not silently dropped — the assertion
 * at the end of this test is that the skip count is exactly what's expected
 * and every record this connector's functionality covers actually runs and
 * passes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiSqlLogicTestConformanceTest {

    private static final String TRINO_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest conformance run");

        session = TestingSession.testSessionBuilder()
                .setCatalog(TRINO_CATALOG)
                .setSchema("data")
                .build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(TRINO_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", VGI_CATALOG_NAME,
                "vgi.connections", "2"));
    }

    @AfterAll
    void stop() {
        if (runner != null) runner.close();
    }

    /**
     * Substrings that mark a record as needing something this connector (or
     * Trino itself) doesn't have — a curated denylist for this specific file,
     * not a general static analyzer. See the class javadoc for what each
     * category actually needs.
     */
    private static final List<String> NON_PORTABLE_MARKERS = List.of(
            "ATTACH ",           // DuckDB's own ATTACH syntax — this harness attaches via createCatalog instead
            "DESCRIBE",          // DuckDB's 6-column DESCRIBE shape has no Trino equivalent
            "rowid_struct",      // needs ROW-type support in VgiTypeMapping
            "rowid_sequence(");  // table-function CALL syntax — needs Trino's ConnectorTableFunction

    @Test
    @Timeout(180)
    void rowIdColumnsMatchTheRealTestFile() throws Exception {
        Path testFile = Path.of(System.getProperty("user.home"),
                "Development/vgi/test/sql/integration/table/rowid.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(),
                "~/Development/vgi/test/sql/integration/table/rowid.test not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(
                runner, session, testFile, "example.", TRINO_CATALOG, NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // rowid.test has 8 non-portable records (the ATTACH this harness
        // replaces with createCatalog, DESCRIBE, the struct-rowid pair, and
        // the 4 rowid_sequence() statements) — assert the skip count so a
        // future edit to this file's portable content, or a regression that
        // silently starts skipping MORE than expected, shows up here rather
        // than as a quietly-shrinking executed count.
        assertEquals(8, result.skipped(), "expected skip count changed — see NON_PORTABLE_MARKERS");
        assertEquals(8, result.executed(), "expected executed-record count changed");
    }

    /**
     * {@code catalog/window_self_join.test} is a DuckDB optimizer regression
     * test (its whole point is a C++-side deep-copy bug in a window-function
     * self-join rewrite), and most of the queries it runs to exercise that
     * path are plain window-function/correlated-subquery SQL against a real
     * declarative table — portable, apart from: its {@code ATTACH}/{@code
     * DETACH} (this harness attaches via {@code createCatalog} instead), its
     * one query using DuckDB's {@code QUALIFY} clause (Trino has no {@code
     * QUALIFY} — confirmed by actually running it, not assumed: {@code
     * mismatched input 'ROW_NUMBER'} where the parser choked past the
     * unrecognized keyword), and its trailing {@code duckdb_functions()}
     * introspection check.
     */
    private static final List<String> WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "QUALIFY", "duckdb_functions(");

    @Test
    @Timeout(180)
    void windowSelfJoinMatchesTheRealTestFile() throws Exception {
        Path testFile = Path.of(System.getProperty("user.home"),
                "Development/vgi/test/sql/integration/catalog/window_self_join.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(),
                "~/Development/vgi/test/sql/integration/catalog/window_self_join.test not present");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(runner, session, testFile,
                "example.", TRINO_CATALOG, WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 4 non-portable records: ATTACH, the QUALIFY query, duckdb_functions(), DETACH.
        assertEquals(4, result.skipped(), "expected skip count changed — see WINDOW_SELF_JOIN_NON_PORTABLE_MARKERS");
        assertEquals(3, result.executed(), "expected executed-record count changed");
    }
}
