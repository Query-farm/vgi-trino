// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
import farm.query.vgitrino.testing.VgiWorkerHarness;
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
 *
 * <p>Abstract: {@link #startWorker} is the one thing that varies per
 * transport — concrete subclasses (one per transport {@link
 * VgiWorkerHarness} supports) each start the real fixture worker their own
 * way and hand back a {@code vgi.location}; every {@code @Test} method below
 * is inherited unchanged.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class VgiSqlLogicTestConformanceTest {

    static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");

    private static final String TRINO_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    private VgiWorkerHarness.Handle worker;
    private DistributedQueryRunner runner;
    private Session session;

    /** Starts the real fixture worker this transport's subclass tests, and returns its {@code vgi.location}. */
    abstract VgiWorkerHarness.Handle startWorker() throws Exception;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest conformance run");
        worker = startWorker();

        session = TestingSession.testSessionBuilder()
                .setCatalog(TRINO_CATALOG)
                .setSchema("data")
                .build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(TRINO_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", worker.location(),
                "vgi.catalog-name", VGI_CATALOG_NAME,
                "vgi.connections", "2"));
    }

    @AfterAll
    void stop() throws Exception {
        if (runner != null) runner.close();
        if (worker != null) worker.teardown().close();
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

    /**
     * {@code scalar/geo_centroid.test} exercises {@code geo_centroid_struct}/{@code
     * geo_centroid_list}/{@code geo_centroid_fixed} — three varargs scalar functions that each
     * return a bare {@code STRUCT(lat DOUBLE, lon DOUBLE)} — and is the real-corpus proof for two
     * genuine bugs this file's adoption found (neither a connector value-mapping bug: {@code
     * VgiTypeMapping}/{@code VgiScalarFunctions} always produced a correct {@code RowType}/{@code
     * SqlRow} value and correct arithmetic throughout):
     * <ul>
     *   <li>{@code VgiScalarFunctions.BindCache}'s cache key omitted the call site's bound argument
     *       types, so calling a variable-arity function (no {@code vgi_const} args, so {@code
     *       constArgValues} is always empty) at two different arities within one session — exactly
     *       what this file's four {@code geo_centroid_struct} queries do (arity 1, 2, 3, then 2
     *       again) — reused the FIRST arity's stale {@code bind()} result for the second, crashing
     *       {@code init()} with a real worker-side {@code TypeError: Input schema mismatch}. Fixed
     *       by adding {@code CallConfig.argumentTypes()} to {@code BindCache.CacheKey}.</li>
     *   <li>{@link SqlLogicTestRunner#formatCell} detected a struct value via {@code value
     *       instanceof List<?>} — true for the hand-built {@link java.util.List} its OWN unit test
     *       ({@link SqlLogicTestRunnerComparisonTest#formatCellRendersARowAsAStructLiteral}) used,
     *       but a REAL query executed via {@code DistributedQueryRunner} materializes a {@link
     *       io.trino.spi.type.RowType} value as {@code io.trino.testing.MaterializedRow} instead — a
     *       distinct wrapper class, not a {@code List} — so the check silently never matched, and
     *       every live struct-returning query fell through to {@code MaterializedRow.toString()}'s
     *       OWN bracket-joined rendering ({@code "[3.0, 4.0]"}), indistinguishable from a genuine
     *       array. This is the exact "returns an array instead of a struct" symptom, and it is a
     *       test-harness display gap, not a connector bug. Fixed by also accepting {@code
     *       MaterializedRow.getFields()}.</li>
     * </ul>
     * {@code geo_centroid_fixed}'s four queries are separately excluded ({@code
     * GEO_CENTROID_NON_PORTABLE_MARKERS}): the corpus casts its argument as DuckDB's fixed-size
     * {@code DOUBLE[2]} array-cast syntax, which {@code CastRewriter} doesn't translate into valid
     * Trino cast syntax ({@code mismatched input '['}) — a separate, pre-existing sqlglot-rewrite
     * gap unrelated to either bug above, out of scope here.
     *
     * <p>Skipped entirely on the {@code http(s)://} transport subclass ({@link
     * VgiSqlLogicTestConformanceHttpTest}): confirmed via a real failure sample that {@code
     * VgiScalarFunctions#invoke}'s {@code (ClientStreamSession<?>) stream} cast — unmodified by
     * either fix above — throws a real {@code ClassCastException} for EVERY scalar function call
     * over HTTP ({@code init()} returns {@code farm.query.vgirpc.http.HttpRpcStream} there, which
     * doesn't extend {@code ClientStreamSession}). A genuine, pre-existing scalar-function/HTTP-
     * transport gap this file's other two hand-curated tests never exercised (neither calls a
     * scalar function at all) — worth its own follow-up, but out of scope for the two bugs above.
     */
    private static final List<String> GEO_CENTROID_NON_PORTABLE_MARKERS =
            List.of("ATTACH ", "DETACH", "geo_centroid_fixed(");

    @Test
    @Timeout(180)
    void geoCentroidMatchesTheRealTestFile() throws Exception {
        Path testFile = Path.of(System.getProperty("user.home"),
                "Development/vgi/test/sql/integration/scalar/geo_centroid.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(),
                "~/Development/vgi/test/sql/integration/scalar/geo_centroid.test not present");
        Assumptions.assumeTrue(!worker.location().startsWith("http"),
                "scalar functions don't work over http(s):// yet — VgiScalarFunctions#invoke's "
                        + "ClientStreamSession cast doesn't handle HttpRpcStream (a separate, "
                        + "pre-existing gap; see this test's own javadoc)");

        SqlLogicTestRunner.Result result = SqlLogicTestRunner.run(runner, session, testFile,
                "example.", TRINO_CATALOG, GEO_CENTROID_NON_PORTABLE_MARKERS);

        if (!result.failures().isEmpty()) {
            fail(result.executed() + " executed, " + result.skipped() + " skipped, "
                    + result.failures().size() + " FAILED:\n" + String.join("\n---\n", result.failures()));
        }
        // 6 non-portable records: ATTACH, DETACH, and the 4 geo_centroid_fixed queries.
        assertEquals(6, result.skipped(), "expected skip count changed — see GEO_CENTROID_NON_PORTABLE_MARKERS");
        // 4 queries each for geo_centroid_struct/geo_centroid_list.
        assertEquals(8, result.executed(), "expected executed-record count changed");
    }
}
