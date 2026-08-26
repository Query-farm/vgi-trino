// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Real, end-to-end coverage of {@code VgiTableBufferingFunctions}' support for VGI's third,
 * distinct table-in-out kind — {@code TableBufferingFunction} — against a live {@code
 * vgi-fixture-worker}: {@code sum_all_columns} (single-bucket accumulation: every {@code
 * process()} call folds into one running total, {@code combine()} collapses to a single {@code
 * finalize_state_id}) and {@code sum_all_columns_simple_distributed} (behaviourally identical,
 * kept as its own fixture so BOTH real {@code TableBufferingFunction} registrations in the worker
 * get exercised — see the real fixture's own javadoc).
 *
 * <p>Before this connector's {@code VgiTableBufferingFunctions}/{@code
 * VgiTableBufferingDataProcessor} existed, {@code VgiTableInOutTableFunctionsTest} explicitly
 * documented these two functions as "never in scope" because {@code
 * VgiTableInOutTableFunctions.discover} could not tell a {@code TableBufferingFunction} apart from
 * a classic one and would have mis-registered it, crashing at query time with {@code
 * ValueError: Unsupported init phase for TableBufferingFunction}.
 */
final class VgiTableBufferingFunctionsTest {

    private static DistributedQueryRunner runner;
    private static Session session;

    @BeforeAll
    static void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping table-buffering function tests");

        session = TestingSession.testSessionBuilder().setCatalog("vgi_example").setSchema("main").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", "example"));
    }

    @AfterAll
    static void stop() {
        if (runner != null) runner.close();
    }

    private static MaterializedResult query(String sql) {
        return runner.execute(session, sql);
    }

    @Test
    @Timeout(60)
    void sumAllColumnsSingleRow() {
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.sum_all_columns(TABLE(VALUES (1, 2)) T(a, b)))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get(0).getField(0));
        assertEquals(2L, rows.get(0).getField(1));
    }

    @Test
    @Timeout(60)
    void sumAllColumnsMultipleRows() {
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.sum_all_columns("
                        + "TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(9L, rows.get(0).getField(0));
        assertEquals(12L, rows.get(0).getField(1));
    }

    @Test
    @Timeout(60)
    void sumAllColumnsOnAnEmptyTableProducesZero() {
        // Exercises the zero-Sink-call path: ensureExecutionId() must still run once (lazily, at
        // end-of-input) so table_buffering_combine has a valid execution_id to cold-load, even
        // though sinkOneBatch() never ran.
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.sum_all_columns("
                        + "TABLE(SELECT 1 AS a WHERE false)))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(0L, rows.get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void sumAllColumnsHandlesEnoughRowsToForceMultipleSinkCalls() {
        // Forces Trino's own page size limits to split this into more than one Page, so the
        // VgiTableBufferingDataProcessor's sinkOneBatch()-per-page loop — and the resulting
        // multi-element state_ids list handed to table_buffering_combine — genuinely runs more
        // than once, not just a single-batch happy path.
        MaterializedResult result = query(
                "SELECT n FROM TABLE(vgi_example.main.sum_all_columns("
                        + "TABLE(SELECT * FROM UNNEST(SEQUENCE(1, 10000)) t(n))))");
        assertEquals(50005000L, result.getMaterializedRows().get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void sumAllColumnsSimpleDistributedProducesTheSameAnswer() {
        // Behaviourally identical to sum_all_columns, but a SEPARATE real TableBufferingFunction
        // registration — exercises this connector's discovery/dispatch on a second function name,
        // not just a second call of the same one. Column b is explicitly CAST to DOUBLE rather
        // than left as a bare `1.0` literal — Trino infers `decimal(2,1)` for that, and
        // VgiTypeMapping.toArrowField deliberately doesn't cover DECIMAL in the Trino->Arrow
        // direction yet (needs real Int128<->Arrow-decimal handling — see its own javadoc); this
        // test's actual intent is just a second DOUBLE-typed registration, not decimal support.
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.sum_all_columns_simple_distributed("
                        + "TABLE(VALUES (1, CAST(1.0 AS DOUBLE)), (2, CAST(2.0 AS DOUBLE)), "
                        + "(3, CAST(3.0 AS DOUBLE))) T(a, b)))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(6L, rows.get(0).getField(0));
        assertEquals(6.0, (double) rows.get(0).getField(1), 0.0001);
    }
}
