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
 * Real, end-to-end coverage of {@code VgiTableInOutTableFunctions}' CLASSIC
 * (real {@code TableInput}-argument) table-in-out support against a live
 * {@code vgi-fixture-worker}: {@code echo} (pure passthrough, no extra args)
 * and {@code repeat_inputs} (a scalar arg BEFORE the {@code TableInput} arg,
 * plus real multi-batch input large enough to force more than one {@code
 * exchange()} turn).
 *
 * <p>{@code filter_by_setting}/{@code secret_in_out} (need session-settings/
 * secrets wiring for classic table-in-out, not implemented yet) and anything
 * with a finalize phase ({@code sum_all_columns}, {@code
 * substream_partial_sum}, {@code multi_batch_finish}) are deliberately not
 * covered here — see {@code VgiTableInOutTableFunctions}'s own javadoc for
 * the v1 scope this connector actually implements.
 */
final class VgiTableInOutTableFunctionsTest {

    private static DistributedQueryRunner runner;
    private static Session session;

    @BeforeAll
    static void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping classic table-in-out function tests");

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
    void echoPassesThroughEveryRowUnchanged() {
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.echo(TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b))) ORDER BY a");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).getField(0));
        assertEquals(2, rows.get(0).getField(1));
        assertEquals(3, rows.get(1).getField(0));
        assertEquals(4, rows.get(1).getField(1));
        assertEquals(5, rows.get(2).getField(0));
        assertEquals(6, rows.get(2).getField(1));
    }

    @Test
    @Timeout(60)
    void echoOnAnEmptyTableProducesZeroRows() {
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.echo(TABLE(SELECT 1 AS a, 2 AS b WHERE false)))");
        assertEquals(0, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void repeatInputsScalarArgBeforeTheTableArgDuplicatesEachRow() {
        MaterializedResult result = query(
                "SELECT * FROM TABLE(vgi_example.main.repeat_inputs(3, TABLE(VALUES (1, 2)) T(a, b)))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(3, rows.size());
        for (MaterializedRow row : rows) {
            assertEquals(1, row.getField(0));
            assertEquals(2, row.getField(1));
        }
    }

    @Test
    @Timeout(60)
    void repeatInputsHandlesEnoughRowsToForceMultipleExchangeTurns() {
        // Forces Trino's own page size limits to split this into more than one Page, so the
        // VgiTableInOutDataProcessor's process()-per-page loop genuinely runs more than once —
        // not just a single-page happy path.
        MaterializedResult result = query(
                "SELECT count(*) FROM TABLE(vgi_example.main.repeat_inputs(2, "
                        + "TABLE(SELECT * FROM UNNEST(SEQUENCE(1, 10000)) t(a)) ))");
        assertEquals(20000L, result.getMaterializedRows().get(0).getField(0));
    }
}
