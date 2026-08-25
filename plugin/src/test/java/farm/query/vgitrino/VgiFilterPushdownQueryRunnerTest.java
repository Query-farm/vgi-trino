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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Proves predicate pushdown ({@code VgiFilterTranslator}/{@code VgiFilterEncoding})
 * is not just harmless but CORRECT, against a real worker function that
 * actually trusts the pushed filter — {@code example.data.filter_echo_table}
 * declares {@code auto_apply_filters=true}, meaning the worker itself prunes
 * rows before they ever reach Trino. That is exactly the scenario where a
 * wrong translation would silently drop rows Trino's own re-check could never
 * recover (see the README's Scope section and {@code VgiFilterTranslator}'s
 * javadoc for why this was held back until it had this test).
 *
 * <p>Every assertion here also checks the table's {@code pushed_filters}
 * diagnostic column — the worker's own echo of what it parsed — is not the
 * "(none)" sentinel, proving a filter genuinely reached and was applied by
 * the worker, not that the (correct) result merely happened to match without
 * one.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiFilterPushdownQueryRunnerTest {

    private static final String CATALOG = "vgi_example";

    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping live fixture-worker test");

        session = TestingSession.testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("data")
                .build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", "example",
                "vgi.connections", "2"));
    }

    @AfterAll
    void stop() {
        if (runner != null) runner.close();
    }

    @Test
    @Timeout(120)
    void equalityFilterOnIntegerColumnIsAppliedByTheWorker() {
        MaterializedResult result = runner.execute(session,
                "SELECT n, s, pushed_filters FROM filter_echo_table WHERE n = 42");
        assertEquals(1, result.getRowCount(), "the worker must prune to exactly one row");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(42L, row.getField(0));
        assertEquals("row_42", row.getField(1));
        assertNotEquals("(none)", row.getField(2), "a filter must have actually reached the worker");
    }

    @Test
    @Timeout(120)
    void rangeFilterOnIntegerColumnIsAppliedByTheWorker() {
        MaterializedResult result = runner.execute(session,
                "SELECT n, pushed_filters FROM filter_echo_table WHERE n > 5 AND n < 10 ORDER BY n");
        assertEquals(List.of(6L, 7L, 8L, 9L),
                result.getMaterializedRows().stream().map(r -> (Long) r.getField(0)).toList());
        assertNotEquals("(none)", result.getMaterializedRows().get(0).getField(1));
    }

    @Test
    @Timeout(120)
    void inListOnIntegerColumnIsAppliedByTheWorker() {
        MaterializedResult result = runner.execute(session,
                "SELECT n, pushed_filters FROM filter_echo_table WHERE n IN (1, 3, 5) ORDER BY n");
        assertEquals(List.of(1L, 3L, 5L),
                result.getMaterializedRows().stream().map(r -> (Long) r.getField(0)).toList());
        assertNotEquals("(none)", result.getMaterializedRows().get(0).getField(1));
    }

    @Test
    @Timeout(120)
    void equalityFilterOnVarcharColumnIsAppliedByTheWorker() {
        MaterializedResult result = runner.execute(session,
                "SELECT n, pushed_filters FROM filter_echo_table WHERE s = 'row_7'");
        assertEquals(1, result.getRowCount());
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(7L, row.getField(0));
        assertNotEquals("(none)", row.getField(1));
    }

    @Test
    @Timeout(120)
    void unfilteredScanStillReturnsAllRows() {
        // The control: no WHERE clause, no pushdown, all 100 rows — proves the
        // pushdown path above isn't just coincidentally matching a table that
        // always returns everything. count(*) specifically: Trino resolves it
        // to a zero-column projection, which — against this table's real
        // projection_pushdown=true function — used to come back as 0 rows
        // instead of 100 (VgiSplitManager/VgiPageSource sent an explicit
        // projection_ids=[] rather than treating "no desired columns" as "no
        // restriction"; fixed alongside this test finding it).
        MaterializedResult result = runner.execute(session, "SELECT count(*) FROM filter_echo_table");
        assertEquals(100L, result.getMaterializedRows().get(0).getField(0));
    }
}
