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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Real, end-to-end coverage of {@code VgiAggregateFunctions} against a live
 * {@code vgi-fixture-worker}: plain (ungrouped) aggregation, {@code GROUP BY},
 * a nullary aggregate ({@code vgi_count}), a two-field-state aggregate
 * ({@code vgi_avg}), and enough rows in one group to force the pending-row
 * buffer's flush threshold — see {@code VgiAggregateFunctions.AggregateState}.
 */
final class VgiAggregateFunctionsTest {

    private static DistributedQueryRunner runner;
    private static Session session;

    @BeforeAll
    static void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping aggregate function tests");

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

    private static Object scalar(String sql) {
        MaterializedResult result = runner.execute(session, sql);
        return result.getMaterializedRows().get(0).getField(0);
    }

    @Test
    @Timeout(60)
    void vgiSumOverAnUngroupedTable() {
        assertEquals(45L, scalar(
                "SELECT vgi_example.main.vgi_sum(CAST(i AS BIGINT)) FROM UNNEST(SEQUENCE(0, 9, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiSumSkipsNullRowsMatchingDefaultNullHandling() {
        assertEquals(4L, scalar(
                "SELECT vgi_example.main.vgi_sum(x) FROM "
                        + "(VALUES (CAST(1 AS BIGINT)), (NULL), (CAST(3 AS BIGINT))) t(x)"));
    }

    @Test
    @Timeout(60)
    void vgiSumWithGroupBy() {
        MaterializedResult result = runner.execute(session,
                "SELECT g, vgi_example.main.vgi_sum(CAST(x AS BIGINT)) FROM "
                        + "(VALUES (1, 10), (1, 20), (2, 5), (2, 7), (2, 1)) AS t(g, x) "
                        + "GROUP BY g ORDER BY g");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getField(0));
        assertEquals(30L, rows.get(0).getField(1));
        assertEquals(2, rows.get(1).getField(0));
        assertEquals(13L, rows.get(1).getField(1));
    }

    @Test
    @Timeout(60)
    void vgiCountIsANullaryAggregate() {
        assertEquals(10L, scalar("SELECT vgi_example.main.vgi_count() FROM UNNEST(SEQUENCE(1, 10, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiAvgHandlesTwoFieldState() {
        // (1+2+3+4)/4 = 2.5
        assertEquals(2.5d, scalar(
                "SELECT vgi_example.main.vgi_avg(CAST(i AS BIGINT)) FROM UNNEST(SEQUENCE(1, 4, 1)) t(i)"));
    }

    @Test
    @Timeout(120)
    void vgiSumFlushesAcrossMultipleUpdateBatches() {
        // FLUSH_THRESHOLD is 4096 — 10,000 rows forces at least two aggregate_update RPCs.
        long n = 10_000;
        long expected = n * (n + 1) / 2;
        assertEquals(expected, scalar(
                "SELECT vgi_example.main.vgi_sum(CAST(i AS BIGINT)) FROM UNNEST(SEQUENCE(1, " + n + ", 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiSumOfAllNullsReturnsNull() {
        assertNull(scalar("SELECT vgi_example.main.vgi_sum(x) FROM (VALUES (CAST(NULL AS BIGINT)), (NULL)) t(x)"));
    }
}
