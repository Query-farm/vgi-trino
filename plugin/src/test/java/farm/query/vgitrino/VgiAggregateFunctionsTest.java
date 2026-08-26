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

    @Test
    @Timeout(60)
    void vgiPercentileHandlesTheConstArgument() {
        assertEquals(0.0d, scalar(
                "SELECT vgi_example.main.vgi_percentile(CAST(i AS DOUBLE), 0.0) FROM UNNEST(SEQUENCE(0, 9, 1)) t(i)"));
        assertEquals(5.0d, scalar(
                "SELECT vgi_example.main.vgi_percentile(CAST(i AS DOUBLE), 0.5) FROM UNNEST(SEQUENCE(0, 9, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiPercentileRebindsWhenTheConstValueChangesAcrossSeparateQueries() {
        // Each query is its own accumulator/bind (lazy-bind-on-first-row) -- confirms two
        // different constant values in two separate calls both bind and compute correctly,
        // not just whichever one happened to run first.
        assertEquals(9.0d, scalar(
                "SELECT vgi_example.main.vgi_percentile(CAST(i AS DOUBLE), 0.9) FROM UNNEST(SEQUENCE(0, 9, 1)) t(i)"));
        assertEquals(0.0d, scalar(
                "SELECT vgi_example.main.vgi_percentile(CAST(i AS DOUBLE), 0.0) FROM UNNEST(SEQUENCE(0, 9, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiSumAllHandlesASingleVarargColumn() {
        // vgi_sum_all's vararg is any-typed, and its return type is DOUBLE regardless of the
        // input column's own type (confirmed by direct testing, not assumed).
        assertEquals(15.0d, scalar(
                "SELECT vgi_example.main.vgi_sum_all(CAST(i AS BIGINT)) FROM UNNEST(SEQUENCE(1, 5, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiSumAllHandlesTwoVarargColumns() {
        assertEquals(30.0d, scalar(
                "SELECT vgi_example.main.vgi_sum_all(CAST(i AS BIGINT), CAST(i AS BIGINT)) "
                        + "FROM UNNEST(SEQUENCE(1, 5, 1)) t(i)"));
    }

    @Test
    @Timeout(60)
    void vgiSumAllHandlesThreeVarargColumnsWithDifferentValues() {
        // Explicit DOUBLE casts, not bare decimal literals — a bare "1.0" in a VALUES clause
        // infers as DECIMAL in Trino, which VgiTypeMapping deliberately doesn't map in the
        // Trino->Arrow direction (a separate, pre-existing, documented limitation, not part of
        // this test).
        MaterializedResult result = runner.execute(session,
                "SELECT vgi_example.main.vgi_sum_all(a, b, c) FROM (VALUES "
                        + "(CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE), CAST(3.0 AS DOUBLE)), "
                        + "(CAST(4.0 AS DOUBLE), CAST(5.0 AS DOUBLE), CAST(6.0 AS DOUBLE))) AS t(a, b, c)");
        assertEquals(21.0d, result.getMaterializedRows().get(0).getField(0));
    }

    // ------------------------------------------------------------------
    // OVER (...) — windowed usage. See VgiAggregateFunctions' class javadoc,
    // "Windowed usage" section, for the full root-cause writeup: Trino's
    // automatic per-frame-recompute window fallback (no combineFunction, no
    // windowAccumulator override) genuinely DOES work correctly for a VGI
    // aggregate whose update()/finalize() implement real, general-purpose
    // "compute the aggregate over an arbitrary row subset" semantics — but it
    // is a real correctness gap, not merely a performance ceiling, for a VGI
    // aggregate (like vgi_window_median) whose only correct implementation
    // lives behind VGI's separate windowed-aggregate RPC family
    // (aggregate_window_init/aggregate_window), which this connector cannot
    // reach: that protocol is entirely absent from the farm.query:vgi Java
    // SDK this connector depends on (confirmed directly against the
    // vgi-0.27.0.jar class list — no AggregateWindow* types, no
    // aggregate_window* method on VgiService), and separately, Trino's own
    // engine (LocalExecutionPlanner) hardcodes every FunctionKind.AGGREGATE
    // resolved function's OVER (...) usage to go through
    // AggregationImplementation/AggregationWindowFunctionSupplier regardless
    // — FunctionProvider.getWindowFunctionSupplier is never consulted for an
    // aggregate-kind function, only for genuinely WINDOW-kind ones
    // (rank/lag/etc.), so there is no engine-level hook to reach a different
    // RPC family for these calls even if the SDK had them.
    // ------------------------------------------------------------------

    @Test
    @Timeout(60)
    void vgiSumOverASlidingWindowFrameComputesCorrectly() {
        // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING is a genuinely sliding (shrinking-then-growing)
        // frame -- Trino's AggregateWindowFunction.buildNewFrame takes the "couldn't/didn't want to
        // modify" branch on every row (no removeInputFunction registered) and creates a FRESH
        // AggregateState -- a fresh aggregate_bind/aggregate_update/aggregate_finalize sequence --
        // for every single output row. vgi_sum's update()/finalize() genuinely recompute the sum of
        // whatever rows they're given, so this produces the correct centered 3-row moving sum.
        MaterializedResult result = runner.execute(session,
                "SELECT x, vgi_example.main.vgi_sum(CAST(x AS BIGINT)) "
                        + "OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) "
                        + "FROM UNNEST(SEQUENCE(1, 5, 1)) AS t(x) ORDER BY x");
        List<MaterializedRow> rows = result.getMaterializedRows();
        // x:      1  2  3  4  5
        // window: 1+2, 1+2+3, 2+3+4, 3+4+5, 4+5
        long[] expected = {3L, 6L, 9L, 12L, 9L};
        assertEquals(5, rows.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], rows.get(i).getField(1), "row " + i);
        }
    }

    @Test
    @Timeout(60)
    void vgiSumOverAGrowingWindowFrameComputesCorrectly() {
        // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is growing-only: frameStart never moves,
        // so AggregateWindowFunction.processRow takes the "same or expanding frame" branch and keeps
        // accumulating into the SAME AggregateState/accumulator instance for the whole partition
        // (only ever calling accumulate(currentEnd+1, frameEnd), never rebuilding) -- a meaningfully
        // different code path from the sliding-frame case above (fresh accumulator every row).
        MaterializedResult result = runner.execute(session,
                "SELECT x, vgi_example.main.vgi_sum(CAST(x AS BIGINT)) "
                        + "OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                        + "FROM UNNEST(SEQUENCE(1, 5, 1)) AS t(x) ORDER BY x");
        List<MaterializedRow> rows = result.getMaterializedRows();
        long[] expected = {1L, 3L, 6L, 10L, 15L};
        assertEquals(5, rows.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], rows.get(i).getField(1), "row " + i);
        }
    }

    @Test
    @Timeout(60)
    void vgiWindowMedianOverASlidingFrameReturnsNullNotRealMedians() {
        // Ground truth for a genuine, documented ceiling -- NOT the fix target. This is the exact
        // query from vgi's corpus (test/sql/integration/aggregate/window.test), which expects real
        // medians ([1.0, 2.0], [2.0, 2.5], [3.0, 3.0], [4.0, 4.0], [5.0, 5.0], [5.5, ... ]) when run
        // against the C++ extension's native windowed-aggregate dispatch. Run through vgi-trino, it
        // returns NULL for every row: vgi_window_median's update()/finalize() (the only RPCs this
        // connector can reach for OVER usage, per Trino's own per-frame recompute fallback) are a
        // deliberate no-op stub in the fixture -- its real median logic lives ONLY behind the
        // window() callback, reachable only via VGI's separate aggregate_window_init/aggregate_window
        // RPCs, which this connector's Java SDK dependency doesn't expose at all. See the class
        // javadoc's "Windowed usage" section and the block comment above this test group.
        MaterializedResult result = runner.execute(session,
                "SELECT x, vgi_example.main.vgi_window_median(CAST(x AS DOUBLE)) "
                        + "OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) "
                        + "FROM UNNEST(SEQUENCE(1, 7, 1)) AS t(x) ORDER BY x");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(7, rows.size());
        for (MaterializedRow row : rows) {
            assertNull(row.getField(1), "expected NULL (the documented ceiling), not a real median");
        }
    }
}
