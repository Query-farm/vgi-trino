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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Hand-ports the portable core of {@code ~/Development/vgi/test/sql/integration/splits/*.test}
 * — the C++ VGI repo's own cross-SDK conformance suite for split-based
 * parallelism — into Trino-dialect queries against the REAL reference Python
 * fixture worker's split functions ({@code main.split_sequence},
 * {@code main.split_paginated}, {@code main.split_fail_at}, etc. — the exact
 * same fixtures every other SDK's conformance suite runs against), called via
 * Trino's {@code TABLE(...)} polymorphic-table-function syntax now that
 * {@code VgiTableFunction} support exists.
 *
 * <p>Unlike {@link VgiSqlLogicTestConformanceTest}, this does not parse and
 * replay the real {@code .test} files verbatim — DuckDB's own {@code
 * schema.function(args)} call syntax and its {@code :=} named-argument form
 * aren't valid Trino SQL, so each file's assertions are re-expressed by hand
 * as {@code TABLE(catalog.schema.fn(arg => value, ...))} calls. The ROW SETS
 * and EXPECTED VALUES below are transcribed from the real files, not
 * reinvented — see each method's javadoc for which file it ports and what
 * (if anything) was intentionally dropped.
 *
 * <h2>Files NOT ported, and why</h2>
 * <ul>
 *   <li>{@code rollback.test} — {@code SET vgi_split_scans=false} is a DuckDB
 *       extension setting with no Trino equivalent; there is no "disable the
 *       split client path" knob to test here.</li>
 *   <li>{@code pushdown.test} — asserts a filter reaches {@code plan()} for a
 *       table FUNCTION. Trino's {@code ConnectorTableFunction} SPI (483) has
 *       no {@code Constraint}/{@code DynamicFilter} hook at all for PTFs (see
 *       {@code VgiSplitSource}'s javadoc) — this is impossible to port
 *       truthfully, not merely unimplemented; porting it would either always
 *       fail or have to lie about what's pushed.</li>
 *   <li>{@code dynamic_filters.test} — same PTF-has-no-filter-hook boundary;
 *       ported instead, against a plain (non-PTF) declarative table where
 *       Trino's dynamic filtering genuinely applies, as
 *       {@code VgiDynamicFilteringQueryRunnerTest}.</li>
 *   <li>{@code multi_branch.test} — needs {@code catalog_table_scan_branches_get}
 *       support, which this connector doesn't implement (see the README's
 *       Scope section).</li>
 *   <li>{@code ttl_floor.test} — asserts a client-side {@code
 *       vgi_split_token_min_ttl_seconds} floor DuckDB enforces at plan time;
 *       this connector has no equivalent setting or check.</li>
 *   <li>{@code plan_bounds.test} — asserts a client-side page-count cap on
 *       runaway pagination. Not portable here (no reference fixture worker
 *       function actually cursors forever), but the underlying property IS
 *       now implemented and tested — see {@code VgiPlanPageCapTest}, against
 *       a hand-rolled fixture built for exactly this, the same reasoning
 *       {@code VgiConnectorSplitParallelismTest} gives for its own.</li>
 *   <li>{@code cache_interaction.test} — entirely about DuckDB's table-function
 *       result cache ({@code vgi_result_cache_stats()}, {@code duckdb_logs}),
 *       which this connector doesn't have.</li>
 *   <li>{@code transaction_scope.test} — this connector's {@code beginTransaction}/
 *       {@code commit}/{@code rollback} are documented no-ops (VGI's own
 *       {@code catalog_transaction_begin/commit/rollback} aren't wired up — see
 *       {@code VgiConnector}'s javadoc), so there is no per-connector snapshot
 *       property to verify. Reproducing the file's actual shape would also need
 *       real cross-statement transaction-ID chaining through {@code trino-testing}'s
 *       {@code Session} (a plain {@code DistributedQueryRunner.execute(session, sql)}
 *       call is a single autocommit-scoped statement — a later {@code COMMIT}
 *       against the same {@code Session} object fails with "No transaction in
 *       progress" rather than closing the one an earlier {@code START TRANSACTION}
 *       opened), which is harness plumbing this file doesn't otherwise need.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiSplitsFixtureConformanceTest {

    private static final String CATALOG = "vgi_splits";
    private static final String PAGED_CATALOG = "vgi_splits_paged";

    private DistributedQueryRunner runner;
    private Session session;
    private Session pagedSession;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping splits fixture conformance run");
        String location = "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker";

        session = TestingSession.testSessionBuilder().setCatalog(CATALOG).setSchema("main").build();
        pagedSession = TestingSession.testSessionBuilder().setCatalog(PAGED_CATALOG).setSchema("main").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", location,
                "vgi.catalog-name", "example",
                "vgi.connections", "16"));
        // A second attach of the same worker kind, capped to 4 splits per
        // table_function_plan page — cursor_pagination.test's whole point is
        // exercising a multi-page enumeration, which the default (1000)
        // cap would make this fixture's row counts too small to ever trigger.
        runner.createCatalog(PAGED_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", location,
                "vgi.catalog-name", "example",
                "vgi.connections", "16",
                "vgi.max-splits-per-response", "4"));
    }

    @AfterAll
    void stop() {
        if (runner != null) runner.close();
    }

    private long scalarLong(Session s, String sql) {
        return (long) runner.execute(s, sql).getMaterializedRows().get(0).getField(0);
    }

    private MaterializedRow row(Session s, String sql) {
        return runner.execute(s, sql).getMaterializedRows().get(0);
    }

    /** Ports {@code parity.test}: a split scan agrees row-for-row with its non-split twin. */
    @Test
    @Timeout(120)
    void parity() {
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 1))").getFields());
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 5))").getFields());
        // Uneven division — the remainder has to land somewhere, exactly once.
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 4))").getFields());
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 7))").getFields());
        // More splits than rows: the surplus splits are empty, and empty must not truncate.
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 25))").getFields());
        // Identical to the non-split twin, not merely self-consistent, in both directions.
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM ("
                + "SELECT n FROM TABLE(vgi_splits.main.split_sequence(n => 500, splits => 13)) EXCEPT SELECT n FROM TABLE(vgi_splits.main.sequence(count => 500)))"));
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM ("
                + "SELECT n FROM TABLE(vgi_splits.main.sequence(count => 500)) EXCEPT SELECT n FROM TABLE(vgi_splits.main.split_sequence(n => 500, splits => 13)))"));
    }

    /** Ports {@code cursor_pagination.test}, against {@link #PAGED_CATALOG}'s 4-per-page cap. */
    @Test
    @Timeout(120)
    void cursorPagination() {
        // 12 splits at 4 per page — three pages, none overlapping.
        assertEquals(List.of(200L, 200L, 19900L), row(pagedSession,
                "SELECT count(*), count(DISTINCT n), sum(n) FROM TABLE(vgi_splits_paged.main.split_paginated(n => 200, splits => 12))")
                .getFields());
        // A page boundary that falls exactly on the split count.
        assertEquals(List.of(64L, 2016L), row(pagedSession,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits_paged.main.split_paginated(n => 64, splits => 8))").getFields());
        // Fewer splits than one page: the enumeration ends without ever cursoring.
        assertEquals(List.of(10L, 45L), row(pagedSession,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits_paged.main.split_paginated(n => 10, splits => 2))").getFields());
        // Row-for-row identical to the same data enumerated in one page.
        assertEquals(0L, scalarLong(pagedSession, "SELECT count(*) FROM ("
                + "SELECT n FROM TABLE(vgi_splits_paged.main.split_paginated(n => 100, splits => 12)) "
                + "EXCEPT SELECT n FROM TABLE(vgi_splits.main.split_sequence(n => 100, splits => 12)))"));
    }

    /** Ports {@code zero_splits.test}: a fully-pruned plan is legal and empty, never a crash. */
    @Test
    @Timeout(60)
    void zeroSplits() {
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_zero(n => 100, splits => 8))"));
        // List.of rejects null elements, so this can't reuse the List.of(...)
        // shorthand every other assertion in this file uses — sum(n) over an
        // empty input is a genuine NULL, not a value List.of could hold.
        assertEquals(java.util.Arrays.asList(0L, null), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_zero(n => 100, splits => 8))").getFields());
        // UNION ALL of empty and non-empty is the non-empty one.
        assertEquals(List.of(10L, 45L), row(session, "SELECT count(*), sum(n) FROM ("
                + "SELECT n FROM TABLE(vgi_splits.main.split_zero(n => 100, splits => 8)) "
                + "UNION ALL SELECT n FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 3)))").getFields());
        // An empty side joined must produce an empty join, not an error.
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM "
                + "TABLE(vgi_splits.main.split_zero(n => 10, splits => 2)) z "
                + "JOIN TABLE(vgi_splits.main.split_sequence(n => 10, splits => 2)) s ON s.n = z.n"));
    }

    /** Ports {@code zero_row_split.test}: an interleaved zero-ROW split must not end the reader. */
    @Test
    @Timeout(60)
    void zeroRowSplit() {
        assertEquals(List.of(10L, 45L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_empty_ranges(n => 10, splits => 4))").getFields());
        assertEquals(List.of(200L, 19900L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_empty_ranges(n => 200, splits => 16))").getFields());
        // The SAME row set, not merely the same count.
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM ("
                + "SELECT n FROM TABLE(vgi_splits.main.split_empty_ranges(n => 200, splits => 16)) "
                + "EXCEPT SELECT n FROM TABLE(vgi_splits.main.sequence(count => 200)))"));
    }

    /** Ports {@code one_split.test}: the degenerate one-split scan still goes through the whole path. */
    @Test
    @Timeout(60)
    void oneSplit() {
        assertEquals(List.of(100L, 4950L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 100, splits => 1))").getFields());
        assertEquals(List.of(100L, 4950L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 100, splits => 16))").getFields());
        // Every row exactly once — a second reader "helping" would show up as duplicates.
        assertEquals(List.of(500L, 500L), row(session,
                "SELECT count(*), count(DISTINCT n) FROM TABLE(vgi_splits.main.split_sequence(n => 500, splits => 1))")
                .getFields());
        // A one-split scan whose only split is EMPTY must still terminate cleanly.
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_sequence(n => 0, splits => 1))"));
    }

    /** Ports {@code skew.test}: correctness never depends on splits being evenly sized. */
    @Test
    @Timeout(60)
    void skew() {
        assertEquals(List.of(2000L, 2000L, 1999000L), row(session,
                "SELECT count(*), count(DISTINCT n), sum(n) FROM TABLE(vgi_splits.main.split_skewed(n => 2000, splits => 8))")
                .getFields());
        // Identical row set to the evenly-divided twin.
        assertEquals(0L, scalarLong(session, "SELECT count(*) FROM ("
                + "SELECT n FROM TABLE(vgi_splits.main.split_skewed(n => 2000, splits => 8)) "
                + "EXCEPT SELECT n FROM TABLE(vgi_splits.main.split_sequence(n => 2000, splits => 8)))"));
    }

    /** Ports {@code more_splits_than_threads.test}: 1000 splits — no duplicates, no drops. */
    @Test
    @Timeout(120)
    void moreSplitsThanThreads() {
        assertEquals(List.of(5000L, 5000L, 12497500L), row(session,
                "SELECT count(*), count(DISTINCT n), sum(n) FROM TABLE(vgi_splits.main.split_many(n => 5000, splits => 1000))")
                .getFields());
    }

    /**
     * Ports the row-correctness half of {@code batch_index.test} — not the
     * DuckDB-specific batch-index-ordering assertion, which this connector's
     * type mapper has no equivalent surface for, nor the file's
     * {@code CREATE TABLE AS SELECT} sink, since this connector is read-only
     * (VGI-backed catalogs can't be a {@code CREATE TABLE} target at all —
     * only the DIFFERENT DuckDB property the original file used it to probe,
     * ordered-insert compatibility, needed a real table; a plain re-select of
     * the same scan already proves the row set survives many splits).
     */
    @Test
    @Timeout(120)
    void batchIndexRowCorrectness() {
        assertEquals(List.of(200L, 200L, 19900L), row(session,
                "SELECT count(*), count(DISTINCT n), sum(n) FROM TABLE(vgi_splits.main.split_batch_index(n => 200, splits => 7))")
                .getFields());
        assertEquals(List.of(500L, 500L), row(session,
                "SELECT count(*), count(DISTINCT n) FROM TABLE(vgi_splits.main.split_batch_index(n => 500, splits => 40))")
                .getFields());
        assertEquals(List.of(300L, 300L, 44850L), row(session,
                "SELECT count(*), count(DISTINCT n), sum(n) FROM TABLE(vgi_splits.main.split_batch_index(n => 300, splits => 12))")
                .getFields());
        // The degenerate single-split case must use the same rule.
        assertEquals(List.of(40L, 780L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_batch_index(n => 40, splits => 1))").getFields());
        // Empty splits interleaved with the index space.
        assertEquals(List.of(5L, 5L), row(session,
                "SELECT count(*), count(DISTINCT n) FROM TABLE(vgi_splits.main.split_batch_index(n => 5, splits => 20))")
                .getFields());
    }

    /** Ports {@code partition_values.test}: partition values survive greedy split claiming. */
    @Test
    @Timeout(120)
    void partitionValues() {
        MaterializedResult grouped = runner.execute(session,
                "SELECT country, count(*), sum(sales) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 5)) "
                        + "GROUP BY country ORDER BY country");
        List<List<Object>> actual = grouped.getMaterializedRows().stream()
                .map(MaterializedRow::getFields).toList();
        assertEquals(List.of(
                List.of("BR", 5L, 1515L),
                List.of("DE", 5L, 515L),
                List.of("JP", 5L, 1015L),
                List.of("US", 5L, 15L)), actual);

        assertEquals(List.of(4L, 20L), row(session,
                "SELECT count(DISTINCT country), count(*) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 5))")
                .getFields());

        // Filtering on the partition column, as an ordinary column.
        assertEquals(List.of(5L, 1015L), row(session,
                "SELECT count(*), sum(sales) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 5)) "
                        + "WHERE country = 'JP'").getFields());
        assertEquals(10L, scalarLong(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 5)) "
                        + "WHERE country IN ('US', 'DE')"));

        // An empty partition still leaves the others intact (n=0 here means every partition is empty).
        assertEquals(0L, scalarLong(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 0))"));

        // Ordering within a partition survives.
        assertEquals(List.of(301L, 309L), row(session,
                "SELECT min(sales), max(sales) FROM TABLE(vgi_splits.main.split_partitioned(rows_per_country => 9)) "
                        + "WHERE country = 'BR'").getFields());
    }

    /**
     * Ports {@code poisoned_conn.test}: a split whose {@code init()} fails must not return its
     * connection to {@code VgiWorkerClient}'s pool poisoned — the next query on the same pool
     * must be unaffected, repeatedly, including a failure on the very first split.
     */
    @Test
    @Timeout(120)
    void poisonedConnectionDoesNotCorruptTheNextQuery() {
        // Warm the pool.
        assertEquals(40L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_sequence(n => 40, splits => 4))"));

        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 60, splits => 6, fail_at => 3, fail_in_init => true))"));
        // The load-bearing assertion: the very next query on the same worker path is correct.
        assertEquals(List.of(200L, 19900L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 200, splits => 7))").getFields());
        // ...and a differently-shaped query too.
        assertEquals(List.of(33L, 528L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 33, splits => 5))").getFields());

        // Repeat the failure and recover again.
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 60, splits => 6, fail_at => 0, fail_in_init => true))"));
        assertEquals(List.of(200L, 19900L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 200, splits => 7))").getFields());

        // Failing on the FIRST split is its own case.
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 10, splits => 2, fail_at => 0, fail_in_init => true))"));
        assertEquals(10L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_sequence(n => 10, splits => 2))"));
    }

    /**
     * Ports the surface-and-recover half (not the DuckDB result-cache assertions, which this
     * connector has no equivalent of) of {@code errors.test}: a worker failure mid-split
     * surfaces and leaves the catalog usable.
     */
    @Test
    @Timeout(120)
    void midStreamFailureSurfacesAndCatalogStaysUsable() {
        assertEquals(80L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 80, splits => 8, fail_at => -1))"));
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 80, splits => 8, fail_at => 5))"));
        assertEquals(List.of(50L, 1225L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 50, splits => 4))").getFields());
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 80, splits => 8, fail_at => 2))"));
        assertEquals(80L, scalarLong(session, "SELECT count(*) FROM TABLE(vgi_splits.main.split_fail_at(n => 80, splits => 8, fail_at => -1))"));
    }

    /**
     * Ports {@code expired_token.test}: a plan pinned to a version the live catalog has moved
     * past is refused, and the catalog is unharmed afterwards.
     */
    @Test
    @Timeout(60)
    void expiredSplitTokenIsRefused() {
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT count(*) FROM TABLE(vgi_splits.main.split_stale_plan(n => 20, splits => 2))"));
        assertThrows(RuntimeException.class, () -> runner.execute(session,
                "SELECT sum(n) FROM TABLE(vgi_splits.main.split_stale_plan(n => 5, splits => 1))"));
        // The catalog is unharmed — a refused token is a clean, bounded failure.
        assertEquals(List.of(40L, 780L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 40, splits => 4))").getFields());
    }

    /**
     * Adapts {@code cancel_midsplit.test}'s abandonment shape (not its DuckDB result-cache
     * assertions, which this connector has no equivalent of): a {@code LIMIT} that stops pulling
     * while most of a 200-split plan is still unclaimed must not corrupt the connection pool —
     * proven by a full, correct scan immediately afterwards, repeated for good measure.
     */
    @Test
    @Timeout(120)
    void abandonedScanLeavesThePoolUsable() {
        assertEquals(3L, scalarLong(session,
                "SELECT count(*) FROM (SELECT n FROM TABLE(vgi_splits.main.split_cacheable(n => 5000, splits => 200)) LIMIT 3)"));
        assertEquals(List.of(5000L, 12497500L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_cacheable(n => 5000, splits => 200))").getFields());

        // Repeated abandonment, of varying depth — each must be clean on its own.
        assertEquals(1L, scalarLong(session,
                "SELECT count(*) FROM (SELECT n FROM TABLE(vgi_splits.main.split_cacheable(n => 2000, splits => 100)) LIMIT 1)"));
        assertEquals(5L, scalarLong(session,
                "SELECT count(*) FROM (SELECT n FROM TABLE(vgi_splits.main.split_cacheable(n => 2000, splits => 100)) LIMIT 5)"));
        assertEquals(50L, scalarLong(session,
                "SELECT count(*) FROM (SELECT n FROM TABLE(vgi_splits.main.split_cacheable(n => 2000, splits => 100)) LIMIT 50)"));

        // The catalog and its pooled connections are healthy afterwards.
        assertEquals(List.of(300L, 44850L), row(session,
                "SELECT count(*), sum(n) FROM TABLE(vgi_splits.main.split_sequence(n => 300, splits => 11))").getFields());
        assertEquals(List.of(2000L, 2000L), row(session,
                "SELECT count(*), count(DISTINCT n) FROM TABLE(vgi_splits.main.split_cacheable(n => 2000, splits => 100))")
                .getFields());
    }
}
