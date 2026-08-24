// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof against the REAL reference fixture worker
 * (uv run --project ~/Development/vgi-python vgi-fixture-worker): a live
 * {@link DistributedQueryRunner} with {@link VgiPlugin} installed, a catalog
 * attached over subprocess transport exactly the way a real deployment would
 * configure one, and a query run through the full metadata → bind →
 * table_function_plan → init → drain → Arrow-to-Trino-Block pipeline.
 *
 * <p>{@code data.numbers} is an explicit-columns table (not function-backed),
 * so this also exercises {@code catalog_table_scan_function_get} — and, since
 * nothing in the fixture worker's declarative catalog opts into splits, the
 * not-split-capable empty-token sentinel path in
 * {@link farm.query.vgitrino.split.VgiSplitSource}/{@link farm.query.vgitrino.page.VgiPageSource}.
 *
 * <p>Skipped (not failed) when {@code ~/Development/vgi-python} isn't present
 * — this test needs the sibling repo checked out, same as vgi-java's own
 * integration suite.
 */
final class VgiConnectorQueryRunnerTest {

    @Test
    @Timeout(180)
    void queriesTheExampleFixtureWorkerEndToEnd() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        org.junit.jupiter.api.Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping live fixture-worker test");

        Session session = TestingSession.testSessionBuilder()
                .setCatalog("vgi_example")
                .setSchema("data")
                .build();

        DistributedQueryRunner runner = DistributedQueryRunner.builder(session)
                .setWorkerCount(1)
                .build();
        try {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                    "vgi.catalog-name", "example",
                    "vgi.connections", "2"));

            MaterializedResult result = runner.execute(session,
                    "SELECT count(*), min(value), max(value) FROM data.numbers");
            assertEquals(1, result.getRowCount());
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(100L, row.getField(0), "data.numbers is documented as the first 100 integers");
            long min = ((Number) row.getField(1)).longValue();
            long max = ((Number) row.getField(2)).longValue();
            assertEquals(99, max - min, "100 consecutive integers span 99");

            // A second, independent query on the same catalog proves the
            // connection pool's borrow/release cycle leaves it reusable.
            MaterializedResult tables = runner.execute(session, "SHOW TABLES FROM data");
            assertTrue(tables.getRowCount() > 0, "the data schema must list at least one table");
        } finally {
            runner.close();
        }
    }
}
