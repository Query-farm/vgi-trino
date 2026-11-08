// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol 3.2 {@code TableInfo.scan_planning = "filtered"} tables against the
 * reference fixture worker's {@code data.planned_days}. Such a table's scan is
 * resolved through {@code catalog_table_scan_branches_get}, which this
 * connector doesn't implement, so the table must still list and describe from
 * its declared columns while a scan fails with a clear NOT_SUPPORTED error
 * rather than an opaque worker-side "unknown table" from
 * {@code catalog_table_scan_function_get}.
 */
final class VgiFilteredScanPlanningTest {

    @Test
    @Timeout(180)
    void filteredTableDescribesButRefusesToScan() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping live fixture-worker test");

        Session session = TestingSession.testSessionBuilder()
                .setCatalog("vgi_example")
                .setSchema("data")
                .build();

        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session)
                .setWorkerCount(1)
                .build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                    "vgi.catalog-name", "example",
                    "vgi.connections", "2"));

            MaterializedResult described = runner.execute(session, "DESCRIBE data.planned_days");
            List<String> columns = described.getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .toList();
            assertEquals(List.of("event_date", "n"), columns);

            RuntimeException failure = assertThrows(RuntimeException.class, () ->
                    runner.execute(session, "SELECT count(*) FROM data.planned_days"));
            assertTrue(String.valueOf(failure.getMessage()).contains("scan_planning='filtered'"),
                    "expected the connector's filtered-scan refusal, got: " + failure.getMessage());

            // The catalog stays usable for ordinary bind-time tables afterwards.
            MaterializedRow row = runner.execute(session, "SELECT count(*) FROM data.numbers")
                    .getMaterializedRows().get(0);
            assertEquals(100L, row.getField(0));
        }
    }
}
