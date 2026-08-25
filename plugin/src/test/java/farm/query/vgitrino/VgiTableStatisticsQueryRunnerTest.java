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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves {@code VgiMetadata#getTableStatistics}'s per-column statistics
 * ({@code catalog_table_column_statistics_get}, decoded via {@code
 * ColumnStatisticsDecoder}) against a real worker's real advertised
 * statistics — the reference fixture worker's {@code data.numbers} table
 * (100 integers, {@code 0..99}) carries genuine DuckDB-extracted column
 * statistics on its {@code value} column, precisely so this isn't testing a
 * hand-rolled fixture's made-up numbers.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiTableStatisticsQueryRunnerTest {

    private static final String CATALOG = "vgi_example";

    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping live fixture-worker test");

        session = TestingSession.testSessionBuilder().setCatalog(CATALOG).setSchema("data").build();
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

    private Optional<MaterializedRow> statsRow(String columnName) {
        MaterializedResult stats = runner.execute(session, "SHOW STATS FOR numbers");
        return stats.getMaterializedRows().stream()
                .filter(r -> columnName.equals(r.getField(0)))
                .findFirst();
    }

    @Test
    @Timeout(60)
    void columnStatisticsMatchTheWorkersRealDuckDbExtractedValues() {
        MaterializedRow row = statsRow("value").orElseThrow(() -> new AssertionError("no stats row for 'value'"));
        // SHOW STATS FOR column order: column_name, data_size,
        // distinct_values_count, nulls_fraction, row_count, low_value, high_value.
        Double distinctValuesCount = (Double) row.getField(2);
        Double nullsFraction = (Double) row.getField(3);
        Object lowValue = row.getField(5);
        Object highValue = row.getField(6);

        assertEquals(0.0, nullsFraction, "range(100) produces no nulls");
        assertEquals("0", String.valueOf(lowValue));
        assertEquals("99", String.valueOf(highValue));
        assertTrue(distinctValuesCount != null && distinctValuesCount >= 90.0 && distinctValuesCount <= 100.0,
                "100 genuinely distinct integers should report a distinct count near 100, got "
                        + distinctValuesCount);
    }

    @Test
    @Timeout(60)
    void tableRowCountRowIsAlsoPresent() {
        // A DIFFERENT table (data.numbers itself doesn't advertise
        // cardinality_estimate — its row count comes only implicitly from its
        // column statistics' distinct-value counts, not this separate field)
        // — cardinality_inlined_table declares cardinality_estimate=10000
        // explicitly, proving the pre-existing row-count-only behavior wasn't
        // regressed by adding column statistics alongside it.
        MaterializedResult stats = runner.execute(session, "SHOW STATS FOR cardinality_inlined_table");
        MaterializedRow summary = stats.getMaterializedRows().stream()
                .filter(r -> r.getField(0) == null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no table-level summary row"));
        assertEquals(10000.0, (Double) summary.getField(4));
    }

    @Test
    @Timeout(60)
    void aTableWithNoAdvertisedStatisticsStillWorks() {
        // sequence_view or any other table without a `statistics=` block must
        // not throw just because getTableStatistics now makes an extra RPC —
        // catalog_table_column_statistics_get answers "none" (empty bytes)
        // rather than erroring, and this must degrade to row-count-only (or
        // fully empty) exactly as it did before column statistics existed.
        MaterializedResult result = runner.execute(session, "SELECT count(*) FROM rowid_first");
        assertEquals(1, result.getRowCount());
    }
}
