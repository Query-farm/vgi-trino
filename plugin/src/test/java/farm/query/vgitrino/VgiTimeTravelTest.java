// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
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
 * Proves {@code FOR VERSION/TIMESTAMP AS OF} end to end against the real reference
 * fixture worker's {@code data.versioned_data} table (the exact fixture already
 * used by every other test in this suite — no new fixture built for this) — a
 * table whose SCHEMA itself evolves across versions (1: {@code {id}}; 2: {@code
 * {id, name, score, active}}; 3/current: {@code {id, score}}), transcribed from
 * {@code vgi-python/vgi/_test_fixtures/table/versioned.py}, not invented.
 *
 * <p>See {@code VgiTimeTravel}'s javadoc for the {@code ConnectorTableVersion} →
 * VGI {@code at_unit}/{@code at_value} conversion this exercises.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiTimeTravelTest {

    private static final String CATALOG = "vgi_example";

    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping time travel test");

        session = TestingSession.testSessionBuilder().setCatalog(CATALOG).setSchema("data").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", "example"));
    }

    @AfterAll
    void stop() {
        if (runner != null) runner.close();
    }

    private MaterializedResult query(String sql) {
        return runner.execute(session, sql);
    }

    @Test
    @Timeout(60)
    void versionOneHasTheOriginalSingleColumnSchema() {
        MaterializedResult result = query(
                "SELECT * FROM versioned_data FOR VERSION AS OF 1 ORDER BY id");
        assertEquals(List.of("id"), columnNames(result));
        assertEquals(List.of(List.of(1L), List.of(2L), List.of(3L)), rows(result));
    }

    @Test
    @Timeout(60)
    void versionTwoAddedThreeColumnsAndTwoRows() {
        MaterializedResult result = query(
                "SELECT * FROM versioned_data FOR VERSION AS OF 2 ORDER BY id");
        assertEquals(List.of("id", "name", "score", "active"), columnNames(result));
        assertEquals(5, result.getRowCount());
        assertEquals(List.of(1L, "alice", 10.0, true), result.getMaterializedRows().get(0).getFields());
    }

    @Test
    @Timeout(60)
    void versionThreeDroppedTwoColumnsFromVersionTwo() {
        MaterializedResult result = query(
                "SELECT * FROM versioned_data FOR VERSION AS OF 3 ORDER BY id");
        assertEquals(List.of("id", "score"), columnNames(result));
        assertEquals(List.of(15.0, 25.0, 35.0, 45.0),
                result.getMaterializedRows().stream().map(r -> r.getField(1)).toList());
    }

    @Test
    @Timeout(60)
    void noAsOfClauseMatchesCurrentVersionThree() {
        // The regression check: a plain read (this connector's existing, already-tested behavior)
        // must be completely unaffected by this feature landing.
        MaterializedResult result = query("SELECT * FROM versioned_data ORDER BY id");
        assertEquals(List.of("id", "score"), columnNames(result));
        assertEquals(4, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void timestampBeforeTwoThousandTwentyOneResolvesToVersionOne() {
        MaterializedResult result = query(
                "SELECT * FROM versioned_data FOR TIMESTAMP AS OF TIMESTAMP '2020-06-15 00:00:00' ORDER BY id");
        assertEquals(List.of("id"), columnNames(result));
        assertEquals(3, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void timestampInTwentyTwentyTwoResolvesToVersionThree() {
        MaterializedResult result = query(
                "SELECT * FROM versioned_data FOR TIMESTAMP AS OF TIMESTAMP '2022-01-01 00:00:00' ORDER BY id");
        assertEquals(List.of("id", "score"), columnNames(result));
        assertEquals(4, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void unknownVersionFailsCleanlyNotAHangOrCrash() {
        assertThrows(QueryFailedException.class,
                () -> query("SELECT * FROM versioned_data FOR VERSION AS OF 99"));
    }

    private static List<String> columnNames(MaterializedResult result) {
        return result.getColumnNames();
    }

    private static List<List<Object>> rows(MaterializedResult result) {
        return result.getMaterializedRows().stream().map(MaterializedRow::getFields).toList();
    }
}
