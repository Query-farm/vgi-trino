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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Real, end-to-end coverage of {@code VgiTableInOutFunctions}' literal-call
 * support against a live {@code vgi-fixture-worker}: {@code blended_drop}
 * (the 1-&gt;0 cardinality), {@code blended_explode} (1-&gt;0/1-&gt;1/1-&gt;N, all
 * from the same registration, exercised via its one positional argument's
 * value), and confirming that {@code geo_encode} — deliberately overloaded
 * (2-arg and 3-arg registrations sharing one name) in the reference fixture —
 * is correctly skipped at discovery rather than mis-registered, exactly like
 * {@code VgiTableFunctions}' own established overload-collision handling.
 */
final class VgiTableInOutFunctionsTest {

    private static DistributedQueryRunner runner;
    private static Session session;

    @BeforeAll
    static void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping table-in-out function tests");

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
    void blendedDropLiteralCallEmitsZeroRows() {
        // BlendedDropFunction always answers with a real 0-row batch for its one
        // synthesized input row — the exchange must produce Processed(0 rows) then
        // Finished, never mistake the empty answer for end-of-stream.
        MaterializedResult result = query("SELECT * FROM TABLE(vgi_example.main.blended_drop(42.0))");
        assertEquals(0, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void blendedExplodeLiteralCallFansOutToZeroRows() {
        MaterializedResult result = query("SELECT * FROM TABLE(vgi_example.main.blended_explode(0))");
        assertEquals(0, result.getRowCount());
    }

    @Test
    @Timeout(60)
    void blendedExplodeLiteralCallFansOutToOneRow() {
        MaterializedResult result = query("SELECT * FROM TABLE(vgi_example.main.blended_explode(1))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(0L, rows.get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void blendedExplodeLiteralCallFansOutToManyRows() {
        // n=5 -> exactly one exchange turn's output batch holding 5 rows (0..4) —
        // real multi-row fan-out from a single literal call, not five separate turns.
        MaterializedResult result = query(
                "SELECT i FROM TABLE(vgi_example.main.blended_explode(5)) ORDER BY i");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(5, rows.size());
        for (int i = 0; i < 5; i++) {
            assertEquals((long) i, rows.get(i).getField(0));
        }
    }

    @Test
    @Timeout(60)
    void blendedExplodeLiteralCallCanBeUsedTwiceInOneQuery() {
        // Two independent literal call sites, each its own bind + split + exchange.
        MaterializedResult result = query(
                "SELECT (SELECT count(*) FROM TABLE(vgi_example.main.blended_explode(2))) + "
                        + "(SELECT count(*) FROM TABLE(vgi_example.main.blended_explode(3)))");
        assertEquals(5L, result.getMaterializedRows().get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void geoEncodeIsSkippedAtDiscoveryBecauseItIsOverloadedByArity() {
        // GeoEncodeFunction (2 positional args) and GeoEncode3Function (3 positional
        // args) both register under the SAME name "geo_encode" in the reference
        // fixture worker — VGI resolves this by arity, but Trino's ConnectorTableFunction
        // model requires exactly one registration per name (see VgiTableInOutFunctions'
        // own javadoc), so neither overload should be callable through this connector —
        // the call must fail with "function not registered", not silently pick one arity.
        assertThrows(RuntimeException.class,
                () -> query("SELECT * FROM TABLE(vgi_example.main.geo_encode(52.0, 13.0))"));
    }
}
