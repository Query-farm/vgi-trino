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
 * {@code vgi-fixture-worker}: {@code echo} (pure passthrough, no extra args),
 * {@code repeat_inputs} (a scalar arg BEFORE the {@code TableInput} arg, plus
 * real multi-batch input large enough to force more than one {@code
 * exchange()} turn), {@code filter_by_setting} (a {@code required_settings}
 * value read from a Trino session property), and the two real
 * finalize-phase fixtures — {@code substream_partial_sum} (single-batch
 * finish) and {@code multi_batch_finish} (finish emitting MANY separate
 * batches, each its own {@code tick()} round trip).
 *
 * <p>{@code secret_in_out} is NOT covered: its {@code on_bind} resolves a
 * secret fully dynamically, with no static {@code Secret()}/{@code
 * Meta.required_secrets} declaration — so {@code FunctionInfo.required_secrets}
 * is genuinely empty for it, and this connector's settings/secrets support
 * (deliberately reusing the exact same {@code required_secrets}-gated
 * resolution {@code VgiScalarFunctions} already uses) never forwards a
 * credential a function didn't statically declare needing — see {@code
 * VgiScalarFunctions.BindCache#resolveSecretFields}'s own javadoc for why
 * that gate is deliberate, not an oversight. {@code sum_all_columns}/{@code
 * sum_all_columns_simple_distributed} are {@code TableBufferingFunction}s (a
 * third, distinct VGI kind with its own {@code TABLE_BUFFERING}/{@code
 * TABLE_BUFFERING_FINALIZE} phases, not this kind's {@code INPUT}/{@code
 * FINALIZE}) — out of scope here, covered instead by {@code
 * VgiTableBufferingFunctionsTest} against {@link
 * farm.query.vgitrino.function.VgiTableBufferingFunctions}/{@link
 * farm.query.vgitrino.function.VgiTableBufferingDataProcessor}.
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

    @Test
    @Timeout(60)
    void filterBySettingReadsAStringSessionProperty() {
        Session withThreshold = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "threshold", "5")
                .build();
        MaterializedResult result = runner.execute(withThreshold,
                "SELECT value FROM TABLE(vgi_example.main.filter_by_setting("
                        + "TABLE(VALUES (3), (7), (5), (1)) T(value))) ORDER BY value");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(2, rows.size());
        assertEquals(5, rows.get(0).getField(0));
        assertEquals(7, rows.get(1).getField(0));
    }

    @Test
    @Timeout(60)
    void substreamPartialSumDrivesASingleBatchFinalizeTurn() {
        // No PARTITION BY -> exactly one Trino partition -> one VgiTableInOutDataProcessor
        // instance -> its "per-substream partial" IS the whole answer (see the real fixture's own
        // javadoc on why an outer SUM() isn't needed here the way DuckDB's real multi-substream
        // fan-out needs one).
        MaterializedResult result = query(
                "SELECT n FROM TABLE(vgi_example.main.substream_partial_sum("
                        + "TABLE(SELECT * FROM UNNEST(SEQUENCE(1, 100)) t(n))))");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(1, rows.size());
        assertEquals(5050L, rows.get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void multiBatchFinishDrainsSeveralSeparateFinalizeBatches() {
        // multi_batch_finish emits ONE finalize batch PER INPUT ROW (the first carrying the real
        // total, the rest carrying 0) specifically to catch a broken multi-batch continuation:
        // COUNT(*) proves no batch was lost/repeated, SUM(n) proves no batch's content was lost/
        // duplicated/reordered — see the real fixture's own javadoc.
        MaterializedResult result = query(
                "SELECT count(*), sum(n) FROM TABLE(vgi_example.main.multi_batch_finish("
                        + "TABLE(SELECT * FROM UNNEST(SEQUENCE(1, 50)) t(n))))");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(50L, row.getField(0));
        assertEquals(1275L, row.getField(1));
    }
}
