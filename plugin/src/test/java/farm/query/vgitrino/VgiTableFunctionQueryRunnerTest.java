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

/**
 * End-to-end proof that VGI's callable table functions — {@code sequence(n)},
 * {@code split_sequence(n, splits)} — are reachable from Trino's Polymorphic
 * Table Function syntax ({@code TABLE(catalog.schema.fn(args))}), against the
 * REAL Python fixture worker.
 *
 * <p>Complements {@link farm.query.vgitrino.VgiConnectorQueryRunnerTest} (which
 * only exercises declarative tables) and
 * {@link farm.query.vgitrino.VgiConnectorSplitParallelismTest} (which proves
 * split parallelism with a hand-rolled in-process fixture): this one proves
 * both — a plain table function AND a split-capable one — using the actual
 * reference implementation's own {@code sequence}/{@code split_sequence}
 * functions, reachable only once {@code ConnectorTableFunction} support
 * existed to call them at all.
 */
final class VgiTableFunctionQueryRunnerTest {

    @Test
    @Timeout(180)
    void callsTableFunctionsOnTheRealFixtureWorker() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        org.junit.jupiter.api.Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping live fixture-worker test");

        Session session = TestingSession.testSessionBuilder()
                .setCatalog("vgi_example")
                .setSchema("main")
                .build();

        DistributedQueryRunner runner = DistributedQueryRunner.builder(session)
                .setWorkerCount(1)
                .build();
        try {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                    "vgi.catalog-name", "example",
                    "vgi.connections", "3",
                    "vgi.max-splits-per-response", "2"));

            // A plain (not split-capable) table function. sequence()'s sole
            // positional argument is named "count" (see vgi-python's
            // SequenceFunctionArgs), not "n" — split_sequence() below is a
            // different function with its own "n"/"splits" named args.
            MaterializedResult plain = runner.execute(session,
                    "SELECT count(*), min(n), max(n) FROM TABLE(vgi_example.main.sequence(count => 10))");
            MaterializedRow plainRow = plain.getMaterializedRows().get(0);
            assertEquals(10L, plainRow.getField(0));
            assertEquals(0L, plainRow.getField(1));
            assertEquals(9L, plainRow.getField(2));

            // A split-capable table function: real multi-split parallelism
            // through the SAME ConnectorTableFunctionHandle path, paginated
            // 2-per-response against a real worker (not the in-process
            // hand-rolled fixture VgiConnectorSplitParallelismTest uses).
            MaterializedResult split = runner.execute(session,
                    "SELECT count(*), count(distinct n), sum(n) "
                            + "FROM TABLE(vgi_example.main.split_sequence(n => 200, splits => 12))");
            MaterializedRow splitRow = split.getMaterializedRows().get(0);
            assertEquals(200L, splitRow.getField(0), "every row from all 12 splits");
            assertEquals(200L, splitRow.getField(1), "no split may double-count or drop a row");
            assertEquals(19900L, splitRow.getField(2), "sum(0..199) = 19900");
        } finally {
            runner.close();
        }
    }
}
