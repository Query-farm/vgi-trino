// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.table.PlanRequest;
import farm.query.vgi.table.PlanResult;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.TestingSession;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ports the property {@code splits/plan_bounds.test} asserts (a worker that
 * paginates forever hits a client-side page cap and throws, never silently
 * truncating) against a hand-rolled in-process fixture — real reference
 * worker functions all terminate their own pagination legitimately, so
 * exercising a genuinely runaway one needs a fixture built for the purpose,
 * the same reasoning {@link VgiConnectorSplitParallelismTest} gives for its
 * own hand-rolled split function.
 */
final class VgiPlanPageCapTest {

    private static final int PORT = 28480;

    /** Emits one split and a fresh cursor every call — pagination that never ends. */
    public static final class EndlessCursorFunction implements TableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

        @Override public String name() { return "endless_cursor_fn"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Split-capable function that never stops cursoring").withSplits();
        }

        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long page = request.cursor() == null || request.cursor().length == 0
                    ? 0 : ByteBuffer.wrap(request.cursor()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            ScanSplit split = new ScanSplit(
                    ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(page).array(),
                    new byte[0], 1L, true, 8L, null, null, null, null, null);
            return PlanResult.of(List.of(split)).withNextCursor(
                    ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(page + 1).array());
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            return new OneRowState();
        }
    }

    /** Emits a single row, then finishes — the split's own content doesn't matter for this test. */
    public static final class OneRowState extends TableProducerState {
        public boolean emitted;

        public OneRowState() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            BatchUtil.emit(EndlessCursorFunction.OUTPUT_SCHEMA, 1, out, (root, rows, ignored) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                v.allocateNew(rows);
                v.set(0, 0L);
            });
        }
    }

    @Test
    @Timeout(60)
    void runawayPaginationThrowsRatherThanHangingOrTruncating() throws Exception {
        Worker worker = Worker.builder()
                .catalogName("plancaptest")
                .defaultSchema("main")
                .registerTable(new EndlessCursorFunction())
                .registerCatalogTable(CatalogTable.builder("main", "endless_cursor",
                                SchemaUtil.serializeSchema(EndlessCursorFunction.OUTPUT_SCHEMA))
                        .scanFunction("endless_cursor_fn", null, Map.of())
                        .build());

        Thread serverThread = new Thread(() -> {
            try {
                worker.runTcp("127.0.0.1", PORT, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "vgi-plan-cap-test-worker");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(500);

        Session session = TestingSession.testSessionBuilder().setCatalog("vgi_plan_cap").setSchema("main").build();
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_plan_cap", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "tcp://127.0.0.1:" + PORT,
                    "vgi.catalog-name", "plancaptest",
                    "vgi.connections", "2",
                    // Force every page to hold exactly one split, so a small
                    // page cap is reached in a handful of round trips rather
                    // than needing the (also small, but still 1024-call)
                    // default to prove the same point slowly.
                    "vgi.max-splits-per-response", "1",
                    "vgi.max-plan-pages", "4"));

            QueryFailedException e = assertThrows(QueryFailedException.class,
                    () -> runner.execute(session, "SELECT count(*) FROM endless_cursor"));
            assertTrue(e.getMessage().contains("exceeded the scan-planning page cap"),
                    "must name what was hit, not just fail generically: " + e.getMessage());
            assertTrue(e.getMessage().contains("4 pages"), "must name the configured cap: " + e.getMessage());

            // The catalog survives it: hitting the cap is a bounded, recoverable
            // failure, not a poisoned connection or a wedged pool — the same
            // query fails the SAME clean way again, promptly, rather than
            // hanging or throwing something unrelated (a stuck borrow(),
            // corrupted connection state, etc. would surface here).
            QueryFailedException again = assertThrows(QueryFailedException.class,
                    () -> runner.execute(session, "SELECT count(*) FROM endless_cursor"));
            assertTrue(again.getMessage().contains("exceeded the scan-planning page cap"),
                    "repeat failure must be the same clean one, not pool corruption: " + again.getMessage());
        }
    }
}
