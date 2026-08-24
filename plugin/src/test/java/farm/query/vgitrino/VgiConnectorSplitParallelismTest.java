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
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves REAL multi-split parallel redemption through Trino, not just the
 * not-split-capable fallback path {@link VgiConnectorQueryRunnerTest} exercises.
 *
 * <p>The reference fixture workers (both vgi-python's and vgi-java's own
 * {@code vgi-example-worker}) don't expose any split-capable function as a
 * plain declarative catalog table — the split fixtures are callable table
 * FUNCTIONS only, and this connector's v1 doesn't implement Trino's
 * {@code ConnectorTableFunction} SPI (Phase 8). So this test builds its own
 * small in-process vgi-java {@link Worker} — a split-capable function wired
 * into a {@link CatalogTable} — served over {@code tcp://} rather than
 * modifying either shared fixture worker (which back cross-SDK conformance
 * counts this connector shouldn't perturb).
 */
final class VgiConnectorSplitParallelismTest {

    private static final int PORT = 28477;

    /** {@code [lo, hi)} owned by one split. */
    private record Range(long lo, long hi) {}

    private static byte[] encode(Range r) {
        return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(r.lo()).putLong(r.hi()).array();
    }

    private static Range decode(byte[] payload) {
        ByteBuffer b = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        return new Range(b.getLong(), b.getLong());
    }

    private static List<Range> evenRanges(long n, long k) {
        List<Range> out = new ArrayList<>();
        long base = n / k;
        long extra = n % k;
        long lo = 0;
        for (long i = 0; i < k; i++) {
            long hi = lo + base + (i < extra ? 1 : 0);
            out.add(new Range(lo, hi));
            lo = hi;
        }
        return out;
    }

    /** Split-capable {@code split_seq(n, splits)}: emits {@code 0..n-1} divided into {@code splits} ranges. */
    public static final class SplitSeqFunction implements TableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

        @Override public String name() { return "split_seq_fn"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Split-capable 0..n-1 for real multi-split Trino testing").withSplits();
        }

        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.named("n", Schemas.INT64, "0"),
                    ArgSpec.named("splits", Schemas.INT64, "1"));
        }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT_SCHEMA));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long n = argLong(params, "n");
            long splits = argLong(params, "splits");
            List<ScanSplit> out = new ArrayList<>();
            for (Range r : evenRanges(n, splits)) {
                long rows = r.hi() - r.lo();
                out.add(new ScanSplit(encode(r), new byte[0], rows, true, rows * 8,
                        null, null, null, null, null));
            }
            return PlanResult.of(out);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = params.splitPayloads();
            List<Range> ranges = new ArrayList<>();
            if (payloads != null) for (byte[] p : payloads) ranges.add(decode(p));
            return new SeqState(ranges);
        }

        private static long argLong(TableBindParams params, String name) {
            Object v = params.arguments().named().get(name);
            return v instanceof Number num ? num.longValue() : 0L;
        }
    }

    /** Emits every row of its claimed ranges in one batch, then finishes. */
    public static final class SeqState extends TableProducerState {
        public List<Range> ranges = List.of();
        public boolean emitted;

        public SeqState() {}

        SeqState(List<Range> ranges) { this.ranges = ranges; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            int total = 0;
            for (Range r : ranges) total += (int) (r.hi() - r.lo());
            if (total == 0) { out.finish(); return; }
            BatchUtil.emit(SplitSeqFunction.OUTPUT_SCHEMA, total, out, (root, rows, ignored) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                v.allocateNew(rows);
                int idx = 0;
                for (Range r : ranges) for (long x = r.lo(); x < r.hi(); x++) v.set(idx++, x);
            });
        }
    }

    @Test
    @Timeout(120)
    void redeemsRealMultiSplitScanThroughTrino() throws Exception {
        SplitSeqFunction fn = new SplitSeqFunction();
        Worker worker = Worker.builder()
                .catalogName("splittest")
                .defaultSchema("main")
                .registerTable(fn)
                .registerCatalogTable(CatalogTable.builder("main", "split_seq", SchemaUtil.serializeSchema(SplitSeqFunction.OUTPUT_SCHEMA))
                        .scanFunction("split_seq_fn", null, Map.of("n", 97L, "splits", 5L))
                        .build());

        Thread serverThread = new Thread(() -> {
            try {
                worker.runTcp("127.0.0.1", PORT, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "vgi-split-test-worker");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(500); // let the TCP listener bind before Trino connects

        Session session = TestingSession.testSessionBuilder()
                .setCatalog("vgi_split")
                .setSchema("main")
                .build();
        DistributedQueryRunner runner = DistributedQueryRunner.builder(session)
                .setWorkerCount(1)
                .build();
        try {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_split", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "tcp://127.0.0.1:" + PORT,
                    "vgi.catalog-name", "splittest",
                    "vgi.connections", "3",
                    // Force multiple splits per response so pagination is
                    // exercised too, not just single-shot planning.
                    "vgi.max-splits-per-response", "2"));

            MaterializedResult result = runner.execute(session,
                    "SELECT count(*), min(n), max(n), count(distinct n) FROM split_seq");
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(97L, row.getField(0), "5 splits of [0,97) must together produce all 97 rows");
            assertEquals(0L, row.getField(1));
            assertEquals(96L, row.getField(2));
            assertEquals(97L, row.getField(3), "no split may double-count or drop a row");
        } finally {
            runner.close();
        }
    }
}
