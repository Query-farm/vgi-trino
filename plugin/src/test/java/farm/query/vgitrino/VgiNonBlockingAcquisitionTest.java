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
 * Proves connection acquisition for split redemption is genuinely
 * non-blocking, not merely bounded: {@code vgi.connection-acquire-timeout-millis}
 * is set to an absurdly small value (1ms) alongside far more splits than
 * {@code vgi.connections} allows concurrently. Before {@code VgiPageSource}/
 * {@code VgiTableFunctionSplitProcessor} moved off {@link
 * farm.query.vgitrino.client.VgiWorkerClient#borrow} onto {@link
 * farm.query.vgitrino.client.VgiWorkerClient#borrowAsync}, redemption's own
 * connection wait was subject to that same bounded timeout — with only 1ms
 * of patience and 100 splits contending for 2 connections, ordinary queuing
 * delay alone (each split takes real, if brief, time to drain before its
 * connection frees up) would make this scan fail intermittently or outright,
 * not just under adversarial timing. Passing reliably here is the actual
 * proof that waiting a turn no longer costs any of {@code
 * connection-acquire-timeout-millis}'s budget at all.
 */
final class VgiNonBlockingAcquisitionTest {

    private static final int PORT = 28481;

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

    /** Split-capable {@code contended_seq(n, splits)}: 0..n-1, divided into {@code splits} ranges. */
    public static final class ContendedSeqFunction implements TableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("n", Schemas.INT64));

        @Override public String name() { return "contended_seq_fn"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Split-capable, deliberately heavily contended").withSplits();
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
            BatchUtil.emit(ContendedSeqFunction.OUTPUT_SCHEMA, total, out, (root, rows, ignored) -> {
                BigIntVector v = (BigIntVector) root.getVector("n");
                v.allocateNew(rows);
                int idx = 0;
                for (Range r : ranges) for (long x = r.lo(); x < r.hi(); x++) v.set(idx++, x);
            });
        }
    }

    @Test
    @Timeout(120)
    void hundredSplitsOverTwoConnectionsSucceedDespiteA1MsAcquireTimeout() throws Exception {
        ContendedSeqFunction fn = new ContendedSeqFunction();
        Worker worker = Worker.builder()
                .catalogName("nonblockingtest")
                .defaultSchema("main")
                .registerTable(fn)
                .registerCatalogTable(CatalogTable.builder("main", "contended_seq",
                                SchemaUtil.serializeSchema(ContendedSeqFunction.OUTPUT_SCHEMA))
                        .scanFunction("contended_seq_fn", null, Map.of("n", 10000L, "splits", 100L))
                        .build());

        Thread serverThread = new Thread(() -> {
            try {
                worker.runTcp("127.0.0.1", PORT, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "vgi-nonblocking-test-worker");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(500);

        Session session = TestingSession.testSessionBuilder().setCatalog("vgi_nonblocking").setSchema("main").build();
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_nonblocking", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "tcp://127.0.0.1:" + PORT,
                    "vgi.catalog-name", "nonblockingtest",
                    // Only 2 real connections for 100 splits — heavy, deliberate
                    // oversubscription — combined with an all-but-zero acquire
                    // timeout: a split waiting its turn via the OLD borrow()-based
                    // path would fail this almost immediately.
                    "vgi.connections", "2",
                    "vgi.connection-acquire-timeout-millis", "1",
                    "vgi.max-splits-per-response", "10"));

            long sum = (long) runner.execute(session, "SELECT count(*), sum(n) FROM contended_seq")
                    .getMaterializedRows().get(0).getField(1);
            assertEquals(49_995_000L, sum, "0+1+...+9999 — every one of the 100 splits must contribute intact");
        }
    }
}
