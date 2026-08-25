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
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Proves {@code VgiSplitSource}/{@code VgiPageSourceProvider}'s dynamic-filter
 * wiring is not just harmless but CORRECT: a JOIN's build-side values must
 * reach the worker (as a real, applied filter, via a fixture that declares
 * {@code auto_apply_filters=true}) and reach EVERY split, not merely the
 * first one a {@code table_function_plan} page happens to return.
 *
 * <p>Modeled on the C++ VGI repo's own {@code splits/dynamic_filters.test}
 * (the exact scenario and even the "report the filter as data, since no
 * assertion about row counts alone can distinguish a working dynamic filter
 * from a silently dropped one" rationale come from there), adapted to a
 * hand-rolled in-process vgi-java fixture the same way
 * {@link VgiConnectorSplitParallelismTest} is: the reference fixture workers'
 * only split-capable, filter-pushdown-capable function
 * ({@code split_dynamic_filter}) is a callable table FUNCTION, and Trino's
 * {@code ConnectorTableFunction} SPI has no {@code Constraint}/{@code
 * DynamicFilter} hook at all (see {@code VgiSplitSource}'s own javadoc) — so
 * dynamic filtering can only be exercised, in Trino, against a plain
 * declarative table.
 */
final class VgiDynamicFilteringQueryRunnerTest {

    private static final int PORT_JOIN = 28478;
    private static final int PORT_PLAIN = 28479;

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

    /**
     * Split-capable {@code dyn_filter_fn(n, splits)}: emits {@code 0..n-1}
     * divided into {@code splits} ranges, each row stamped with a rendering of
     * whatever filter its OWN split's {@code init()} received. {@code
     * auto_apply_filters=true} means rows outside that filter never reach the
     * output at all — the fixture must actually apply it, not merely report it.
     */
    public static final class DynFilterSeqFunction implements TableFunction {

        static final Schema OUTPUT_SCHEMA = Schemas.of(
                Schemas.nullable("n", Schemas.INT64),
                Schemas.nullable("pushed_filters", Schemas.UTF8));

        @Override public String name() { return "dyn_filter_fn"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Split-capable dynamic-filter probe for Trino testing")
                    .withSplits()
                    .withPushdown(false, true, true);
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
            PushdownFilters decoded = params.pushdownFilters() == null || params.pushdownFilters().length == 0
                    ? PushdownFilters.empty()
                    : PushdownFiltersDecoder.decode(params.pushdownFilters(), params.joinKeys());
            return new SeqState(ranges, params.filters(), decoded.formatInline());
        }

        private static long argLong(TableBindParams params, String name) {
            Object v = params.arguments().named().get(name);
            return v instanceof Number num ? num.longValue() : 0L;
        }
    }

    /** Emits every row of its claimed ranges in one (filter-compacted) batch, then finishes. */
    public static final class SeqState extends TableProducerState {
        public List<Range> ranges = List.of();
        public FilterApplier filters;
        public String filterDescription = "(none)";
        public boolean emitted;

        public SeqState() {}

        SeqState(List<Range> ranges, FilterApplier filters, String filterDescription) {
            this.ranges = ranges;
            this.filters = filters;
            this.filterDescription = filterDescription;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            int total = 0;
            for (Range r : ranges) total += (int) (r.hi() - r.lo());
            if (total == 0) { out.finish(); return; }
            byte[] filterBytes = filterDescription.getBytes(StandardCharsets.UTF_8);
            BatchUtil.emitFiltered(DynFilterSeqFunction.OUTPUT_SCHEMA, total, filters, out, null, (root, rows, ignored) -> {
                BigIntVector nv = (BigIntVector) root.getVector("n");
                VarCharVector fv = (VarCharVector) root.getVector("pushed_filters");
                nv.allocateNew(rows);
                fv.allocateNew(rows);
                int idx = 0;
                for (Range r : ranges) {
                    for (long x = r.lo(); x < r.hi(); x++) {
                        nv.set(idx, x);
                        fv.set(idx, filterBytes);
                        idx++;
                    }
                }
            });
        }
    }

    private DistributedQueryRunner start(String catalogName, int port) throws Exception {
        DynFilterSeqFunction fn = new DynFilterSeqFunction();
        Worker worker = Worker.builder()
                .catalogName("dyntest")
                .defaultSchema("main")
                .registerTable(fn)
                .registerCatalogTable(CatalogTable.builder("main", "dyn_filter_table",
                                SchemaUtil.serializeSchema(DynFilterSeqFunction.OUTPUT_SCHEMA))
                        .scanFunction("dyn_filter_fn", null, Map.of("n", 300L, "splits", 30L))
                        .cardinality(300L, 300L)
                        .build());

        Thread serverThread = new Thread(() -> {
            try {
                worker.runTcp("127.0.0.1", port, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "vgi-dynfilter-test-worker-" + port);
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(500); // let the TCP listener bind before Trino connects

        Session session = TestingSession.testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("main")
                .build();
        DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(catalogName, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "tcp://127.0.0.1:" + port,
                "vgi.catalog-name", "dyntest",
                "vgi.connections", "3",
                // Force pagination across several table_function_plan calls, so
                // "every split sees the filter" is actually exercising more than
                // one plan-response page.
                "vgi.max-splits-per-response", "5",
                // Long enough that this in-process, single-worker join's tiny
                // VALUES build side has certainly resolved before the first
                // getNextBatch call — the whole point under test.
                "vgi.dynamic-filtering-wait-timeout-millis", "5000"));
        return runner;
    }

    @Test
    @Timeout(120)
    void joinBuildSideValuesReachAndPruneEverySplit() throws Exception {
        try (DistributedQueryRunner runner = start("vgi_dynjoin", PORT_JOIN)) {
            Session session = runner.getDefaultSession();

            // The join itself: correct rows only.
            MaterializedResult joined = runner.execute(session,
                    "SELECT s.n FROM dyn_filter_table s "
                            + "JOIN (VALUES (3), (17), (142), (299)) AS keys(k) ON s.n = keys.k "
                            + "ORDER BY s.n");
            assertEquals(List.of(3L, 17L, 142L, 299L),
                    joined.getMaterializedRows().stream().map(r -> (Long) r.getField(0)).toList(),
                    "the join must return exactly the matching rows, from whichever splits produced them");

            // Every row that survived (across every split, however many
            // table_function_plan pages it took to enumerate 30 splits 5 at a
            // time) must show the SAME real filter, not "(none)" — proving the
            // build-side values reached every split's init(), not just the
            // first plan page's.
            MaterializedResult distinctFilters = runner.execute(session,
                    "SELECT count(DISTINCT pushed_filters), min(pushed_filters) FROM dyn_filter_table s "
                            + "JOIN (VALUES (3), (17), (142), (299)) AS keys(k) ON s.n = keys.k");
            MaterializedRow row = distinctFilters.getMaterializedRows().get(0);
            assertEquals(1L, row.getField(0), "every surviving row must report the identical pushed-down filter");
            assertNotEquals("(none)", row.getField(1), "a real filter must have reached the worker");
        }
    }

    @Test
    @Timeout(120)
    void unjoinedScanReportsNoFilterAndReturnsEveryRow() throws Exception {
        try (DistributedQueryRunner runner = start("vgi_dynplain", PORT_PLAIN)) {
            Session session = runner.getDefaultSession();
            // The control: no join, no WHERE, no pushdown — proves the "(none)"
            // sentinel is a real report, not this fixture's answer for everything.
            MaterializedResult result = runner.execute(session,
                    "SELECT count(*), count(DISTINCT pushed_filters) FROM dyn_filter_table");
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(300L, row.getField(0));
            assertEquals(1L, row.getField(1));
        }
    }
}
