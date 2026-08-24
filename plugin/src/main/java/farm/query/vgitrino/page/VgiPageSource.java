// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.page;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import farm.query.vgitrino.split.VgiSplit;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;

/**
 * Redeems one {@link VgiSplit} via {@code init()} and drains its producer
 * stream into Trino {@link Page}s.
 *
 * <p>Holds one pooled connection for its entire lifetime (unlike
 * {@link farm.query.vgitrino.metadata.VgiMetadata}/{@link farm.query.vgitrino.split.VgiSplitManager},
 * which borrow-and-return per call) — a producer stream is stateful and
 * lockstep, so it cannot hop between connections mid-drain.
 *
 * <p>v1 reads every column of the split's bound output schema and lets
 * Trino's own engine project down to what the query actually asked for,
 * rather than pushing {@code projection_ids} into {@code init()} — see the
 * plan's Phase 4. {@code getNextSourcePage} blocks synchronously on the
 * underlying RPC tick, same trade-off plenty of blocking-I/O connectors make;
 * async prefetch is a documented follow-up, not a v1 blocker.
 */
public final class VgiPageSource implements ConnectorPageSource {

    private final VgiWorkerClient client;
    private final VgiWorkerClient.Attached connection;
    private final ClientStreamSession<?> session;
    private final Schema arrowSchema;
    private final List<VgiColumnHandle> columns;

    private long completedBytes;
    private long completedPositions;
    private boolean finished;
    private boolean connectionHealthy = true;

    /**
     * @param client the connection pool to borrow from for this split's lifetime
     * @param split the split to redeem
     * @param columns the columns this page source must emit, in requested order
     * @param tableOutputSchema the owning table's full (bind-time) Arrow schema bytes
     */
    public VgiPageSource(VgiWorkerClient client, VgiSplit split, List<VgiColumnHandle> columns,
            byte[] tableOutputSchema) {
        this.client = client;
        this.columns = columns;
        this.connection = client.borrow();
        boolean ok = false;
        try {
            InitRequest initRequest = new InitRequest(
                    split.bindCall(),
                    tableOutputSchema,
                    split.bindOpaqueData(),
                    null,           // projection_ids — Phase 4
                    null,           // pushdown_filters — Phase 4
                    null,           // join_keys
                    null,           // phase (producer mode)
                    null,           // execution_id — primary init, worker mints one
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id
                    split.token().length == 0 ? null : List.of(split.token()),
                    null);          // row_limit
            RpcStream<? extends StreamState> stream = connection.service().init(initRequest, null);
            this.session = (ClientStreamSession<?>) stream;
            this.arrowSchema = ArrowSchemaCodec.deserializeSchema(tableOutputSchema);
            ok = true;
        } finally {
            if (!ok) client.release(connection, false);
        }
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos() {
        return 0; // not tracked in v1
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) return null;
        AnnotatedBatch batch;
        try {
            batch = session.tick();
        } catch (NoSuchElementException endOfStream) {
            finished = true;
            return null;
        } catch (RuntimeException e) {
            connectionHealthy = false;
            throw e;
        }
        VectorSchemaRoot root = batch.root();
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            VgiColumnHandle column = columns.get(i);
            Field field = arrowSchema.getFields().get(column.ordinal());
            FieldVector vector = root.getVector(field.getName());
            Type type = VgiTypeMapping.toTrinoType(field);
            blocks[i] = VgiTypeMapping.toBlock(type, vector, rowCount);
        }
        Page page = new Page(rowCount, blocks);
        completedBytes += page.getSizeInBytes();
        completedPositions += rowCount;
        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        try {
            session.close();
        } catch (Exception e) {
            connectionHealthy = false;
        } finally {
            client.release(connection, connectionHealthy);
        }
    }
}
