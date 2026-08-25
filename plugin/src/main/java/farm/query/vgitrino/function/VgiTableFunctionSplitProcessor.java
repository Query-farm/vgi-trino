// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.split.VgiSplit;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Redeems one {@link VgiSplit} of a table-function call via {@code init()}
 * and drains it into Trino {@link Page}s.
 *
 * <p>The table-function analogue of {@link farm.query.vgitrino.page.VgiPageSource}
 * — same redemption/drain mechanics, wrapped in the pull-based
 * {@link TableFunctionSplitProcessor#process()} shape (no columns list: a
 * table function has no separate projection step at this layer, so every
 * column of the bound output schema is always emitted).
 */
public final class VgiTableFunctionSplitProcessor implements TableFunctionSplitProcessor {

    private final VgiWorkerClient client;
    private final VgiWorkerClient.Attached connection;
    private final ClientStreamSession<?> session;
    private final Schema arrowSchema;

    private boolean finished;
    private boolean connectionHealthy = true;

    /**
     * @param client the pool to borrow a connection from for this split's lifetime
     * @param split the split to redeem
     * @param outputSchema the call's bound (IPC-encoded) output schema
     */
    public VgiTableFunctionSplitProcessor(VgiWorkerClient client, VgiSplit split, byte[] outputSchema) {
        this.client = client;
        this.connection = client.borrow();
        boolean ok = false;
        try {
            InitRequest initRequest = new InitRequest(
                    split.bindCall(),
                    outputSchema,
                    split.bindOpaqueData(),
                    null,           // projection_ids — no projection step for table functions yet
                    null,           // pushdown_filters
                    null,           // join_keys
                    null,           // phase (producer mode)
                    null,           // execution_id
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id
                    split.token().length == 0 ? null : List.of(split.token()),
                    null);          // row_limit
            RpcStream<? extends StreamState> stream = connection.service().init(initRequest, null);
            this.session = (ClientStreamSession<?>) stream;
            this.arrowSchema = ArrowSchemaCodec.deserializeSchema(outputSchema);
            ok = true;
        } finally {
            if (!ok) client.release(connection, false);
        }
    }

    @Override
    public TableFunctionProcessorState process() {
        if (finished) return TableFunctionProcessorState.Finished.FINISHED;
        AnnotatedBatch batch;
        try {
            batch = session.tick();
        } catch (NoSuchElementException endOfStream) {
            finished = true;
            return TableFunctionProcessorState.Finished.FINISHED;
        } catch (RuntimeException e) {
            connectionHealthy = false;
            throw e;
        }
        VectorSchemaRoot root = batch.root();
        int rowCount = root.getRowCount();
        List<Field> fields = arrowSchema.getFields();
        Block[] blocks = new Block[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            FieldVector vector = root.getVector(field.getName());
            Type type = VgiTypeMapping.toTrinoType(field);
            blocks[i] = VgiTypeMapping.toBlock(type, vector, rowCount);
        }
        return TableFunctionProcessorState.Processed.produced(new Page(rowCount, blocks));
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
