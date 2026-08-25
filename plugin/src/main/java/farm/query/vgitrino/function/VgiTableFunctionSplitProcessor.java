// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.split.VgiSplit;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import farm.query.vgirpc.AnnotatedBatch;
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
import java.util.concurrent.CompletableFuture;

/**
 * Redeems one {@link VgiSplit} of a table-function call via {@code init()}
 * and drains it into Trino {@link Page}s.
 *
 * <p>The table-function analogue of {@link farm.query.vgitrino.page.VgiPageSource}
 * — same redemption/drain mechanics (including the same non-blocking
 * connection acquisition; see that class's own javadoc for the full
 * rationale), wrapped in the pull-based {@link TableFunctionSplitProcessor#process()}
 * shape (no columns list: a table function has no separate projection step
 * at this layer, so every column of the bound output schema is always
 * emitted) — {@code process()} itself returns {@link TableFunctionProcessorState.Blocked}
 * in place of {@code ConnectorPageSource.isBlocked()}'s separate method.
 */
public final class VgiTableFunctionSplitProcessor implements TableFunctionSplitProcessor {

    private final VgiWorkerClient client;
    private final Schema arrowSchema;

    private final CompletableFuture<VgiWorkerClient.Attached> connectionFuture;
    private final CompletableFuture<VgiWorkerClient.Attached> redemption;

    private volatile RpcStream<?> session;
    private volatile boolean closeRequested;
    private boolean finished;
    private volatile boolean connectionHealthy = true;

    /**
     * @param client the pool to borrow a connection from for this split's lifetime
     * @param split the split to redeem
     * @param outputSchema the call's bound (IPC-encoded) output schema
     */
    public VgiTableFunctionSplitProcessor(VgiWorkerClient client, VgiSplit split, byte[] outputSchema) {
        this.client = client;
        this.arrowSchema = ArrowSchemaCodec.deserializeSchema(outputSchema);
        this.connectionFuture = client.borrowAsync();
        this.redemption = connectionFuture.thenApplyAsync(a -> redeem(a, split, outputSchema), client.executor());
    }

    /** Runs on {@link VgiWorkerClient#executor()} once a connection is assigned. */
    private VgiWorkerClient.Attached redeem(VgiWorkerClient.Attached a, VgiSplit split, byte[] outputSchema) {
        if (closeRequested) {
            client.release(a, true);
            return null;
        }
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
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            this.session = stream;
            ok = true;
            return a;
        } finally {
            if (!ok) client.release(a, false);
        }
    }

    @Override
    public TableFunctionProcessorState process() {
        if (finished) return TableFunctionProcessorState.Finished.FINISHED;
        if (!redemption.isDone()) {
            return TableFunctionProcessorState.Blocked.blocked(redemption.thenApply(a -> null));
        }
        // "Done" can mean "failed" (the connection future completed
        // exceptionally, or redeem()'s own init() call threw) — join()
        // surfaces that here as a CompletionException rather than silently
        // producing no rows.
        redemption.join();
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
        finished = true;
        closeRequested = true;
        client.cancelPendingBorrow(connectionFuture);
        redemption.whenComplete((connection, error) -> {
            if (connection == null) return;
            try {
                session.close();
            } catch (Exception e) {
                connectionHealthy = false;
            } finally {
                client.release(connection, connectionHealthy);
            }
        });
    }
}
