// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
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
 * Redeems a table-in-out literal call's one {@link
 * farm.query.vgitrino.split.VgiTableInOutSplit} via {@code init(phase=INPUT)}
 * plus exactly one write-then-read exchange turn, then reports it finished.
 *
 * <p>This is deliberately NOT a tick loop like {@link VgiTableFunctionSplitProcessor}'s
 * producer-mode scan: a blended function's INPUT-phase exchange contract emits exactly one data
 * batch per exchange call (which may itself hold 0, 1, or many rows — VGI's own {@code
 * BlendedDropFunction}/{@code BlendedExplodeFunction} fixtures exercise all three), and a literal
 * call is guaranteed to have no finalize phase to continue into (see {@link
 * VgiTableInOutFunctions#discover}'s {@code has_finalize} check) — so one {@code exchange()} call
 * is the whole answer, and {@link #process()} only needs to hand it to Trino, then report {@code
 * FINISHED} on the next call.
 *
 * <p>Same non-blocking connection-acquisition pattern as {@link VgiTableFunctionSplitProcessor}
 * — see that class's own javadoc for the full rationale (a literal call can run concurrently
 * across many call sites in one query, same as any other split-scheduled unit of work).
 */
public final class VgiTableInOutSplitProcessor implements TableFunctionSplitProcessor {

    private final VgiWorkerClient client;
    private final VgiTableInOutFunctionHandle handle;
    private final Schema arrowSchema;

    private final CompletableFuture<VgiWorkerClient.Attached> connectionFuture;
    private final CompletableFuture<VgiWorkerClient.Attached> redemption;

    private volatile RpcStream<?> session;
    private volatile boolean closeRequested;
    private boolean produced;
    private boolean finished;
    private volatile boolean connectionHealthy = true;

    /**
     * @param client the pool to borrow a connection from for this call's lifetime
     * @param handle the already-bound literal call to redeem
     */
    public VgiTableInOutSplitProcessor(VgiWorkerClient client, VgiTableInOutFunctionHandle handle) {
        this.client = client;
        this.handle = handle;
        this.arrowSchema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        this.connectionFuture = client.borrowAsync();
        this.redemption = connectionFuture.thenApplyAsync(this::redeem, client.executor());
    }

    /** Runs on {@link VgiWorkerClient#executor()} once a connection is assigned. */
    private VgiWorkerClient.Attached redeem(VgiWorkerClient.Attached a) {
        if (closeRequested) {
            client.release(a, true);
            return null;
        }
        boolean ok = false;
        try {
            InitRequest initRequest = new InitRequest(
                    handle.bindCall(),
                    handle.outputSchema(),
                    handle.bindOpaqueData(),
                    null,           // projection_ids
                    null,           // pushdown_filters
                    null,           // join_keys
                    "INPUT",        // phase — the only phase a literal (no-finalize) call ever uses
                    null,           // execution_id — worker mints one
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id
                    null,           // split_tokens — no split enumeration for a literal call
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
        // "Done" can mean "failed" — join() surfaces that as a CompletionException
        // rather than silently producing no rows.
        redemption.join();
        if (produced) {
            // The one exchange turn already happened on a previous call — nothing more to give.
            finished = true;
            return TableFunctionProcessorState.Finished.FINISHED;
        }
        produced = true;
        ClientStreamSession<?> exchangeSession = (ClientStreamSession<?>) session;
        try (VectorSchemaRoot input = ArrowSchemaCodec.deserializeBatch(handle.literalInputBatch())) {
            AnnotatedBatch out = exchangeSession.exchange(new AnnotatedBatch(input, null));
            Page page = toPage(out);
            // Copy the answer out of the reader-owned root (above, via toPage) BEFORE
            // signalling end-of-input: close() drains any trailing output, which would
            // otherwise invalidate/reuse the very root toPage just read from.
            exchangeSession.close();
            return TableFunctionProcessorState.Processed.produced(page);
        } catch (NoSuchElementException endOfStream) {
            // The server ended the stream instead of answering — ClientStreamSession.exchange
            // already closed the session itself in this case (see its own javadoc).
            finished = true;
            return TableFunctionProcessorState.Finished.FINISHED;
        } catch (RuntimeException e) {
            connectionHealthy = false;
            throw e;
        }
    }

    private Page toPage(AnnotatedBatch batch) {
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
        return new Page(rowCount, blocks);
    }

    @Override
    public void close() throws IOException {
        finished = true;
        closeRequested = true;
        client.cancelPendingBorrow(connectionFuture);
        redemption.whenComplete((connection, error) -> {
            if (connection == null) return;
            try {
                if (session != null) session.close(); // idempotent — see ClientStreamSession.close's own javadoc
            } catch (Exception e) {
                connectionHealthy = false;
            } finally {
                client.release(connection, connectionHealthy);
            }
        });
    }
}
