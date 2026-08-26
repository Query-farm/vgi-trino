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
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Drives ONE partition of a classic (non-blended) VGI table-in-out call:
 * {@code init(phase=INPUT)} once, then one {@code exchange()} turn (write one
 * page's worth of input, read the matching output) per {@link #process}
 * call, until Trino signals true end-of-input — a long-lived streaming
 * session fed incrementally, unlike {@link VgiTableInOutSplitProcessor}'s
 * single-shot literal-call exchange.
 *
 * <p>Every VGI classic table-in-out function this connector registers has NO
 * finalize phase (see {@code VgiTableInOutTableFunctions#discover}'s v1
 * scope note) — so the whole call is exactly this one-phase write/read loop;
 * there is no second {@code init(phase=FINALIZE)} turn to drive.
 *
 * <h2>A genuine, undocumented-elsewhere SPI gap: no close hook</h2>
 *
 * <p>Unlike {@link io.trino.spi.function.table.TableFunctionSplitProcessor}
 * (which declares a {@code close()} Trino always calls), {@link
 * TableFunctionDataProcessor} — verified directly against the real SPI
 * source — declares ONLY {@link #process}, with no lifecycle hook at all. If
 * Trino ever abandons a partition before this processor itself returns
 * {@code FINISHED} (e.g. a {@code LIMIT} satisfied by an earlier partition),
 * there is no notification and this processor's borrowed connection leaks
 * until GC — a real ceiling of this Trino version's SPI, not a bug this
 * class can fix; every other place in this connector that borrows a
 * connection has a real release path, this is the one exception.
 */
public final class VgiTableInOutDataProcessor implements TableFunctionDataProcessor {

    private final VgiWorkerClient client;
    private final VgiTableInOutTableFunctionHandle handle;
    private final Schema inputArrowSchema;
    private final Schema outputArrowSchema;
    private final List<Type> inputColumnTypes;
    private final List<Type> outputColumnTypes;

    private final CompletableFuture<VgiWorkerClient.Attached> connectionFuture;
    private final CompletableFuture<VgiWorkerClient.Attached> redemption;

    private ClientStreamSession<?> session;
    private boolean finished;
    private boolean connectionHealthy = true;

    public VgiTableInOutDataProcessor(VgiWorkerClient client, VgiTableInOutTableFunctionHandle handle) {
        this.client = client;
        this.handle = handle;
        this.inputArrowSchema = ArrowSchemaCodec.deserializeSchema(handle.inputSchema());
        this.outputArrowSchema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        this.inputColumnTypes = fieldsToTypes(inputArrowSchema);
        this.outputColumnTypes = fieldsToTypes(outputArrowSchema);
        this.connectionFuture = client.borrowAsync();
        this.redemption = connectionFuture.thenApplyAsync(this::redeem, client.executor());
    }

    /** Runs on {@link VgiWorkerClient#executor()} once a connection is assigned. */
    private VgiWorkerClient.Attached redeem(VgiWorkerClient.Attached a) {
        boolean ok = false;
        try {
            InitRequest initRequest = new InitRequest(
                    handle.bindCall(),
                    handle.outputSchema(),
                    handle.bindOpaqueData(),
                    null,           // projection_ids
                    null,           // pushdown_filters
                    null,           // join_keys
                    "INPUT",        // phase
                    null,           // execution_id — worker mints one
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id
                    null,           // split_tokens — no split enumeration for a data-driven call
                    null);          // row_limit
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            this.session = (ClientStreamSession<?>) stream;
            ok = true;
            return a;
        } finally {
            if (!ok) client.release(a, false);
        }
    }

    @Override
    public TableFunctionProcessorState process(List<Optional<Page>> input) {
        if (finished) return TableFunctionProcessorState.Finished.FINISHED;
        if (!redemption.isDone()) {
            return TableFunctionProcessorState.Blocked.blocked(redemption.thenApply(a -> null));
        }
        // "Done" can mean "failed" — join() surfaces that as a CompletionException.
        VgiWorkerClient.Attached connection = redemption.join();

        if (input == null) {
            // True end-of-input: no finalize phase to continue into (v1 scope) — signal EOS and
            // release. close() drains any trailing output, which is a no-op here since a no-finalize
            // worker has nothing left to say once every input batch has been answered.
            finishAndRelease(connection);
            return TableFunctionProcessorState.Finished.FINISHED;
        }
        // VGI structurally forbids more than one TableInput argument (see VgiArgSpec.tableArg's
        // javadoc), so there is always exactly one source here.
        Optional<Page> maybePage = input.isEmpty() ? Optional.empty() : input.get(0);
        if (maybePage.isEmpty()) {
            // This source is already exhausted (or, defensively, has nothing new this turn) — nothing
            // to send; wait for the next call rather than exchanging an empty page for no reason.
            return TableFunctionProcessorState.Processed.usedInput();
        }
        try (VectorSchemaRoot inputBatch =
                VgiTypeMapping.pageToBatch(inputArrowSchema, inputColumnTypes, maybePage.get())) {
            AnnotatedBatch out = session.exchange(new AnnotatedBatch(inputBatch, null));
            int rowCount = out.root().getRowCount();
            if (rowCount == 0) {
                // "Worker consumed this input, produced nothing yet" — NOT end of stream; keep going.
                return TableFunctionProcessorState.Processed.usedInput();
            }
            return TableFunctionProcessorState.Processed.usedInputAndProduced(toPage(out.root(), rowCount));
        } catch (NoSuchElementException endOfStream) {
            // The server ended the stream instead of answering — ClientStreamSession.exchange
            // already closed the session itself in this case (see its own javadoc).
            finished = true;
            client.release(connection, false);
            return TableFunctionProcessorState.Finished.FINISHED;
        } catch (RuntimeException e) {
            finished = true;
            connectionHealthy = false;
            client.release(connection, false);
            throw e;
        }
    }

    private void finishAndRelease(VgiWorkerClient.Attached connection) {
        finished = true;
        try {
            session.close(); // idempotent — see ClientStreamSession.close's own javadoc
        } catch (RuntimeException e) {
            connectionHealthy = false;
        } finally {
            client.release(connection, connectionHealthy);
        }
    }

    private Page toPage(VectorSchemaRoot root, int rowCount) {
        List<Field> fields = outputArrowSchema.getFields();
        Block[] blocks = new Block[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            FieldVector vector = root.getVector(field.getName());
            blocks[i] = VgiTypeMapping.toBlock(outputColumnTypes.get(i), vector, rowCount);
        }
        return new Page(rowCount, blocks);
    }

    private static List<Type> fieldsToTypes(Schema schema) {
        List<Type> types = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) types.add(VgiTypeMapping.toTrinoType(field));
        return types;
    }
}
