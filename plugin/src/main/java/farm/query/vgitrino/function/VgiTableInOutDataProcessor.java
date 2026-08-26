// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.GlobalInitResponse;
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
 * <h2>Finalize phase</h2>
 *
 * <p>If {@link VgiTableInOutTableFunctionHandle#hasFinalize()}, true
 * end-of-input does NOT release the connection — it closes the INPUT-phase
 * stream (which itself closes the input writer and drains any leftover
 * output), then issues a SECOND, separate {@code init(phase=FINALIZE)} call
 * on the SAME connection, and drains ITS answer in producer mode ({@code
 * tick()} in a loop) until true end-of-stream, handing each non-empty batch
 * back as its own {@link TableFunctionProcessorState.Processed#produced}
 * result — {@code finish()} can legitimately return several separate output
 * batches (confirmed against the real fixture worker's {@code
 * multi_batch_finish}), each requiring its own {@code tick()} round trip, not
 * just multiple rows in one batch.
 *
 * <p>The FINALIZE call MUST carry the INPUT phase's own {@code execution_id}
 * (never a fresh/null one) — confirmed against the real reference client and
 * worker: VGI's server-side state store ({@code BoundStorage}) is keyed by
 * {@code execution_id}, not by the connection or worker process, so a fresh
 * id would silently correlate to an empty accumulator instead of throwing.
 * {@link #redeem} captures it off the INPUT-phase stream's header (a real
 * {@link GlobalInitResponse}, per the real service interface's {@code
 * @StreamHeader(GlobalInitResponse.class)} declaration on {@code init}).
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
    private GlobalInitResponse inputHeader;
    private boolean finalizeStarted;
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
                    null,           // execution_id — worker mints one; captured below for FINALIZE
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id — only matters for HTTP multi-backend fan-out
                    null,           // split_tokens — no split enumeration for a data-driven call
                    null);          // row_limit
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            this.session = (ClientStreamSession<?>) stream;
            this.inputHeader = (GlobalInitResponse) stream.header();
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
            // True end-of-input. Every subsequent call also arrives with input==null (per the real
            // SPI's own contract: "if all sources are fully processed, the argument is null") — so
            // once finalizeStarted is true, every later call here just continues draining it.
            if (!handle.hasFinalize()) {
                finishAndRelease(connection);
                return TableFunctionProcessorState.Finished.FINISHED;
            }
            if (!finalizeStarted) {
                startFinalize(connection);
            }
            return drainFinalizeOnce(connection);
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

    /**
     * Ends the INPUT-phase stream and opens the FINALIZE-phase one on the SAME connection, carrying
     * the INPUT phase's own {@code execution_id}/{@code opaque_data} forward — see this class's own
     * javadoc for why that carry-forward is load-bearing, not optional.
     */
    private void startFinalize(VgiWorkerClient.Attached connection) {
        try {
            session.close(); // idempotent; ends the input writer and drains any leftover INPUT output
            InitRequest finalizeInit = new InitRequest(
                    handle.bindCall(),
                    handle.outputSchema(),
                    handle.bindOpaqueData(),
                    null, null, null,
                    "FINALIZE",
                    inputHeader.execution_id(),
                    inputHeader.opaque_data(),
                    null, null, null, null,
                    null, null,
                    null,           // finalize_state_id — only for the (separate) buffered-table path
                    null,           // substream_id
                    null,
                    null);
            RpcStream<? extends StreamState> finalizeStream = connection.service().init(finalizeInit, null);
            this.session = (ClientStreamSession<?>) finalizeStream;
            this.finalizeStarted = true;
        } catch (RuntimeException e) {
            finished = true;
            connectionHealthy = false;
            client.release(connection, false);
            throw e;
        }
    }

    /**
     * Drains the FINALIZE-phase producer stream — {@code finish()} may legally take several
     * {@code tick()}s to drain fully (see this class's own javadoc) — LOOPING on a zero-row tick
     * rather than returning it to Trino, since this method is only ever reached with {@code
     * input == null} (true end-of-input). Confirmed against the real Trino engine ({@code
     * RegularTableFunctionPartition.toOutputPages}): it throws {@code "When function got no
     * input, it should either produce output or return Blocked state"} — unconditionally, not a
     * race — the instant a null-input call returns a bare {@code Processed} with no page. A
     * mid-stream zero-row (but not-EOS) tick is exactly that shape, so it must never be returned
     * directly; retrying the tick in the same call (still a real blocking network round trip, not
     * a local busy-spin) is the correct fix, since {@code Blocked} would need a real {@code
     * CompletableFuture} this synchronous transport doesn't have.
     */
    private TableFunctionProcessorState drainFinalizeOnce(VgiWorkerClient.Attached connection) {
        while (true) {
            try {
                AnnotatedBatch out = session.tick();
                int rowCount = out.root().getRowCount();
                if (rowCount == 0) {
                    continue;
                }
                return TableFunctionProcessorState.Processed.produced(toPage(out.root(), rowCount));
            } catch (NoSuchElementException endOfStream) {
                finishAndRelease(connection);
                return TableFunctionProcessorState.Finished.FINISHED;
            } catch (RuntimeException e) {
                finished = true;
                connectionHealthy = false;
                client.release(connection, false);
                throw e;
            }
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
