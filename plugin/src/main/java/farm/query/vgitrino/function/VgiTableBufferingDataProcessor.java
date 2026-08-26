// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.TableBufferingCombineRequest;
import farm.query.vgi.protocol.TableBufferingCombineResponse;
import farm.query.vgi.protocol.TableBufferingDestructorRequest;
import farm.query.vgi.protocol.TableBufferingProcessRequest;
import farm.query.vgi.protocol.TableBufferingProcessResponse;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowBatchCodec;
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

/**
 * Drives ONE partition of a VGI {@code TableBufferingFunction} call: {@code
 * table_buffering_process} (unary Sink RPC) once per input page, then — on true end-of-input —
 * one {@code table_buffering_combine} (unary Combine RPC), then a streaming producer-mode {@code
 * init(phase=TABLE_BUFFERING_FINALIZE)}/{@code tick()} sequence (Source phase) per returned {@code
 * finalize_state_id}, and finally a best-effort {@code table_buffering_destructor}.
 *
 * <h2>Why this looks like {@code VgiAggregateFunctions}, not {@code VgiTableInOutDataProcessor}</h2>
 *
 * <p>Confirmed directly from the real worker SDK: unlike classic table-in-out's {@code
 * init(phase=INPUT)}/{@code exchange()} streaming session, a {@code TableBufferingFunction}'s Sink
 * and Combine phases are plain UNARY RPCs — {@code table_buffering_process}/{@code
 * table_buffering_combine} — with no {@code OutputCollector} and no persistent stream at all. This
 * connector therefore borrows-and-releases a (possibly different) pooled connection for EACH Sink
 * call and for the one Combine call, exactly like {@code VgiAggregateFunctions#aggregate_update}/
 * {@code #aggregate_combine} already do, rather than holding one connection open across the whole
 * scan. Only the Source/finalize phase needs connection affinity — one held connection per {@code
 * finalize_state_id}'s streaming {@code tick()} sequence, mirroring {@link
 * VgiTableInOutDataProcessor}'s FINALIZE-phase drain loop exactly.
 *
 * <h2>Establishing {@code execution_id} — the one place a stream is opened for the Sink phase</h2>
 *
 * <p>The very first thing this processor does (lazily, on the first real page OR on true
 * end-of-input if the scan saw zero rows) is a single {@code init(phase=TABLE_BUFFERING)} call —
 * this DOES open an exchange-mode {@link RpcStream} on the wire (the real worker's {@code
 * TableInOutBindData}-equivalent selects exchange mode whenever {@code bind_call.input_schema} is
 * non-null), but this connector never exchanges real data on it: it reads {@code execution_id} off
 * the stream's {@link GlobalInitResponse} header, then immediately calls {@code session.close()}
 * to drain it (send EOS, discard any — always empty — output), freeing the connection for the
 * unary RPCs that follow. This mirrors the real C++ extension's own {@code drain_init_stream}
 * helper (see {@code PhysicalVgiTableBufferingFunction::Sink}'s own comment: "the init RPC opens a
 * Stream on the wire; for TABLE_BUFFERING we don't use it — all subsequent traffic is unary
 * RPCs"). Every {@code TableBufferingProcessRequest}/{@code TableBufferingCombineRequest}/{@code
 * TableBufferingDestructorRequest} after that carries this SAME {@code execution_id} — VGI's
 * server-side state store ({@code BoundStorage}) is keyed by it, not by connection or worker
 * process, which is exactly what lets each of those unary calls land on any pooled connection.
 *
 * <h2>Batch index</h2>
 *
 * <p>A monotonically increasing counter, incremented once per Sink call, is always supplied as
 * {@code batch_index} — trivially valid for {@code Meta.requires_input_batch_index=True}
 * functions, since a single Trino partition delivers every page to this processor strictly in
 * order on one thread; there is no parallel-ingest reordering to reconstruct.
 *
 * <h2>A genuine SPI gap, inherited from {@code VgiTableInOutDataProcessor}</h2>
 *
 * <p>{@link TableFunctionDataProcessor} declares no {@code close()}/lifecycle hook — if Trino
 * abandons a partition mid-Source-phase (e.g. a {@code LIMIT} satisfied elsewhere) before this
 * processor reaches {@code FINISHED} on its own, the currently-held finalize-stream connection
 * leaks until GC. The Sink phase itself never holds a connection between calls, so it has no such
 * exposure.
 */
public final class VgiTableBufferingDataProcessor implements TableFunctionDataProcessor {

    private final VgiWorkerClient client;
    private final VgiTableBufferingFunctionHandle handle;
    private final Schema inputArrowSchema;
    private final Schema outputArrowSchema;
    private final List<Type> inputColumnTypes;
    private final List<Type> outputColumnTypes;

    // Sink phase state.
    private byte[] executionId;
    private long nextBatchIndex;
    private final List<byte[]> stateIds = new ArrayList<>();

    // Combine phase state.
    private List<byte[]> finalizeStateIds;
    private int finalizeIndex = -1;

    // Source phase state — held only for the CURRENTLY-draining finalize_state_id's stream.
    private VgiWorkerClient.Attached finalizeConnection;
    private ClientStreamSession<?> finalizeSession;

    private boolean finished;

    public VgiTableBufferingDataProcessor(VgiWorkerClient client, VgiTableBufferingFunctionHandle handle) {
        this.client = client;
        this.handle = handle;
        this.inputArrowSchema = ArrowSchemaCodec.deserializeSchema(handle.inputSchema());
        this.outputArrowSchema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        this.inputColumnTypes = fieldsToTypes(inputArrowSchema);
        this.outputColumnTypes = fieldsToTypes(outputArrowSchema);
    }

    @Override
    public TableFunctionProcessorState process(List<Optional<Page>> input) {
        if (finished) return TableFunctionProcessorState.Finished.FINISHED;

        if (input != null) {
            // VGI structurally forbids more than one TableInput argument, so there is always
            // exactly one source here (see VgiArgSpec.tableArg's javadoc).
            Optional<Page> maybePage = input.isEmpty() ? Optional.empty() : input.get(0);
            if (maybePage.isEmpty()) {
                return TableFunctionProcessorState.Processed.usedInput();
            }
            sinkOneBatch(maybePage.get());
            return TableFunctionProcessorState.Processed.usedInput();
        }

        // True end-of-input: Combine, then drive the Source phase one step per call.
        ensureExecutionId(); // in case the scan saw zero rows and sinkOneBatch never ran
        if (finalizeStateIds == null) {
            finalizeStateIds = runCombine();
        }
        return driveFinalize();
    }

    /** Sink phase: one {@code table_buffering_process} unary RPC for this page's batch. */
    private void sinkOneBatch(Page page) {
        ensureExecutionId();
        try (VectorSchemaRoot inputBatch = VgiTypeMapping.pageToBatch(inputArrowSchema, inputColumnTypes, page)) {
            byte[] batchBytes = ArrowBatchCodec.serialize(inputBatch);
            long batchIndex = nextBatchIndex++;
            byte[] executionIdSnapshot = executionId;
            TableBufferingProcessResponse response = client.withConnection(a -> a.service().table_buffering_process(
                    new TableBufferingProcessRequest(handle.functionName(), executionIdSnapshot, batchBytes,
                            a.handle(), null, batchIndex, handle.schemaName()),
                    null));
            stateIds.add(response.state_id());
        }
    }

    /**
     * Establishes {@link #executionId} via a single {@code init(phase=TABLE_BUFFERING)} call, idempotent
     * after the first successful run. See this class's own javadoc for why the opened stream is
     * immediately drained and discarded rather than exchanged on.
     */
    private void ensureExecutionId() {
        if (executionId != null) return;
        executionId = client.withConnection(a -> {
            InitRequest initRequest = new InitRequest(
                    handle.bindCall(),
                    handle.outputSchema(),
                    handle.bindOpaqueData(),
                    null,           // projection_ids
                    null,           // pushdown_filters
                    null,           // join_keys
                    "TABLE_BUFFERING", // phase
                    null,           // execution_id — worker mints one (primary init); captured below
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id — only for the FINALIZE phase
                    null,           // substream_id — only matters for HTTP multi-backend fan-out
                    null,           // split_tokens — no split enumeration for a data-driven call
                    null);          // row_limit
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            GlobalInitResponse header = (GlobalInitResponse) stream.header();
            byte[] mintedExecutionId = header.execution_id();
            // Drains the exchange-mode stream (EOS write + drain any — always empty — output),
            // mirroring the C++ operator's own drain_init_stream so the connection is free for the
            // unary table_buffering_* RPCs that follow.
            ((ClientStreamSession<?>) stream).close();
            return mintedExecutionId;
        });
    }

    /** Combine phase: exactly one {@code table_buffering_combine} unary RPC, over every {@code state_id}
     *  collected from every Sink call (possibly zero, for an empty input table). */
    private List<byte[]> runCombine() {
        byte[] executionIdSnapshot = executionId;
        TableBufferingCombineResponse response = client.withConnection(a -> a.service().table_buffering_combine(
                new TableBufferingCombineRequest(handle.functionName(), executionIdSnapshot, List.copyOf(stateIds),
                        a.handle(), null, handle.schemaName()),
                null));
        return response.finalize_state_ids();
    }

    /**
     * One step of the Source phase: open the next {@code finalize_state_id}'s stream if none is
     * currently open, then drain {@code tick()}s of the currently-open one — LOOPING internally
     * until it has a real page to return, or reaches genuine completion.
     *
     * <p>This must never return {@link TableFunctionProcessorState.Processed#usedInput()} (no
     * page) while this call was reached with {@code input == null} (true end-of-input) — confirmed
     * against the real Trino engine ({@code RegularTableFunctionPartition.toOutputPages}): it
     * tracks whether the JUST-FED input was {@code null}, and if the returned {@link
     * TableFunctionProcessorState} is a bare {@code Processed} with no result page, throws {@code
     * "When function got no input, it should either produce output or return Blocked state"} —
     * unconditionally, not a race. Opening a new finalize stream produces no page yet, and a
     * mid-stream zero-row (but not-EOS) {@code tick()} produces no page yet either — both are
     * exactly the shape that throws. Since {@code Blocked} needs a real {@code CompletableFuture}
     * this synchronous blocking-network-I/O design doesn't have, the correct fix is to never
     * surface that intermediate no-progress state to Trino at all: loop straight into the next
     * synchronous step (opening the stream then immediately ticking it; retrying a zero-row tick;
     * advancing to the next {@code finalize_state_id} on EOS) until a real page, true {@code
     * FINISHED}, or a genuine error is reached — each iteration is still a real blocking network
     * round trip, not a local busy-spin.
     */
    private TableFunctionProcessorState driveFinalize() {
        while (true) {
            if (finalizeSession == null) {
                finalizeIndex++;
                if (finalizeIndex >= finalizeStateIds.size()) {
                    runDestructorBestEffort();
                    finished = true;
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                openFinalizeStream(finalizeStateIds.get(finalizeIndex));
                continue;
            }
            try {
                AnnotatedBatch out = finalizeSession.tick();
                int rowCount = out.root().getRowCount();
                if (rowCount == 0) {
                    continue;
                }
                return TableFunctionProcessorState.Processed.produced(toPage(out.root(), rowCount));
            } catch (NoSuchElementException endOfStream) {
                closeCurrentFinalizeStream(true);
                continue;
            } catch (RuntimeException e) {
                closeCurrentFinalizeStream(false);
                finished = true;
                throw e;
            }
        }
    }

    private void openFinalizeStream(byte[] finalizeStateId) {
        VgiWorkerClient.Attached connection = client.borrow();
        boolean ok = false;
        try {
            InitRequest finalizeInit = new InitRequest(
                    handle.bindCall(),
                    handle.outputSchema(),
                    handle.bindOpaqueData(),
                    null, null, null,
                    "TABLE_BUFFERING_FINALIZE",
                    executionId,
                    null,
                    null, null, null, null,
                    null, null,
                    finalizeStateId,
                    null,           // substream_id
                    null,
                    null);
            RpcStream<? extends StreamState> stream = connection.service().init(finalizeInit, null);
            this.finalizeSession = (ClientStreamSession<?>) stream;
            this.finalizeConnection = connection;
            ok = true;
        } finally {
            if (!ok) client.release(connection, false);
        }
    }

    private void closeCurrentFinalizeStream(boolean healthy) {
        boolean stillHealthy = healthy;
        try {
            finalizeSession.close(); // idempotent; see ClientStreamSession.close's own javadoc
        } catch (RuntimeException e) {
            stillHealthy = false;
        } finally {
            client.release(finalizeConnection, stillHealthy);
            finalizeSession = null;
            finalizeConnection = null;
        }
    }

    /** Best-effort end-of-query cleanup — mirrors the real C++ extension's own destructor RPC,
     *  which likewise never lets a failure here surface as a query error. */
    private void runDestructorBestEffort() {
        try {
            byte[] executionIdSnapshot = executionId;
            client.withConnection(a -> a.service().table_buffering_destructor(
                    new TableBufferingDestructorRequest(handle.functionName(), executionIdSnapshot, a.handle(),
                            null, handle.schemaName()),
                    null));
        } catch (RuntimeException e) {
            // Best-effort — the worker's own cleanup_old_entries GC is the long-term backstop.
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
