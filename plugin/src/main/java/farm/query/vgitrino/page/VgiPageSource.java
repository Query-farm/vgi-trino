// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.page;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import farm.query.vgitrino.split.VgiSplit;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import farm.query.vgirpc.AnnotatedBatch;
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
import java.util.concurrent.CompletableFuture;

/**
 * Redeems one {@link VgiSplit} via {@code init()} and drains its producer
 * stream into Trino {@link Page}s.
 *
 * <p>Holds one pooled connection for its entire lifetime (unlike
 * {@link farm.query.vgitrino.metadata.VgiMetadata}/{@link farm.query.vgitrino.split.VgiSplitManager},
 * which borrow-and-return per call) — a producer stream is stateful and
 * lockstep, so it cannot hop between connections mid-drain.
 *
 * <h2>Non-blocking connection acquisition</h2>
 *
 * <p>Construction never blocks the calling Trino engine thread: it kicks off
 * {@link VgiWorkerClient#borrowAsync}, and {@link #isBlocked} surfaces that
 * future directly so the engine parks this page source (yielding the thread
 * back to whatever else it can usefully do) rather than occupying a thread
 * to wait its turn. Trino is free to construct more concurrent page sources
 * than {@link farm.query.vgitrino.VgiConfig#connections()} allows — a
 * {@code LIMIT} satisfiable from the very first split still starts
 * redeeming others in parallel before the engine notices it has enough — and
 * this is exactly how those extras are meant to queue: as pending futures,
 * never as blocked threads.
 *
 * <p>The actual redemption ({@code init()}, a real RPC) runs on {@link
 * VgiWorkerClient#executor()} once a connection is assigned — {@code
 * thenApplyAsync}, deliberately, never a bare {@code thenApply}: without an
 * explicit executor the continuation could run inline on whichever thread
 * completes the connection future, which is {@link VgiWorkerClient#release}
 * — some unrelated split's teardown, or this pool's own constructor-time
 * replacement logic — and none of those should end up unexpectedly running
 * a different split's {@code init()} RPC.
 *
 * <p>{@link #close} on a page source that hasn't been assigned a connection
 * yet withdraws its wait ({@link VgiWorkerClient#cancelPendingBorrow}) rather
 * than trying to cancel the redemption future outright — a plain {@code
 * CompletableFuture.cancel()} does not stop an already-running {@code
 * thenApplyAsync} stage, so cancelling while redemption is in flight would
 * let it finish anyway (opening a real connection this future can no longer
 * publish) and leak both the connection and its stream. Instead, a {@code
 * closeRequested} flag makes the redemption step itself release a
 * connection it received too late, and {@link #close}'s own cleanup — a
 * {@code whenComplete} callback, not a synchronous check — fires exactly
 * once whenever redemption actually resolves, whether that's already
 * happened or hasn't yet.
 */
public final class VgiPageSource implements ConnectorPageSource {

    private final VgiWorkerClient client;
    private final List<VgiColumnHandle> columns;
    private final Schema arrowSchema;

    /** The raw, not-yet-redeemed connection reservation — needed separately from {@link #redemption} so
     *  {@link #close} can withdraw it from {@link VgiWorkerClient}'s waiter queue specifically. */
    private final CompletableFuture<VgiWorkerClient.Attached> connectionFuture;
    /** {@link #connectionFuture} followed by the actual {@code init()} redemption, or {@code null} if
     *  {@link #close} beat redemption to the connection (see the class javadoc). */
    private final CompletableFuture<VgiWorkerClient.Attached> redemption;

    private volatile RpcStream<?> session;
    private volatile boolean closeRequested;
    private long completedBytes;
    private long completedPositions;
    private boolean finished;
    private volatile boolean connectionHealthy = true;

    /**
     * @param client the connection pool to borrow from for this split's lifetime
     * @param split the split to redeem
     * @param columns the columns this page source must emit, in requested order
     * @param tableOutputSchema the owning table's full (bind-time) Arrow schema bytes
     * @param pushdownFilters the encoded static filter predicate for this same
     *        projection (see {@link farm.query.vgitrino.filter.VgiFilterEncoding}),
     *        or {@code null} if there's nothing to push. Column indices inside
     *        it are relative to {@code projectionIds}'s (sorted-by-ordinal)
     *        order below, which the caller must have built the filter against —
     *        sorting independently by ordinal in both places is what keeps the
     *        two consistent without sharing a list instance
     */
    public VgiPageSource(VgiWorkerClient client, VgiSplit split, List<VgiColumnHandle> columns,
            byte[] tableOutputSchema, byte[] pushdownFilters) {
        this.client = client;
        this.columns = columns;
        this.arrowSchema = ArrowSchemaCodec.deserializeSchema(tableOutputSchema);
        this.connectionFuture = client.borrowAsync();
        this.redemption = connectionFuture.thenApplyAsync(
                a -> redeem(a, split, tableOutputSchema, pushdownFilters), client.executor());
    }

    /** Runs on {@link VgiWorkerClient#executor()} once a connection is assigned. */
    private VgiWorkerClient.Attached redeem(VgiWorkerClient.Attached a, VgiSplit split,
            byte[] tableOutputSchema, byte[] pushdownFilters) {
        if (closeRequested) {
            // Closed before we ever got to use this connection — give it
            // straight back rather than redeeming (and thereby occupying) a
            // split nobody wants anymore.
            client.release(a, true);
            return null;
        }
        boolean ok = false;
        try {
            // Same projection Trino already told the split source, sourced the
            // same way — the columns this page source's own caller passed in.
            // Empty means "no restriction" (null), not "zero columns" — see
            // VgiSplitManager's own comment on this for the real-worker bug
            // sending projection_ids=[] caused (0 rows instead of the count).
            List<Integer> projectionIds = columns.isEmpty()
                    ? null : columns.stream().map(VgiColumnHandle::ordinal).sorted().toList();
            InitRequest initRequest = new InitRequest(
                    split.bindCall(),
                    tableOutputSchema,
                    split.bindOpaqueData(),
                    projectionIds,
                    pushdownFilters,
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
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            this.session = stream;
            ok = true;
            return a;
        } finally {
            if (!ok) client.release(a, false);
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
    public CompletableFuture<?> isBlocked() {
        return redemption.isDone() ? NOT_BLOCKED : redemption;
    }

    @Override
    public SourcePage getNextSourcePage() {
        if (finished) return null;
        // Trino only calls this once isBlocked() reports done, but "done" can
        // mean "failed" (borrowAsync's connection never came through cleanly,
        // or init() itself threw) — join() surfaces that as a
        // CompletionException here rather than silently returning no rows.
        VgiWorkerClient.Attached connection = redemption.join();
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
        finished = true;
        closeRequested = true;
        // Best-effort withdraw the connection request if it hasn't been
        // assigned one yet — see this class's own javadoc for why this
        // doesn't also try to cancel `redemption` itself.
        client.cancelPendingBorrow(connectionFuture);
        // Fires immediately (inline, this thread) if redemption is already
        // done — the common case, a split that finished normally — or later,
        // on whichever thread eventually resolves it, if not. Exactly once
        // either way, so this is the sole place that releases the
        // connection back for a page source that DID get one.
        redemption.whenComplete((connection, error) -> {
            if (connection == null) return; // never got one, or redeem() already released it itself
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
