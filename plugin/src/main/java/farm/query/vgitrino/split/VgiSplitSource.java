// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.VgiConfig;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.filter.VgiFilterEncoding;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Paginates {@code table_function_plan} into {@link VgiSplit}s, following
 * {@code next_cursors} across calls until the plan is exhausted.
 *
 * <p>{@code max_splits_per_response} maps directly onto {@code maxSize} —
 * VGI's own protocol docs name Trino's {@code getNextBatch(maxSize)} as the
 * reason that field exists at all — so the pagination cap this source asks
 * for is exactly what Trino asked it for, clamped to this catalog's
 * {@link VgiConfig#maxSplitsPerResponse()} ceiling.
 *
 * <h2>Dynamic filtering</h2>
 *
 * <p>Every {@link #getNextBatch} call carries Trino's own
 * {@link DynamicFilterSnapshot} — a join's build-side domain, narrowed as
 * more of it is collected. This intersects with the scan's own static
 * constraint (from {@code applyFilter}) and travels to the worker exactly
 * where VGI's protocol says a plan-time filter belongs: the FIRST call
 * (no {@link #cursor} yet) sends everything currently known as plain
 * {@code pushdown_filters}; a LATER call sends only what's narrowed further
 * since the previous call, as {@code refined_filters} — which the protocol
 * documents as affecting future splits only, so a split already emitted
 * under a looser filter is never invalidated by one. {@code filters_complete}
 * carries {@link DynamicFilterSnapshot#isComplete()} on every call, so a
 * worker that wants to hold back splits until the filter set stabilizes can.
 *
 * <p>{@link #getRequestedDynamicFilterWaitTimeoutMillis} asks Trino to delay
 * the very first call until either the dynamic filter completes or the
 * configured timeout elapses — without it, a broadcast join's build side
 * often hasn't finished by the time this source would otherwise start
 * planning, and every split already emitted (which a continuation's
 * {@code refined_filters} cannot retroactively narrow) would miss the
 * filter entirely.
 *
 * <p>Reusing {@code VgiFilterTranslator}/{@code VgiFilterEncoding} — the same
 * machinery {@code applyFilter}'s static pushdown uses — rather than VGI's
 * separate {@code join_keys} wire mechanism is a deliberate simplification:
 * Trino's {@code DynamicFilter} already reduces a join's build side to a
 * {@code TupleDomain} (a discrete value set when small enough, otherwise a
 * min/max range), which is exactly the shape {@code VgiFilterTranslator}
 * already translates for an ordinary {@code IN}/range predicate. There is no
 * need for a second encoding path just because the domain arrived from a
 * join instead of a literal {@code WHERE} clause.
 *
 * <p>This applies only to plain table scans. Trino's {@code ConnectorTableFunction}
 * SPI (as of Trino 483) has no {@code Constraint}/{@code DynamicFilter} hook
 * anywhere in the {@code TABLE(...)} call path — {@code ConnectorTableFunctionHandle}
 * is a bare marker interface and {@code TableFunctionProcessorProvider.getSplitProcessor}
 * takes no filter of any kind — so a PTF-sourced split source is always built
 * with {@link TupleDomain#all()} and an empty projection here; see the
 * README's Scope section.
 */
public final class VgiSplitSource implements ConnectorSplitSource {

    private final VgiWorkerClient client;
    private final VgiConfig config;
    private final byte[] bindCall;
    private final byte[] bindOpaqueData;
    private final List<Integer> projectionIds;
    private final Schema fullSchema;
    private final List<VgiColumnHandle> projectedColumns;
    private final TupleDomain<VgiColumnHandle> staticConstraint;

    private byte[] cursor;
    private volatile boolean finished;
    private int pagesFetched;

    /** The fullest (static ∩ dynamic-so-far) predicate already sent to the worker, across calls. */
    private TupleDomain<VgiColumnHandle> lastCommunicatedPredicate = TupleDomain.all();

    /**
     * @param client the pooled worker connection
     * @param config this catalog's configuration (sizing knobs)
     * @param bindCall the serialised {@code BindRequest} this scan was bound with
     * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
     * @param projectionIds the columns this scan actually reads (their ordinals
     *        in the table's bind-time Arrow schema), or {@code null} for all of
     *        them. Sourced from whatever Trino already told
     *        {@link VgiSplitManager#getSplits} it needed — no
     *        {@code applyProjection} plumbing required to get this far
     * @param fullSchema the table's full (bind-time) Arrow schema — needed to
     *        translate a column's constraint into VGI's Arrow-typed filter
     *        AST (see {@code VgiFilterTranslator})
     * @param projectedColumns the same columns {@code projectionIds} names,
     *        as {@link VgiColumnHandle}s in that same order — a filter's
     *        column index is relative to this projection, per
     *        {@code VgiFilterEncoding}'s own javadoc
     * @param staticConstraint this scan's own stored constraint (from
     *        {@code applyFilter}), or {@link TupleDomain#all()} for a table
     *        function call, which carries none
     */
    public VgiSplitSource(VgiWorkerClient client, VgiConfig config, byte[] bindCall, byte[] bindOpaqueData,
            List<Integer> projectionIds, Schema fullSchema, List<VgiColumnHandle> projectedColumns,
            TupleDomain<VgiColumnHandle> staticConstraint) {
        this.client = client;
        this.config = config;
        this.bindCall = bindCall;
        this.bindOpaqueData = bindOpaqueData;
        this.projectionIds = projectionIds;
        this.fullSchema = fullSchema;
        this.projectedColumns = projectedColumns;
        this.staticConstraint = staticConstraint;
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis() {
        return config.dynamicFilteringWaitTimeoutMillis();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilter) {
        // client.executor(), NOT the default (the JVM-wide common ForkJoinPool)
        // — see that field's own javadoc for why a connector-private pool is
        // worth using regardless of what runs on it.
        return CompletableFuture.supplyAsync(() -> fetchNextBatch(maxSize, dynamicFilter), client.executor());
    }

    private List<ConnectorSplit> fetchNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilter) {
        pagesFetched++;
        if (pagesFetched > config.maxPlanPages()) {
            // Stopping early and returning what was already collected would
            // turn this into a SILENT SUBSET — a correct-looking answer
            // missing rows, with no error — which is worse than failing
            // outright. Name the cap so an operator can tell this was the
            // client's bound, not a genuine absence of further data.
            throw new RuntimeException("table_function_plan exceeded the scan-planning page cap ("
                    + config.maxPlanPages() + " pages, vgi.max-plan-pages) — the worker either has an "
                    + "unusually large split enumeration or never stops cursoring; raise vgi.max-plan-pages "
                    + "if the former");
        }
        int cap = Math.min(Math.max(1, maxSize), Math.max(1, config.maxSplitsPerResponse()));

        TupleDomain<VgiColumnHandle> dynamicPredicate = dynamicFilter == null
                ? TupleDomain.all()
                : dynamicFilter.currentPredicate().transformKeys(VgiColumnHandle.class::cast);
        TupleDomain<VgiColumnHandle> merged = staticConstraint.intersect(dynamicPredicate);
        boolean filtersComplete = dynamicFilter == null || dynamicFilter.isComplete();

        byte[] pushdownFiltersForThisCall = null;
        byte[] refinedFiltersForThisCall = null;
        if (cursor == null) {
            // First call: everything currently known (static + whatever of the
            // dynamic filter has arrived so far) travels as the plain field.
            pushdownFiltersForThisCall = VgiFilterEncoding.encode(merged, fullSchema, projectedColumns);
        } else if (!merged.equals(lastCommunicatedPredicate)) {
            // A later page: send only the narrowing beyond what earlier splits
            // (already emitted, and not re-planned) were told applies.
            refinedFiltersForThisCall = VgiFilterEncoding.encode(merged, fullSchema, projectedColumns);
        }
        lastCommunicatedPredicate = merged;

        TableFunctionPlanRequest request = new TableFunctionPlanRequest(
                bindCall, bindOpaqueData,
                projectionIds,
                pushdownFiltersForThisCall,
                null,                       // join_keys — v1 folds a dynamic filter into
                                             // pushdown_filters/refined_filters via the same
                                             // Domain-shaped translation a static predicate
                                             // uses, rather than VGI's separate join_keys
                                             // wire mechanism; see this class's own doc
                null,                       // row_limit
                config.targetSplitBytes(),
                config.minSplits(),
                (long) cap,
                cursor,
                refinedFiltersForThisCall,
                filtersComplete,
                null, null,                 // start/end position
                null, null, null, null,     // order-by hint
                null, null);                // tablesample hint

        PlanResponse response = client.withConnection(a ->
                a.service().table_function_plan(RecordCodec.serializeToBytes(request), null));

        List<ConnectorSplit> out = new ArrayList<>(response.splits().size());
        for (byte[] blob : response.splits()) {
            ScanSplit split = RecordCodec.deserializeFromBytes(blob, ScanSplit.class);
            if (split.token().length == 0) {
                // The not-split-capable sentinel: exactly one such split, standing
                // for the whole scan. Stop here regardless of next_cursors.
                out.add(new VgiSplit(bindCall, bindOpaqueData, new byte[0], 0L, 0L, List.of()));
                finished = true;
                return out;
            }
            long estimatedBytes = split.estimated_bytes() == null ? 0L : split.estimated_bytes();
            long targetHint = config.targetSplitBytes() == null ? 0L : config.targetSplitBytes();
            out.add(new VgiSplit(bindCall, bindOpaqueData, split.token(),
                    estimatedBytes, targetHint, resolveAddresses(response, split)));
        }

        List<byte[]> nextCursors = response.next_cursors();
        if (nextCursors == null || nextCursors.isEmpty()) {
            finished = true;
        } else {
            // v1 follows only the first continuation cursor. More than one means
            // parallel enumeration, which the protocol itself documents as sound
            // only when the cursors partition the remaining enumeration disjointly
            // and exhaustively — safe to ignore the rest and just keep following
            // one, at the cost of not fanning the plan phase itself out further.
            cursor = nextCursors.get(0);
        }
        return out;
    }

    private static List<String> resolveAddresses(PlanResponse response, ScanSplit split) {
        List<Long> locationIds = split.location_ids();
        List<String> locations = response.locations();
        if (locationIds == null || locationIds.isEmpty() || locations == null || locations.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>(locationIds.size());
        for (Long id : locationIds) {
            if (id != null && id >= 0 && id < locations.size()) {
                out.add(locations.get(id.intValue()));
            }
        }
        return out;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void close() {
        // No per-source resources: splits are redeemed through the shared pool.
    }
}
