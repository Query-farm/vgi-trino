// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.VgiConfig;
import farm.query.vgitrino.client.VgiWorkerClient;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;

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
 * <p>v1 does not yet act on {@link DynamicFilterSnapshot}: the plan's Phase 4
 * calls for awaiting a dynamic filter once before the first plan call and
 * threading it into {@code refined_filters}, but that isn't wired up here yet.
 */
public final class VgiSplitSource implements ConnectorSplitSource {

    private final VgiWorkerClient client;
    private final VgiConfig config;
    private final byte[] bindCall;
    private final byte[] bindOpaqueData;
    private final List<Integer> projectionIds;
    private final byte[] pushdownFilters;

    private byte[] cursor;
    private volatile boolean finished;

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
     * @param pushdownFilters the encoded static filter predicate for THIS same
     *        projection (see {@link farm.query.vgitrino.filter.VgiFilterEncoding}),
     *        or {@code null} if there's nothing to push. A plan is built from
     *        static filters only — join-key values aren't known until per-tick
     *        redemption
     */
    public VgiSplitSource(VgiWorkerClient client, VgiConfig config, byte[] bindCall, byte[] bindOpaqueData,
            List<Integer> projectionIds, byte[] pushdownFilters) {
        this.client = client;
        this.config = config;
        this.bindCall = bindCall;
        this.bindOpaqueData = bindOpaqueData;
        this.projectionIds = projectionIds;
        this.pushdownFilters = pushdownFilters;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilter) {
        return CompletableFuture.supplyAsync(() -> fetchNextBatch(maxSize));
    }

    private List<ConnectorSplit> fetchNextBatch(int maxSize) {
        int cap = Math.min(Math.max(1, maxSize), Math.max(1, config.maxSplitsPerResponse()));
        TableFunctionPlanRequest request = new TableFunctionPlanRequest(
                bindCall, bindOpaqueData,
                projectionIds,
                pushdownFilters,
                null,                       // join_keys — not known until per-split redemption
                null,                       // row_limit
                config.targetSplitBytes(),
                config.minSplits(),
                (long) cap,
                cursor,
                null,                       // refined_filters — Phase 4
                true,                       // filters_complete — no dynamic filtering yet, so always complete
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
