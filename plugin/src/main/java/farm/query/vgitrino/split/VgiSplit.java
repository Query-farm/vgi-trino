// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

/**
 * One unit of a VGI scan: either a real, redeemable split (non-empty
 * {@code token}) or the "whole scan, not split-capable" sentinel VGI's
 * framework returns for functions that never opted into splitting (empty
 * {@code token} — see {@code VgiServiceImpl}'s own doc comment on this exact
 * convention). {@link farm.query.vgitrino.page.VgiPageSource} branches on
 * that emptiness: a non-empty token redeems via {@code init()}'s
 * {@code split_tokens}; an empty one calls {@code init()} with no split
 * tokens at all, an ordinary primary scan.
 *
 * <p>A plain record — see {@link farm.query.vgitrino.metadata.VgiTableHandle}
 * for why no Jackson annotations are needed.
 *
 * @param bindCall the serialised {@code BindRequest} this split's scan was bound with
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param token the split's redemption token, or empty for the not-split-capable sentinel
 * @param estimatedBytes this split's estimated size, or 0 if unknown
 * @param targetSplitBytesHint the {@code target_split_bytes} this scan's plan
 *        request asked for, used to normalise {@link #getSplitWeight()}; 0 if
 *        the client asked for no particular size
 * @param addresses hosts where this split is cheap to read, from
 *        {@code ScanSplit.location_ids}/{@code PlanResponse.locations}; empty
 *        when the worker named none
 */
public record VgiSplit(
        byte[] bindCall,
        byte[] bindOpaqueData,
        byte[] token,
        long estimatedBytes,
        long targetSplitBytesHint,
        List<String> addresses) implements ConnectorSplit {

    /** Reference split size used to normalise weight when no target size was
     *  requested — matches a common default target split size. */
    private static final long DEFAULT_REFERENCE_BYTES = 128L * 1024 * 1024;

    @Override
    public List<HostAddress> getAddresses() {
        return addresses.stream().map(HostAddress::fromString).toList();
    }

    @Override
    public SplitWeight getSplitWeight() {
        if (estimatedBytes <= 0) return SplitWeight.standard();
        long reference = targetSplitBytesHint > 0 ? targetSplitBytesHint : DEFAULT_REFERENCE_BYTES;
        double proportion = (double) estimatedBytes / reference;
        // Clamp: SplitWeight.fromProportion requires a positive value, and an
        // engine that bin-packs gets nothing useful from a split claimed to
        // weigh 10,000x a standard one.
        return SplitWeight.fromProportion(Math.min(100.0, Math.max(0.01, proportion)));
    }
}
