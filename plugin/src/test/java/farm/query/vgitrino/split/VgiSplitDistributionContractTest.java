// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgi.protocol.PlanResponse;
import farm.query.vgi.protocol.ScanSplit;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class VgiSplitDistributionContractTest {

    @Test
    void acceptsMatchedKeyAndRangeMetadata() {
        PlanResponse plan = plan(new byte[] {1}, new byte[] {2});
        ScanSplit split = split(new byte[] {3}, 1L, new byte[] {9});

        assertDoesNotThrow(() -> VgiSplitSource.validateSplitDistribution(plan, split));
    }

    @Test
    void rejectsMissingPerSplitKeyValue() {
        PlanResponse plan = plan(new byte[] {1}, null);
        ScanSplit split = split(null, null, new byte[] {9});

        assertThrows(RuntimeException.class,
                () -> VgiSplitSource.validateSplitDistribution(plan, split));
    }

    @Test
    void rejectsRangeIndexWithoutPlanRangeMetadata() {
        PlanResponse plan = plan(null, null);
        ScanSplit split = split(null, 0L, new byte[] {9});

        assertThrows(RuntimeException.class,
                () -> VgiSplitSource.validateSplitDistribution(plan, split));
    }

    @Test
    void rejectsDistributionOnUnsplitSentinel() {
        PlanResponse plan = plan(new byte[] {1}, null);
        ScanSplit sentinel = split(new byte[] {2}, null, new byte[0]);

        assertThrows(RuntimeException.class,
                () -> VgiSplitSource.validateSplitDistribution(plan, sentinel));
    }

    private static PlanResponse plan(byte[] keyGrouping, byte[] rangePartitioning) {
        return new PlanResponse(
                List.of(), List.of(), null, null, null, null, null, null, null,
                "catalog", null, keyGrouping, rangePartitioning, List.of(), List.of(),
                null, null, null);
    }

    private static ScanSplit split(byte[] keyValue, Long rangeIndex, byte[] token) {
        return new ScanSplit(
                new byte[0], token, null, false, null, keyValue, rangeIndex,
                null, null, null, null, null, null);
    }
}
