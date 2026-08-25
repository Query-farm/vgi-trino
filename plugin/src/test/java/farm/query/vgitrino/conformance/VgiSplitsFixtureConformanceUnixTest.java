// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/**
 * {@link VgiSplitsFixtureConformanceTest} over {@code unix://} transport — the first real exercise
 * anywhere in this test tree of ONE real Python worker process serving many concurrent pooled
 * connections (both catalogs' 16-connection pools attach to the same worker instance), rather than
 * subprocess transport's one-interpreter-per-connection model.
 */
final class VgiSplitsFixtureConformanceUnixTest extends VgiSplitsFixtureConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() throws Exception {
        return VgiWorkerHarness.unix(VGI_PYTHON);
    }
}
