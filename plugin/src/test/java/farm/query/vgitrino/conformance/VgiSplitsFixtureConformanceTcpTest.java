// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/**
 * {@link VgiSplitsFixtureConformanceTest} over {@code tcp://} transport, against the real reference
 * Python fixture worker (not the hand-rolled Java TCP fixtures elsewhere in this test tree) — same
 * one-worker-many-pooled-connections shape as {@link VgiSplitsFixtureConformanceUnixTest}.
 */
final class VgiSplitsFixtureConformanceTcpTest extends VgiSplitsFixtureConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() throws Exception {
        return VgiWorkerHarness.tcp(VGI_PYTHON);
    }
}
