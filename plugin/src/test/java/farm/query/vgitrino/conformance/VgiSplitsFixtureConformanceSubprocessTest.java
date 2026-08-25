// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/** {@link VgiSplitsFixtureConformanceTest} over subprocess transport (this connector's default). */
final class VgiSplitsFixtureConformanceSubprocessTest extends VgiSplitsFixtureConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() {
        return VgiWorkerHarness.subprocess(VGI_PYTHON);
    }
}
