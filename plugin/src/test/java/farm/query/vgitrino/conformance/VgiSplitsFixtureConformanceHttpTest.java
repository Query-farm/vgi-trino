// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/** {@link VgiSplitsFixtureConformanceTest} over {@code http(s)://} transport, against the real
 *  {@code vgi-fixture-http} server. */
final class VgiSplitsFixtureConformanceHttpTest extends VgiSplitsFixtureConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() throws Exception {
        return VgiWorkerHarness.http(VGI_PYTHON);
    }
}
