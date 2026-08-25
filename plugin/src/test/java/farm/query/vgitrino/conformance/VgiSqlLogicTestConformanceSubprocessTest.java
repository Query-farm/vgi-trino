// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/** {@link VgiSqlLogicTestConformanceTest} over subprocess transport (this connector's default). */
final class VgiSqlLogicTestConformanceSubprocessTest extends VgiSqlLogicTestConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() {
        return VgiWorkerHarness.subprocess(VGI_PYTHON);
    }
}
