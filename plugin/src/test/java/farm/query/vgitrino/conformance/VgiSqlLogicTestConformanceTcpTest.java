// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.testing.VgiWorkerHarness;

/** {@link VgiSqlLogicTestConformanceTest} over {@code tcp://} transport, against the real reference
 *  Python fixture worker. */
final class VgiSqlLogicTestConformanceTcpTest extends VgiSqlLogicTestConformanceTest {
    @Override
    VgiWorkerHarness.Handle startWorker() throws Exception {
        return VgiWorkerHarness.tcp(VGI_PYTHON);
    }
}
