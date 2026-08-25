// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.testing;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Smoke-tests {@link VgiWorkerHarness} itself: each factory really starts a worker this connector
 *  can attach to and query, before the larger conformance-suite refactor depends on it. */
final class VgiWorkerHarnessTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");

    @Test
    @Timeout(60)
    void unixHarnessWorks() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        VgiWorkerHarness.Handle handle = VgiWorkerHarness.unix(VGI_PYTHON);
        try {
            assertQueryable(handle.location());
        } finally {
            handle.teardown().close();
        }
    }

    @Test
    @Timeout(60)
    void tcpHarnessWorks() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        VgiWorkerHarness.Handle handle = VgiWorkerHarness.tcp(VGI_PYTHON);
        try {
            assertQueryable(handle.location());
        } finally {
            handle.teardown().close();
        }
    }

    @Test
    @Timeout(60)
    void httpHarnessWorks() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(), "~/Development/vgi-python not present");
        VgiWorkerHarness.Handle handle = VgiWorkerHarness.http(VGI_PYTHON);
        try {
            assertQueryable(handle.location());
        } finally {
            handle.teardown().close();
        }
    }

    private static void assertQueryable(String location) throws Exception {
        Session session = TestingSession.testSessionBuilder().setCatalog("vgi_harness").setSchema("data").build();
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_harness", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", location,
                    "vgi.catalog-name", "example"));
            MaterializedResult result = runner.execute(session, "SELECT count(*) FROM numbers");
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(100L, row.getField(0));
        }
    }
}
