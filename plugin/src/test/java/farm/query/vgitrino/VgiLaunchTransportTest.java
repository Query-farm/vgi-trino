// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgirpc.launcher.PosixLauncherSupport;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves {@code launch:} end to end against the REAL reference Python fixture
 * worker (not a Java-only fixture) — a genuine cross-language exercise of the
 * shared-warm-worker transport: {@link farm.query.vgirpc.launcher.LauncherClient}
 * spawns {@code vgi-fixture-worker --unix <path> --idle-timeout <secs>} (the exact
 * CLI contract {@code docs/launcher-protocol.md} specifies), and every one of this
 * catalog's pooled connections attaches to that SAME worker process, sharing one
 * Python interpreter — the property {@code VgiLaunchTransportConformanceTest}-style
 * suites in {@code vgi-rpc-java} already prove at the transport-client level; this
 * test is the one confirming the connector actually wires it up correctly end to end.
 */
final class VgiLaunchTransportTest {

    @Test
    @Timeout(15)
    void posixLauncherSupportOverlayIsActiveOnThisRuntime() {
        // A build-plumbing sanity check as much as a functional one: vgi-rpc-java's
        // launcher package ships a Java-21 baseline (launch: unsupported,
        // available()==false) plus a Java-22 FFM overlay in the same JAR's
        // META-INF/versions/22. This connector requires JDK 25 (see the README), so
        // the overlay must be the one actually loaded here — if this assertion ever
        // flips false, something in the dependency chain started serving the
        // baseline classes instead of the real multi-release JAR.
        assertTrue(PosixLauncherSupport.available(),
                "expected the real (FFM-backed) launcher support on this JDK, not the JDK21 baseline stub");
    }

    @Test
    @Timeout(60)
    void launchTransportWorksAgainstTheRealPythonWorker() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping launch: transport test");

        String location = "launch:uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker";
        Session session = TestingSession.testSessionBuilder()
                .setCatalog("vgi_launch").setSchema("data").build();
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_launch", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", location,
                    "vgi.catalog-name", "example",
                    // Several pooled connections attaching to the SAME launch: tuple —
                    // they must all resolve to the one shared worker, not spawn N.
                    "vgi.connections", "4"));

            MaterializedResult result = runner.execute(session, "SELECT count(*), sum(value) FROM numbers");
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(100L, row.getField(0));
            assertEquals(4950L, row.getField(1));
        }
    }
}
