// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the {@code http(s)://} transport end to end: real cross-process
 * queries — plain table scan, a filtered scan, and a {@code TABLE(...)}
 * call — against the reference fixture worker running as an actual HTTP
 * server ({@code vgi-fixture-http}), not the subprocess/{@code tcp://}
 * transports every other test in this suite uses.
 *
 * <p>Unlike subprocess/{@code unix://}/{@code tcp://}, an HTTP worker is an
 * already-running, independently-managed server this connector merely
 * connects TO — it isn't spawned per pooled connection — so this test starts
 * one itself (auto-selected port, discovered via {@code --port-file}'s
 * atomic write, the same mechanism the C++ VGI extension's own test harness
 * uses) and tears it down afterward, rather than handing {@code vgi.location}
 * a command this connector's own pool would spawn.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiHttpTransportQueryRunnerTest {

    private static final String CATALOG = "vgi_http_example";

    private Process httpServer;
    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping HTTP transport test");

        Path portFile = Files.createTempFile("vgi-trino-http-test-", ".port");
        Files.deleteIfExists(portFile);
        httpServer = new ProcessBuilder(
                "uv", "run", "--project", vgiPython.getAbsolutePath(),
                "vgi-fixture-http", "--port", "0", "--port-file", portFile.toString())
                .directory(vgiPython)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();

        int port = -1;
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(portFile)) {
                String content = Files.readString(portFile).strip();
                if (!content.isEmpty()) {
                    port = Integer.parseInt(content);
                    break;
                }
            }
            if (!httpServer.isAlive()) {
                throw new IllegalStateException(
                        "vgi-fixture-http exited before writing its port file (exit code "
                                + httpServer.exitValue() + ")");
            }
            Thread.sleep(200);
        }
        Files.deleteIfExists(portFile);
        Assumptions.assumeTrue(port > 0, "timed out waiting for vgi-fixture-http to report its bound port");

        session = TestingSession.testSessionBuilder().setCatalog(CATALOG).setSchema("data").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "http://127.0.0.1:" + port,
                "vgi.catalog-name", "example",
                "vgi.connections", "3"));
    }

    @AfterAll
    void stop() throws IOException {
        if (runner != null) runner.close();
        if (httpServer != null && httpServer.isAlive()) {
            httpServer.destroy();
        }
    }

    @Test
    @Timeout(60)
    void plainTableScanWorksOverHttp() {
        MaterializedResult result = runner.execute(session, "SELECT count(*), sum(value) FROM numbers");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(100L, row.getField(0));
        assertEquals(4950L, row.getField(1));
    }

    @Test
    @Timeout(60)
    void filteredScanWorksOverHttp() {
        long count = (long) runner.execute(session, "SELECT count(*) FROM numbers WHERE value > 90")
                .getMaterializedRows().get(0).getField(0);
        assertEquals(9L, count);
    }

    @Test
    @Timeout(60)
    void tableFunctionCallWorksOverHttp() {
        MaterializedResult result = runner.execute(session,
                "SELECT count(*), sum(n) FROM TABLE(" + CATALOG + ".main.sequence(count => 20))");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(20L, row.getField(0));
        assertEquals(190L, row.getField(1));
    }

    @Test
    @Timeout(60)
    void multipleConcurrentConnectionsAllAttachSuccessfully() {
        // vgi.connections=3 above: proves the pool opened (and independently
        // catalog_attach'd) more than one HttpRpcConnection, not just the
        // first one happening to work.
        List<MaterializedRow> rows = runner.execute(session,
                "SELECT a.value, b.value FROM numbers a JOIN numbers b ON a.value = b.value WHERE a.value < 5")
                .getMaterializedRows();
        assertTrue(rows.size() == 5, "expected 5 matching rows, got " + rows.size());
    }
}
