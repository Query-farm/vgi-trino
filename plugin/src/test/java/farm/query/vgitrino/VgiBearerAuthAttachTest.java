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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Hand-authored counterpart to {@code bearer_auth/bearer_token.test} — not a
 * file-replay (Trino's catalog credentials live in {@code createCatalog}'s
 * properties map, not an inline DuckDB {@code ATTACH ... bearer_token '...'}
 * option, so there's no SQL text to replay), but the same three real
 * scenarios: a correct token attaches and serves queries, a wrong token fails
 * {@code catalog_attach} outright, and no token at all against a
 * bearer-protected server fails cleanly rather than hanging or mis-serving.
 *
 * <p>The file's fourth scenario ({@code bearer_token}/{@code
 * oauth_refresh_token} mutual exclusivity) is a DuckDB ATTACH-option
 * validation with no vgi-trino equivalent (this connector has no {@code
 * oauth_refresh_token} config at all) and isn't ported. Its scalar-function
 * assertion ({@code SELECT example.double(21)}) is also skipped — scalar
 * functions are a stated non-goal — in favor of a plain table read, which
 * exercises the same authenticated-connection path.
 *
 * <p>Starts its own {@code vgi-fixture-http} with {@code VGI_BEARER_TOKENS}
 * set (per vgi-python's own documented static-bearer-auth env var), the same
 * port-file discovery pattern {@link VgiHttpTransportQueryRunnerTest} uses.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiBearerAuthAttachTest {

    private static final String CORRECT_TOKEN = "vgi-trino-test-token";

    private Process httpServer;
    private String location;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping bearer-auth test");

        Path portFile = Files.createTempFile("vgi-trino-bearer-test-", ".port");
        Files.deleteIfExists(portFile);
        ProcessBuilder builder = new ProcessBuilder(
                "uv", "run", "--project", vgiPython.getAbsolutePath(),
                "vgi-fixture-http", "--port", "0", "--port-file", portFile.toString())
                .directory(vgiPython)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD);
        // "token=principal" pairs, per vgi-python's documented static bearer-auth env var.
        builder.environment().put("VGI_BEARER_TOKENS", CORRECT_TOKEN + "=tester");
        httpServer = builder.start();

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
        location = "http://127.0.0.1:" + port;
    }

    @AfterAll
    void stop() throws IOException {
        if (httpServer != null && httpServer.isAlive()) {
            httpServer.destroy();
        }
    }

    @Test
    @Timeout(60)
    void correctBearerTokenAttachesAndServesQueries() throws Exception {
        try (DistributedQueryRunner runner = DistributedQueryRunner
                .builder(TestingSession.testSessionBuilder().build())
                .setWorkerCount(1)
                .build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_bearer_ok", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", location,
                    "vgi.catalog-name", "example",
                    "vgi.http-bearer-token", CORRECT_TOKEN));

            Session session = TestingSession.testSessionBuilder()
                    .setCatalog("vgi_bearer_ok").setSchema("data").build();
            MaterializedResult result = runner.execute(session, "SELECT count(*) FROM numbers");
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertEquals(100L, row.getField(0));
        }
    }

    @Test
    @Timeout(60)
    void wrongBearerTokenFailsAttach() throws Exception {
        try (DistributedQueryRunner runner = DistributedQueryRunner
                .builder(TestingSession.testSessionBuilder().build())
                .setWorkerCount(1)
                .build()) {
            runner.installPlugin(new VgiPlugin());
            // catalog_attach happens eagerly (VgiWorkerClient opens and attaches every
            // pooled connection in createCatalog's own call), so a rejected token fails
            // right here — the ported file's own point ("the diagnostic arrives at
            // ATTACH, not at the first query").
            assertThrows(RuntimeException.class, () ->
                    runner.createCatalog("vgi_bearer_wrong", VgiConnectorFactory.NAME, Map.of(
                            "vgi.location", location,
                            "vgi.catalog-name", "example",
                            "vgi.http-bearer-token", "wrong-token")));
        }
    }

    @Test
    @Timeout(60)
    void missingBearerTokenFailsAttach() throws Exception {
        try (DistributedQueryRunner runner = DistributedQueryRunner
                .builder(TestingSession.testSessionBuilder().build())
                .setWorkerCount(1)
                .build()) {
            runner.installPlugin(new VgiPlugin());
            // No vgi.http-bearer-token at all against a bearer-protected server —
            // the ported file's point is this must fail cleanly at attach, not
            // hang or silently serve unauthenticated.
            assertThrows(RuntimeException.class, () ->
                    runner.createCatalog("vgi_bearer_none", VgiConnectorFactory.NAME, Map.of(
                            "vgi.location", location,
                            "vgi.catalog-name", "example")));
        }
    }
}
