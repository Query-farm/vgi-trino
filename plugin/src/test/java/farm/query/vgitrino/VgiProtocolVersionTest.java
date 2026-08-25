// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hand-authored counterpart to {@code protocol_version/version_mismatch.test}
 * — not a file-replay (the file is a single {@code statement error} against
 * an {@code ATTACH}, which has no Trino-syntax equivalent to replay), but the
 * same real assertion: a worker whose {@code protocol_version} doesn't match
 * at the major+minor level must fail the very first RPC, not silently mis-serve.
 *
 * <p>Runs the real {@code vgi-fixture-bad-protocol-worker} fixture (which
 * advertises {@code 99.0.0}), confirmed to exist as a console script in
 * {@code ~/Development/vgi-python}'s {@code vgi-fixtures} package — this is
 * an actual cross-language protocol-enforcement guard, not a Java-side check,
 * so exercising it end to end from Trino is exactly what the ported test file
 * itself proves for the C++ extension.
 */
final class VgiProtocolVersionTest {

    @Test
    @Timeout(60)
    void mismatchedProtocolVersionFailsAttachOutright() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping protocol-version test");

        try (DistributedQueryRunner runner = DistributedQueryRunner
                .builder(TestingSession.testSessionBuilder().build())
                .setWorkerCount(1)
                .build()) {
            runner.installPlugin(new VgiPlugin());

            RuntimeException failure = assertThrows(RuntimeException.class, () ->
                    runner.createCatalog("vgi_bad_protocol", VgiConnectorFactory.NAME, Map.of(
                            "vgi.location", "uv run --project " + vgiPython.getAbsolutePath()
                                    + " vgi-fixture-bad-protocol-worker",
                            "vgi.catalog-name", "example")));

            // The dispatch-boundary error should surface as a protocol-version
            // complaint, not some unrelated failure — same load-bearing
            // assertion the ported file makes ("protocol_version mismatch"),
            // just matched against whatever wording round-trips through this
            // stack's own exception chain rather than DuckDB's.
            String chain = causeChainMessages(failure);
            assertTrue(chain.toLowerCase().contains("protocol"),
                    "expected a protocol-version complaint somewhere in the cause chain, got: " + chain);
        }
    }

    private static String causeChainMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur).append('\n');
        }
        return sb.toString();
    }
}
