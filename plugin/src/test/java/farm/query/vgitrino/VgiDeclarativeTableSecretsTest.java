// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real, end-to-end coverage of a DECLARATIVE table's backing scan function's
 * {@code required_settings}/{@code required_secrets} — {@code
 * secret_demo_table} (schema {@code data}), scanned with a bare {@code SELECT
 * * FROM catalog.schema.name}, never a {@code TABLE(...)} call — against a
 * live {@code vgi-fixture-worker}. Mirrors {@code
 * secret/secret_function_backed_table.test} in the C++ integration corpus.
 *
 * <p>{@code secret_demo_table}'s backing function, {@code secret_demo}, is
 * the interesting case specifically BECAUSE its {@code on_bind} resolves the
 * {@code vgi_example} secret fully dynamically via {@code
 * SecretsAccessor.get()} — no static {@code Secret()}/{@code
 * Meta.required_secrets} declaration at all, confirmed live against the real
 * fixture ({@code resolve_metadata(SecretDemoFunction).required_secrets ==
 * []}). Wiring {@code FunctionInfo.required_settings}/{@code required_secrets}
 * into {@code VgiSplitManager}'s plain-table {@code getSplits} alone is NOT
 * enough to make this table return real data — it also needs {@code
 * VgiSplitManager}'s two-phase bind retry (triggered by a non-empty {@code
 * BindResponse.lookup_secret_types} on the first bind), which is what this
 * class actually exercises end to end. See {@code VgiSplitManager}'s own
 * javadoc, and the README's "Settings and secrets for declarative tables"
 * section, for the full trace.
 */
final class VgiDeclarativeTableSecretsTest {

    private static DistributedQueryRunner runner;
    private static Session session;

    @BeforeAll
    static void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping declarative table secret tests");

        session = TestingSession.testSessionBuilder().setCatalog("vgi_example").setSchema("data").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", "example"));
    }

    @AfterAll
    static void stop() {
        if (runner != null) runner.close();
    }

    /** {@code Session.builder(session)} with the {@code vgi_example} secret's fields supplied
     *  via {@code --extra-credential}, exactly mirroring {@code
     *  VgiScalarFunctionsTest#secretFieldReadsMultipleFieldsOfOneSecretFromExtraCredentials}. */
    private static Session withSecret() {
        return Session.builder(session)
                .setIdentity(Identity.forUser("test").withExtraCredentials(Map.of(
                        "vgi_secret.vgi_example.secret_string", "test_str",
                        "vgi_secret.vgi_example.api_key", "ak-456")).build())
                .build();
    }

    @Test
    @Timeout(60)
    void scanningTheTableWithoutASecretReturnsNoRows() {
        // No --extra-credential at all: the two-phase retry resolves nothing (the credential
        // genuinely isn't there), so the scan legitimately comes back empty — never an error.
        MaterializedResult result = runner.execute(session,
                "SELECT COUNT(*) FROM vgi_example.data.secret_demo_table");
        assertEquals(0L, result.getMaterializedRows().get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void scanningTheTableWithASecretReturnsRows() {
        MaterializedResult result = runner.execute(withSecret(),
                "SELECT COUNT(*) > 0 FROM vgi_example.data.secret_demo_table");
        assertTrue((Boolean) result.getMaterializedRows().get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void theSecretStringFieldSurfacesInTheScannedRows() {
        MaterializedResult result = runner.execute(withSecret(),
                "SELECT value FROM vgi_example.data.secret_demo_table WHERE key = 'secret_string'");
        assertEquals("test_str", result.getMaterializedRows().get(0).getField(0));
    }

    @Test
    @Timeout(60)
    void theApiKeyFieldSurfacesInTheScannedRows() {
        MaterializedResult result = runner.execute(withSecret(),
                "SELECT value FROM vgi_example.data.secret_demo_table WHERE key = 'api_key'");
        assertEquals("ak-456", result.getMaterializedRows().get(0).getField(0));
    }
}
