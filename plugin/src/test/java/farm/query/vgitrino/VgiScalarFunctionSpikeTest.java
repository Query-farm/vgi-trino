// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SPIKE — does Trino's connector-defined scalar-function SPI actually dispatch a real call to a
 * VGI worker? See {@code farm.query.vgitrino.function.VgiScalarFunctionSpike}'s javadoc for the
 * full design and its deliberate limitations. This is the empirical answer.
 */
final class VgiScalarFunctionSpikeTest {

    @Test
    @Timeout(60)
    void realVgiScalarDispatchesEndToEnd() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping scalar function spike");

        Session session = TestingSession.testSessionBuilder().setCatalog("vgi_example").setSchema("main").build();
        try (DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build()) {
            runner.installPlugin(new VgiPlugin());
            runner.createCatalog("vgi_example", VgiConnectorFactory.NAME, Map.of(
                    "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                    "vgi.catalog-name", "example"));

            MaterializedResult result = runner.execute(session, "SELECT vgi_example.main.passthru('hello spike')");
            assertEquals("hello spike", result.getMaterializedRows().get(0).getField(0));
        }
    }
}
