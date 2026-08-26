// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
import farm.query.vgitrino.testing.VgiWorkerHarness;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs hand-written, genuinely Trino-native {@code .trino.test} files from {@code
 * plugin/src/test/resources/trino-adaptations/} — the companion to {@link
 * VgiSqlLogicTestConformanceTest}'s real-DuckDB-file replay, for the cases that replay
 * fundamentally cannot reach.
 *
 * <p><b>When to add a {@code .trino.test} file, and when not to.</b> The upstream {@code .test}
 * corpus ({@code ~/Development/vgi/test/sql/integration/}) is the real, evolving, cross-SDK
 * conformance suite — {@link SqlLogicTestRunner}'s rewrite pipeline (catalog rename, {@code
 * TABLE(...)} wrapping, {@code ::}-cast rewriting, etc.) keeps as much of it running against Trino
 * as a textual rewrite honestly can, which means any new upstream test case is covered here for
 * free the moment it's added there — a real, deliberate property this connector's test strategy
 * has leaned on throughout (see the README). A hand-written adaptation gives up that "for free"
 * property for whatever it covers, so it earns its keep only when BOTH:
 * <ul>
 *   <li>the underlying DuckDB test exercises a real, ALREADY-WORKING piece of this connector
 *       (not a missing feature — {@code echo}/{@code constant_columns}/{@code vgi_clamp} (a VGI
 *       {@code macro}, an entirely unimplemented function kind) /{@code row_sum} (a {@code
 *       table_in_out} function, also unimplemented) need real connector work, not a test file),
 *       AND</li>
 *   <li>there's a genuinely different, Trino-expressible way to test the SAME underlying
 *       behavior — not just a syntax problem the rewrite pipeline could eventually solve.</li>
 * </ul>
 *
 * <p>The clearest case so far is the confirmed Trino PTF SPI ceiling (see the README's "Predicate
 * pushdown" scope note): a table function that echoes back what filter/order/column info it
 * received for testing purposes ({@code filter_echo}, {@code order_echo}, and siblings) can never
 * have that echo verified through Trino — {@code ConnectorTableFunctionHandle} carries no filter,
 * ORDER BY/LIMIT reach the connector through no hook at all — so the echoed value is always {@code
 * "(none)"}/{@code -1}, by construction, no matter what the query does. What's still genuinely
 * worth testing is that the underlying scan + Trino's own engine-level filter/sort/limit still
 * produce the CORRECT rows regardless — a real correctness question a hand-written adaptation can
 * answer, and the DuckDB original (with its unreachable echo assertions) cannot even be attempted.
 *
 * <p>Unlike {@link VgiSqlLogicTestConformanceTest}'s two pinned files (asserting an exact,
 * hand-curated skip count against a file this project doesn't own), these files are hand-authored
 * specifically for this connector — every record is expected to pass outright, with zero skips, so
 * this is a strict gate: any failure here is either a real regression or a stale adaptation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiTrinoAdaptationsTest {

    private static final File VGI_PYTHON = new File(System.getProperty("user.home"), "Development/vgi-python");
    private static final String TRINO_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    private VgiWorkerHarness.Handle worker;
    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        Assumptions.assumeTrue(VGI_PYTHON.isDirectory(),
                "~/Development/vgi-python not present — skipping Trino-adaptation tests");
        worker = VgiWorkerHarness.subprocess(VGI_PYTHON);

        session = TestingSession.testSessionBuilder().setCatalog(TRINO_CATALOG).setSchema("main").build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(TRINO_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", worker.location(),
                "vgi.catalog-name", VGI_CATALOG_NAME,
                "vgi.connections", "4"));
    }

    @AfterAll
    void stop() throws Exception {
        if (runner != null) runner.close();
        if (worker != null) worker.teardown().close();
    }

    /** One {@link DynamicTest} per {@code .trino.test} file found under {@code trino-adaptations/}
     *  on the test classpath — each asserts zero failures for every record in that file. */
    @TestFactory
    Stream<DynamicTest> adaptations() throws Exception {
        Path root = adaptationsRoot();
        List<Path> files;
        try (Stream<Path> walk = Files.walk(root)) {
            files = walk.filter(p -> p.toString().endsWith(".trino.test")).sorted().toList();
        }
        return files.stream().map(file -> DynamicTest.dynamicTest(root.relativize(file).toString(), () -> {
            SqlLogicTestRunner.Result result = SqlLogicTestRunner.runNative(runner, session, file);
            if (!result.failures().isEmpty()) {
                fail(result.executed() + " executed, " + result.failures().size() + " FAILED:\n"
                        + String.join("\n---\n", result.failures()));
            }
        }));
    }

    private static Path adaptationsRoot() throws URISyntaxException {
        URL url = VgiTrinoAdaptationsTest.class.getClassLoader().getResource("trino-adaptations");
        if (url == null) {
            throw new IllegalStateException(
                    "trino-adaptations resource root not found on the test classpath — "
                            + "expected plugin/src/test/resources/trino-adaptations/ to exist");
        }
        return Path.of(url.toURI());
    }
}
