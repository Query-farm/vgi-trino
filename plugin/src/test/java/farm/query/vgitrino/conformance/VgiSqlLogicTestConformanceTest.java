// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import farm.query.vgitrino.VgiConnectorFactory;
import farm.query.vgitrino.VgiPlugin;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the ACTUAL {@code .test} sqllogictest files from
 * {@code ~/Development/vgi/test/sql/integration/} against this connector —
 * not hand-ported equivalents, the real files, parsed by
 * {@link SqlLogicTestFile}, with their real ATTACH-derived catalog reference
 * rewritten to this Trino catalog's name and their real expected output
 * compared against a real Trino query result.
 *
 * <p>Most of the 327-file suite cannot run against Trino at all, for two
 * independent reasons neither of which this connector can fix by itself:
 * <ul>
 *   <li>DuckDB-only introspection surface — {@code duckdb_tables()},
 *       {@code duckdb_databases()}, {@code duckdb_constraints()},
 *       {@code duckdb_logs()}, {@code CALL enable_logging}, DuckDB's own
 *       {@code EXPLAIN (FORMAT JSON)} plan shape. These have no Trino
 *       equivalent, full stop — 119 of the 327 files reference at least one.</li>
 *   <li>DuckDB table-function CALL syntax ({@code schema.function(args)}).
 *       Trino has an equivalent SPI (polymorphic table functions,
 *       {@code TABLE(catalog.schema.fn(args))}) but this connector doesn't
 *       implement it yet — a separate, large piece of work. 161 of the 327
 *       files use call syntax somewhere.</li>
 * </ul>
 *
 * <p>{@code rowid.test} is mostly the portable remainder: plain
 * {@code SELECT}/{@code WHERE}/{@code ORDER BY} against declarative tables.
 * Its {@code DESCRIBE} line (DuckDB's 6-column shape has no Trino
 * equivalent), its struct-typed-rowid case (needs {@code ROW} type support
 * this connector's type mapper doesn't have — see {@code VgiTypeMapping}),
 * and its trailing {@code rowid_sequence(...)} calls (table-function syntax)
 * are skipped with a reported reason, not silently dropped — the assertion
 * at the end of this test is that the skip count is exactly what's expected
 * and every record this connector's functionality covers actually runs and
 * passes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class VgiSqlLogicTestConformanceTest {

    private static final String TRINO_CATALOG = "vgi_example";
    private static final String VGI_CATALOG_NAME = "example";

    private DistributedQueryRunner runner;
    private Session session;

    @BeforeAll
    void start() throws Exception {
        File vgiPython = new File(System.getProperty("user.home"), "Development/vgi-python");
        Assumptions.assumeTrue(vgiPython.isDirectory(),
                "~/Development/vgi-python not present — skipping sqllogictest conformance run");

        session = TestingSession.testSessionBuilder()
                .setCatalog(TRINO_CATALOG)
                .setSchema("data")
                .build();
        runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new VgiPlugin());
        runner.createCatalog(TRINO_CATALOG, VgiConnectorFactory.NAME, Map.of(
                "vgi.location", "uv run --project " + vgiPython.getAbsolutePath() + " vgi-fixture-worker",
                "vgi.catalog-name", VGI_CATALOG_NAME,
                "vgi.connections", "2"));
    }

    @AfterAll
    void stop() {
        if (runner != null) runner.close();
    }

    /**
     * Substrings that mark a record as needing something this connector (or
     * Trino itself) doesn't have — a curated denylist for this specific file,
     * not a general static analyzer. See the class javadoc for what each
     * category actually needs.
     */
    private static final List<String> NON_PORTABLE_MARKERS = List.of(
            "ATTACH ",           // DuckDB's own ATTACH syntax — this harness attaches via createCatalog instead
            "DESCRIBE",          // DuckDB's 6-column DESCRIBE shape has no Trino equivalent
            "rowid_struct",      // needs ROW-type support in VgiTypeMapping
            "rowid_sequence(");  // table-function CALL syntax — needs Trino's ConnectorTableFunction

    @Test
    @Timeout(180)
    void rowIdColumnsMatchTheRealTestFile() throws Exception {
        Path testFile = Path.of(System.getProperty("user.home"),
                "Development/vgi/test/sql/integration/table/rowid.test");
        Assumptions.assumeTrue(testFile.toFile().isFile(),
                "~/Development/vgi/test/sql/integration/table/rowid.test not present");

        List<SqlLogicTestFile.Record> records = SqlLogicTestFile.parse(testFile);
        int executed = 0;
        int skipped = 0;
        List<String> failures = new ArrayList<>();

        for (SqlLogicTestFile.Record record : records) {
            if (record.kind() != SqlLogicTestFile.Kind.QUERY
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_OK
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_ERROR) {
                continue;
            }
            String sql = String.join("\n", record.sql());
            if (sql.isBlank()) continue;

            boolean nonPortable = NON_PORTABLE_MARKERS.stream().anyMatch(sql::contains);
            if (nonPortable) {
                skipped++;
                continue;
            }

            String trinoSql = sql.replace("example.", TRINO_CATALOG + ".").strip();
            trinoSql = trinoSql.endsWith(";") ? trinoSql.substring(0, trinoSql.length() - 1) : trinoSql;

            try {
                switch (record.kind()) {
                    case QUERY -> {
                        MaterializedResult result = runner.execute(session, trinoSql);
                        List<List<String>> actual = new ArrayList<>();
                        for (MaterializedRow row : result.getMaterializedRows()) {
                            List<String> cells = new ArrayList<>(row.getFieldCount());
                            for (int i = 0; i < row.getFieldCount(); i++) {
                                Object v = row.getField(i);
                                cells.add(v == null ? "NULL" : v.toString());
                            }
                            actual.add(cells);
                        }
                        if (!actual.equals(record.expectedRows())) {
                            failures.add("QUERY mismatch for:\n" + trinoSql
                                    + "\nexpected: " + record.expectedRows()
                                    + "\nactual:   " + actual);
                        } else {
                            executed++;
                        }
                    }
                    case STATEMENT_OK -> {
                        runner.execute(session, trinoSql);
                        executed++;
                    }
                    case STATEMENT_ERROR -> {
                        try {
                            runner.execute(session, trinoSql);
                            failures.add("expected an error for:\n" + trinoSql);
                        } catch (RuntimeException e) {
                            // A DuckDB-specific error-message substring isn't
                            // expected to match Trino's own wording — the
                            // meaningful assertion here is just "it failed".
                            executed++;
                        }
                    }
                    default -> { }
                }
            } catch (RuntimeException e) {
                failures.add("unexpected failure for:\n" + trinoSql + "\n" + e);
            }
        }

        if (!failures.isEmpty()) {
            fail(executed + " executed, " + skipped + " skipped, " + failures.size()
                    + " FAILED:\n" + String.join("\n---\n", failures));
        }
        // rowid.test has 8 non-portable records (the ATTACH this harness
        // replaces with createCatalog, DESCRIBE, the struct-rowid pair, and
        // the 4 rowid_sequence() statements) — assert the skip count so a
        // future edit to this file's portable content, or a regression that
        // silently starts skipping MORE than expected, shows up here rather
        // than as a quietly-shrinking executed count.
        assertEquals(8, skipped, "expected skip count changed — see NON_PORTABLE_MARKERS");
        assertEquals(8, executed, "expected executed-record count changed");
    }
}
