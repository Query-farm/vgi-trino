// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Replays a real {@code .test} file's {@link SqlLogicTestFile.Record}s against
 * a live {@link DistributedQueryRunner}, skipping records a curated marker
 * list identifies as non-portable (DuckDB-only syntax/introspection, or a
 * feature this connector doesn't implement yet) rather than transcribing each
 * file's assertions into hand-written Java.
 *
 * <p>Extracted from {@link VgiSqlLogicTestConformanceTest}'s original single
 * use so more of {@code ~/Development/vgi/test/sql/integration/}'s files can
 * reuse the same replay logic instead of duplicating it per file.
 */
final class SqlLogicTestRunner {

    private SqlLogicTestRunner() {}

    /** Outcome of replaying one file: how many records ran, how many were skipped as non-portable,
     *  and — if non-empty — the mismatches/errors that should fail the test. */
    record Result(int executed, int skipped, List<String> failures) {}

    /**
     * @param runner the query runner to execute against
     * @param session the session to execute under (selects the Trino catalog/schema)
     * @param testFile the real {@code .test} file to replay
     * @param vgiCatalogRef the VGI-side catalog reference the file's SQL uses (e.g. {@code "example."})
     * @param trinoCatalog the Trino catalog name to rewrite {@code vgiCatalogRef} to (e.g. {@code "vgi_example"})
     * @param nonPortableMarkers substrings that mark a record as needing something this connector (or Trino
     *        itself) doesn't have — skipped rather than executed
     */
    static Result run(DistributedQueryRunner runner, Session session, Path testFile,
            String vgiCatalogRef, String trinoCatalog, List<String> nonPortableMarkers) throws IOException {
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

            boolean nonPortable = nonPortableMarkers.stream().anyMatch(sql::contains);
            if (nonPortable) {
                skipped++;
                continue;
            }

            String trinoSql = sql.replace(vgiCatalogRef, trinoCatalog + ".").strip();
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
        return new Result(executed, skipped, List.copyOf(failures));
    }
}
