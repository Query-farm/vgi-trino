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
            trinoSql = rewriteDuckDbOnlySyntax(trinoSql, trinoCatalog);

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

    /**
     * Rewrites the two DuckDB-only syntax forms that dominate the sqllogictest census's
     * PARSE_ERROR bucket into their Trino equivalents, so the parser failures that remain
     * actually mean something (a genuine gap) rather than "the test harness never translated
     * the SQL at all":
     * <ul>
     *   <li>DuckDB's bare table-function CALL syntax — {@code FROM schema.function(args)} used
     *       directly as a table reference — becomes Trino's required {@code FROM
     *       TABLE(schema.function(args))}. Only rewrites calls immediately qualified by {@code
     *       trinoCatalog} (i.e. ones this rewrite already routed through the catalog-name
     *       substitution above) that aren't already wrapped in {@code TABLE(...)}.</li>
     *   <li>DuckDB's {@code name := value} named-argument syntax becomes Trino's {@code name =>
     *       value}.</li>
     * </ul>
     *
     * <p>This is a best-effort textual rewrite, not a SQL parser: it only recognizes a table
     * function immediately following a {@code FROM}/{@code JOIN} keyword (the pattern actually
     * observed in the real fixture files), and skips a paren-matching scan that respects
     * single-quoted string literals so an argument containing {@code )} or {@code (} doesn't
     * break it. Comma-joined bare calls with no {@code FROM}/{@code JOIN} immediately before them
     * are not handled — not observed in the corpus, and a real parser would be needed to do it
     * robustly.
     */
    static String rewriteDuckDbOnlySyntax(String sql, String trinoCatalog) {
        return wrapBareTableFunctionCalls(sql, trinoCatalog).replace(":=", "=>");
    }

    private static String wrapBareTableFunctionCalls(String sql, String trinoCatalog) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = sql.length();
        while (i < n) {
            int afterKeyword = matchKeyword(sql, i, "FROM");
            if (afterKeyword < 0) afterKeyword = matchKeyword(sql, i, "JOIN");
            if (afterKeyword < 0) {
                out.append(sql.charAt(i));
                i++;
                continue;
            }
            out.append(sql, i, afterKeyword);
            int j = afterKeyword;
            while (j < n && Character.isWhitespace(sql.charAt(j))) j++;
            out.append(sql, afterKeyword, j);
            i = j;

            if (matchKeyword(sql, i, "TABLE") >= 0) {
                continue; // already wrapped (or a TABLESAMPLE-style keyword) — leave it alone
            }
            String prefix = trinoCatalog + ".";
            if (!sql.startsWith(prefix, i)) {
                continue;
            }
            int p = i + prefix.length();
            int schemaStart = p;
            while (p < n && isIdentChar(sql.charAt(p))) p++;
            if (p == schemaStart || p >= n || sql.charAt(p) != '.') continue;
            p++; // skip the schema/function separator dot
            int funcStart = p;
            while (p < n && isIdentChar(sql.charAt(p))) p++;
            if (p == funcStart || p >= n || sql.charAt(p) != '(') continue;

            int closeParen = findMatchingParen(sql, p);
            if (closeParen < 0) continue;

            String call = sql.substring(i, closeParen + 1);
            out.append("TABLE(").append(call).append(")");
            i = closeParen + 1;
        }
        return out.toString();
    }

    /** Matches {@code keyword} as a whole word at {@code i}; returns the index just past it, or -1. */
    private static int matchKeyword(String s, int i, String keyword) {
        int len = keyword.length();
        if (i + len > s.length() || !s.regionMatches(true, i, keyword, 0, len)) return -1;
        if (i > 0 && isIdentChar(s.charAt(i - 1))) return -1;
        if (i + len < s.length() && isIdentChar(s.charAt(i + len))) return -1;
        return i + len;
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    /** Finds the {@code )} matching the {@code (} at {@code openIndex}, skipping over single-quoted
     *  string literals (with {@code ''} as the escaped-quote form) so parens inside a string argument
     *  don't unbalance the scan. Returns -1 if the parens never balance. */
    private static int findMatchingParen(String s, int openIndex) {
        int depth = 0;
        boolean inString = false;
        for (int k = openIndex; k < s.length(); k++) {
            char c = s.charAt(k);
            if (inString) {
                if (c == '\'') {
                    if (k + 1 < s.length() && s.charAt(k + 1) == '\'') { k++; continue; }
                    inString = false;
                }
                continue;
            }
            if (c == '\'') { inString = true; continue; }
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return k;
            }
        }
        return -1;
    }
}
