// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal parser for the DuckDB-flavoured sqllogictest {@code .test} files
 * under {@code ~/Development/vgi/test/sql/integration/}: enough of the format
 * to drive real records from real files against Trino, not a general-purpose
 * implementation of every directive that format supports.
 *
 * <p>Understands {@code statement ok}, {@code statement error}, and
 * {@code query <types>} records: a directive line, SQL text on the following
 * lines up to either a blank line/EOF or a {@code ----} separator, and — after
 * a separator — expected output: one line per row, tab-separated columns
 * (this codebase's own sqllogictest dialect, not the one-value-per-line
 * SQLite original), until a blank line or EOF. Everything else (comments,
 * {@code require}/{@code require-env}, {@code statement ok} bodies that are
 * themselves {@code SET}s, ...) is either skipped or returned as an opaque
 * {@link Record} for the runner to decide about.
 */
final class SqlLogicTestFile {

    private SqlLogicTestFile() {}

    /** One parsed record from a {@code .test} file. */
    record Record(Kind kind, String directiveLine, List<String> sql, List<List<String>> expectedRows,
                  String expectedErrorSubstring) {}

    enum Kind { STATEMENT_OK, STATEMENT_ERROR, QUERY, OTHER }

    /**
     * Parse a {@code .test} file into its records.
     *
     * @param path the file to parse
     * @return the records, in file order
     * @throws IOException if the file cannot be read
     */
    static List<Record> parse(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path);
        List<Record> out = new ArrayList<>();
        int i = 0;
        while (i < lines.size()) {
            String line = lines.get(i).strip();
            if (line.isEmpty() || line.startsWith("#")) {
                i++;
                continue;
            }
            Kind kind;
            if (line.equals("statement ok")) {
                kind = Kind.STATEMENT_OK;
            } else if (line.startsWith("statement error")) {
                kind = Kind.STATEMENT_ERROR;
            } else if (line.startsWith("query ")) {
                kind = Kind.QUERY;
            } else {
                // require-env / require / mode / halt / loop directives, and
                // anything else this minimal parser doesn't need to act on.
                out.add(new Record(Kind.OTHER, line, List.of(), List.of(), null));
                i++;
                continue;
            }
            String directiveLine = line;
            i++;
            List<String> sql = new ArrayList<>();
            while (i < lines.size() && !lines.get(i).strip().isEmpty()
                    && !lines.get(i).strip().equals("----")) {
                sql.add(lines.get(i));
                i++;
            }
            List<List<String>> expectedRows = new ArrayList<>();
            String expectedErrorSubstring = null;
            if (i < lines.size() && lines.get(i).strip().equals("----")) {
                i++;
                List<String> errorLines = new ArrayList<>();
                while (i < lines.size() && !lines.get(i).strip().isEmpty()) {
                    if (kind == Kind.QUERY) {
                        expectedRows.add(List.of(lines.get(i).split("\t", -1)));
                    } else {
                        errorLines.add(lines.get(i));
                    }
                    i++;
                }
                if (kind == Kind.STATEMENT_ERROR) {
                    expectedErrorSubstring = String.join("\n", errorLines);
                }
            }
            out.add(new Record(kind, directiveLine, List.copyOf(sql), List.copyOf(expectedRows),
                    expectedErrorSubstring));
        }
        return out;
    }
}
