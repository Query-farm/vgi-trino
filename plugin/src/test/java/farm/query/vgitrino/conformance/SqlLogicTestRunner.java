// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import io.trino.Session;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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

        // A file whose own header declares `require-env VGI_TEST_DEDICATED_WORKER` MUST be
        // skipped whole, not executed — confirmed the hard way: table_buffering_pool_recovery.test
        // and table_buffering_worker_crash.test both call crash_on_process(...), which SIGKILLs the
        // live worker PROCESS. Their own headers document this is "not safe under shared-worker
        // transports" and gate it behind this exact env var for that reason. This runner (unlike
        // the real DuckDB sqllogictest harness) otherwise parses every require/require-env line as
        // an opaque Kind.OTHER record and never acts on ANY of them — deliberately: every OTHER
        // require-env this corpus uses just selects which specialized worker/config a file targets,
        // and this connector always replays against the plain shared example fixture worker
        // regardless, so an unmet one of those simply fails its statements normally (already
        // accounted for in the census's ordinary failure buckets) rather than corrupting shared
        // state. VGI_TEST_DEDICATED_WORKER is the one exception: ignoring it let this connector's
        // whole shared worker POOL get SIGKILLed mid-census, permanently exhausting it and hanging
        // every subsequent table-in-out-family query for the rest of the run (confirmed via a live
        // jstack: the census's own client thread blocked forever reading an HTTP response for a
        // query whose server-side VgiTableInOutTableFunction.analyze() was itself blocked forever in
        // VgiWorkerClient.borrow() — a real ~46-minute total stall, not a slow query). This
        // environment never sets VGI_TEST_DEDICATED_WORKER (there is no dedicated-worker mode here),
        // so any file requiring it is skipped in full — mirroring how nonPortableMarkers already
        // skips a record rather than running it.
        boolean requiresDedicatedWorker = records.stream()
                .anyMatch(r -> r.kind() == SqlLogicTestFile.Kind.OTHER
                        && r.directiveLine().strip().equals("require-env VGI_TEST_DEDICATED_WORKER"));
        if (requiresDedicatedWorker && System.getenv("VGI_TEST_DEDICATED_WORKER") == null) {
            long executableRecordCount = records.stream()
                    .filter(r -> r.kind() == SqlLogicTestFile.Kind.QUERY
                            || r.kind() == SqlLogicTestFile.Kind.STATEMENT_OK
                            || r.kind() == SqlLogicTestFile.Kind.STATEMENT_ERROR)
                    .count();
            return new Result(0, (int) executableRecordCount, List.of());
        }

        // First pass: apply every rewrite EXCEPT the :: cast one, and collect the retained
        // (non-skipped) records so their SQL can be cast-rewritten as one batch — sqlglot is a
        // Python subprocess, and spawning one per record (rather than once per file) would be
        // needlessly slow across a 327-file, ~5000-record corpus.
        List<SqlLogicTestFile.Record> retainedRecords = new ArrayList<>();
        List<String> preCastSql = new ArrayList<>();
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

            String trinoSql = replaceOutsideStrings(sql, vgiCatalogRef, trinoCatalog + ".").strip();
            trinoSql = trinoSql.endsWith(";") ? trinoSql.substring(0, trinoSql.length() - 1) : trinoSql;
            trinoSql = rewriteDuckDbOnlySyntax(trinoSql, trinoCatalog);
            retainedRecords.add(record);
            preCastSql.add(trinoSql);
        }

        List<String> castRewritten = CastRewriter.rewriteBatch(preCastSql);
        // The BLOB hex-escape rewrite must run AFTER the cast rewrite, not before: the source
        // corpus uses DuckDB's postfix '\xCA\xFE'::BLOB syntax, which sqlglot itself turns into
        // CAST('\xCA\xFE' AS VARBINARY) as part of its own '::' -> CAST(...) rewrite — the exact
        // textual shape rewriteBlobHexLiterals looks for doesn't exist until THAT step has already
        // run. Running it earlier (as part of rewriteDuckDbOnlySyntax, alongside the other
        // pre-cast-rewrite passes) was a real ordering bug: it silently never matched anything,
        // confirmed by a real mismatch sample still showing the untranslated '::BLOB' escape text
        // reaching Trino verbatim.
        for (int idx = 0; idx < castRewritten.size(); idx++) {
            castRewritten.set(idx, rewriteBlobHexLiterals(castRewritten.get(idx)));
        }

        for (int idx = 0; idx < retainedRecords.size(); idx++) {
            executed += executeAndCompare(runner, session, retainedRecords.get(idx), castRewritten.get(idx), failures);
        }
        return new Result(executed, skipped, List.copyOf(failures));
    }

    /**
     * Runs a hand-written, genuinely Trino-native {@code .trino.test} file — no DuckDB-syntax
     * rewriting at all (no catalog rename, no {@code TABLE(...)} wrap, no cast rewriting), since
     * unlike the real corpus's {@code .test} files, this file's SQL is already valid Trino SQL by
     * construction. See {@code VgiTrinoAdaptationsTest}'s own javadoc for what these files are for
     * and when to add one.
     *
     * @param runner the query runner to execute against
     * @param session the session to execute under (selects the Trino catalog/schema)
     * @param testFile the hand-written {@code .trino.test} file to replay
     */
    static Result runNative(DistributedQueryRunner runner, Session session, Path testFile) throws IOException {
        List<SqlLogicTestFile.Record> records = SqlLogicTestFile.parse(testFile);
        int executed = 0;
        List<String> failures = new ArrayList<>();
        for (SqlLogicTestFile.Record record : records) {
            if (record.kind() != SqlLogicTestFile.Kind.QUERY
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_OK
                    && record.kind() != SqlLogicTestFile.Kind.STATEMENT_ERROR) {
                continue;
            }
            String sql = String.join("\n", record.sql()).strip();
            if (sql.isBlank()) continue;
            String trinoSql = sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
            executed += executeAndCompare(runner, session, record, trinoSql, failures);
        }
        return new Result(executed, 0, List.copyOf(failures));
    }

    /**
     * Execute one record's already-fully-resolved Trino SQL and compare against its expected
     * outcome, appending to {@code failures} on a mismatch — the one piece {@link #run} (rewritten
     * DuckDB SQL) and {@link #runNative} (already-Trino-native SQL) both need identically; they
     * differ only in how {@code trinoSql} got built.
     *
     * @return 1 if this record counts as executed (matched/succeeded/failed-as-expected), 0 if a
     *         failure was appended instead
     */
    private static int executeAndCompare(DistributedQueryRunner runner, Session session,
            SqlLogicTestFile.Record record, String trinoSql, List<String> failures) {
        try {
            switch (record.kind()) {
                case QUERY -> {
                    MaterializedResult result = runner.execute(session, trinoSql);
                    List<Type> types = result.getTypes();
                    List<List<String>> actual = new ArrayList<>();
                    for (MaterializedRow row : result.getMaterializedRows()) {
                        List<String> cells = new ArrayList<>(row.getFieldCount());
                        for (int i = 0; i < row.getFieldCount(); i++) {
                            cells.add(formatCell(types.get(i), row.getField(i)));
                        }
                        actual.add(cells);
                    }
                    List<List<String>> expected = record.expectedRows();
                    // A query with no ORDER BY has no guaranteed row order at all (standard SQL
                    // semantics, not a connector correctness question) — DuckDB's and Trino's own
                    // (both equally valid, but different) execution orders will disagree on an
                    // unordered GROUP BY, confirmed against a real sample
                    // (table/partition_columns.test: identical rows, different order). Compare as
                    // a sorted multiset rather than positionally in that case; an explicit ORDER
                    // BY still gets a real, order-sensitive comparison.
                    if (!trinoSql.toUpperCase(Locale.ROOT).contains("ORDER BY")) {
                        actual = sortedCopy(actual);
                        expected = sortedCopy(expected);
                    }
                    if (!rowsMatch(actual, expected)) {
                        failures.add("QUERY mismatch for:\n" + trinoSql
                                + "\nexpected: " + record.expectedRows()
                                + "\nactual:   " + actual);
                        return 0;
                    }
                    return 1;
                }
                case STATEMENT_OK -> {
                    runner.execute(session, trinoSql);
                    return 1;
                }
                case STATEMENT_ERROR -> {
                    try {
                        runner.execute(session, trinoSql);
                        failures.add("expected an error for:\n" + trinoSql);
                        return 0;
                    } catch (RuntimeException e) {
                        // A DuckDB-specific error-message substring isn't expected to match
                        // Trino's own wording — the meaningful assertion here is just "it failed".
                        return 1;
                    }
                }
                default -> {
                    return 0;
                }
            }
        } catch (RuntimeException e) {
            failures.add("unexpected failure for:\n" + trinoSql + "\n" + e);
            return 0;
        }
    }

    /**
     * Replaces every occurrence of {@code target} with {@code replacement}, except inside a
     * single-quoted string literal (with {@code ''} as the escaped-quote form) — the catalog-name
     * rewrite this guards used to be a blind {@link String#replace}, which happily mangled a
     * string literal's own CONTENTS whenever they happened to contain the substring being
     * replaced: {@code upper_case('test@example.com')} silently became {@code
     * upper_case('test@vgi_example.com')}, a real, confirmed test-harness bug (found via a
     * genuine {@code QUERY mismatch} sample, not suspected) that has nothing to do with the
     * connector under test.
     */
    static String replaceOutsideStrings(String sql, String target, String replacement) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = sql.length();
        boolean inString = false;
        while (i < n) {
            char c = sql.charAt(i);
            if (inString) {
                out.append(c);
                if (c == '\'') {
                    if (i + 1 < n && sql.charAt(i + 1) == '\'') { out.append(sql.charAt(i + 1)); i += 2; continue; }
                    inString = false;
                }
                i++;
                continue;
            }
            if (c == '\'') { inString = true; out.append(c); i++; continue; }
            if (sql.startsWith(target, i)) {
                out.append(replacement);
                i += target.length();
                continue;
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /**
     * Format one result cell to compare against the {@code .test} file's own tab-separated
     * expected-row text — not a bare {@code Object.toString()}, which gets several real DuckDB
     * sqllogictest conventions wrong:
     * <ul>
     *   <li>{@code null} — the literal token {@code NULL} (unchanged from before).</li>
     *   <li>An empty string — DuckDB's convention represents this as the literal token {@code
     *       (empty)}, not the zero-length text itself (confirmed against a real mismatch: {@code
     *       upper_case('')} expected {@code (empty)}, and this harness produced a blank cell that
     *       compared unequal to it despite being the semantically identical result).</li>
     *   <li>{@code byte[]} (VARBINARY) — {@code Object.toString()} on a raw array produces Java's
     *       useless {@code [B@70bae186]} identity string; DuckDB prints {@code \xHH\xHH...} per
     *       byte, confirmed against a real {@code TO_HEX}-independent mismatch on a plain
     *       VARBINARY column.</li>
     *   <li>A {@link RowType} value — {@code Type.getObjectValue} returns a plain {@link List}
     *       with no field names (Trino's own generic representation for any row), which prints as
     *       {@code [3.0, 4.0]}; DuckDB prints a struct as {@code {'field': value, ...}} — with
     *       string field values UNQUOTED, confirmed against a real sample ({@code
     *       table/rowid.test}'s {@code {'a': 0, 'b': s_0}}, not {@code {'a': 0, 'b': 's_0'}}. Only
     *       the top-level column's type is threaded through here (matching what the real
     *       mismatches needed — {@code geo_centroid_struct}/{@code geo_centroid_list}/{@code
     *       rowid_struct} all return a bare, non-nested struct) — a struct nested inside an
     *       array/another struct falls back to the plain {@code List} rendering, a known, narrower
     *       gap than fixing every nesting depth would need. A REAL query (as opposed to this
     *       class's own hand-built unit-test values) materializes a struct as {@code
     *       io.trino.testing.MaterializedRow}, not a plain {@link List} at all — its OWN {@code
     *       toString()} also happens to render as a bracket-joined list, indistinguishable from a
     *       genuine array — so both shapes are accepted here.</li>
     *   <li>A {@link Double} — Java's default {@code Double.toString()} switches to scientific
     *       notation above a certain magnitude ({@code 1.1997E7}); DuckDB always prints the full
     *       plain decimal expansion ({@code 11994000.0}) — confirmed against a real sample
     *       ({@code table/projected_data.test}).</li>
     *   <li>A {@link LocalDateTime} (TIMESTAMP) — {@code Object.toString()} gives Java's ISO-8601
     *       form: a {@code T} separator, and trailing zero seconds/fraction components dropped
     *       entirely ({@code 2026-05-06T12:00}, {@code 2026-05-06T12:34:56.123}). DuckDB always
     *       uses a space separator and always shows a full {@code HH:mm:ss} even when the seconds
     *       are exactly {@code :00} — confirmed against a real sample ({@code
     *       filter_pushdown/temporal.test}: expected {@code 2026-05-06 12:00:00}, this harness
     *       previously rendered {@code 2026-05-06T12:00}). The fractional-second suffix is shown
     *       only when non-zero, at up to microsecond precision (DuckDB's own TIMESTAMP
     *       resolution) — but, unlike Java's 3/6/9-digit-grouped convention, with trailing zeros
     *       stripped down to exactly the significant digits, confirmed directly against a real
     *       {@code duckdb} CLI session ({@code CAST('...00.100' AS TIMESTAMP)} displays as {@code
     *       .1}, not {@code .100000} or Java's own {@code .100}). See {@link #formatTimestamp}.</li>
     *   <li>A {@link ZonedDateTime} (TIMESTAMP WITH TIME ZONE) — same space/{@code HH:mm:ss}/
     *       fraction rules as above, plus DuckDB's plain offset suffix ({@code +HH} for a
     *       whole-hour offset, {@code +HH:mm} otherwise) in place of Java's {@code
     *       ZonedDateTime.toString()} zone-id-bracket suffix ({@code Z[UTC]}, {@code +01:00} —
     *       confirmed against the same real sample: expected {@code 2026-05-06 12:00:00+00}, this
     *       harness previously rendered {@code 2026-05-06T12:00Z[UTC]}). The bare-vs-colon offset
     *       split is confirmed directly against a real {@code duckdb} CLI session with a
     *       fractional-hour {@code TimeZone} set ({@code Asia/Kolkata}'s {@code +05:30} prints
     *       with minutes; {@code UTC}'s {@code +00} prints bare). See {@link #formatOffset}.</li>
     * </ul>
     */
    static String formatCell(Type type, Object value) {
        if (value == null) return "NULL";
        if (value instanceof byte[] bytes) {
            StringBuilder hex = new StringBuilder(bytes.length * 4);
            for (byte b : bytes) hex.append("\\x").append(String.format(Locale.ROOT, "%02X", b));
            return hex.toString();
        }
        if (value instanceof String s) {
            return s.isEmpty() ? "(empty)" : s;
        }
        if (value instanceof Double d) {
            return formatDouble(d);
        }
        if (value instanceof LocalDateTime dt) {
            return formatTimestamp(dt);
        }
        if (value instanceof ZonedDateTime zdt) {
            return formatTimestamp(zdt.toLocalDateTime()) + formatOffset(zdt.getOffset());
        }
        if (type instanceof RowType rowType) {
            // A struct's boxed representation is a plain java.util.List when it came from a
            // hand-built value (e.g. this class's own unit tests), but a REAL query executed via
            // DistributedQueryRunner materializes it as io.trino.testing.MaterializedRow instead —
            // a distinct wrapper class (getFields()/getField(int), no List superinterface at all).
            // The original List-only check silently fell through to the plain value.toString()
            // branch below for every live struct-returning query — confirmed the hard way against
            // the real fixture (scalar/geo_centroid.test's geo_centroid_struct/geo_centroid_list):
            // MaterializedRow.toString() ALSO happens to render as a bracket-joined list
            // ("[3.0, 4.0]"), which is exactly the reported "returns an array instead of a struct"
            // symptom — a formatCell gap, not a connector value-mapping bug (VgiTypeMapping/
            // VgiScalarFunctions already produce a genuine RowType/SqlRow value; only this
            // rendering step never recognized MaterializedRow as one).
            List<?> fieldValues = value instanceof MaterializedRow row ? row.getFields()
                    : value instanceof List<?> list ? list : null;
            if (fieldValues != null) {
                List<RowType.Field> fields = rowType.getFields();
                StringBuilder struct = new StringBuilder("{");
                for (int i = 0; i < fields.size() && i < fieldValues.size(); i++) {
                    if (i > 0) struct.append(", ");
                    String fieldName = fields.get(i).getName().orElse("field" + i);
                    Object fieldValue = fieldValues.get(i);
                    String fieldText = fieldValue instanceof Double d ? formatDouble(d) : String.valueOf(fieldValue);
                    struct.append('\'').append(fieldName).append("': ").append(fieldText);
                }
                return struct.append('}').toString();
            }
        }
        return value.toString();
    }

    /** {@link Double#toString()} without ever falling back to scientific notation — see {@link
     *  #formatCell}'s own javadoc for the real mismatch this fixes. {@link BigDecimal#toPlainString()}
     *  never uses an exponent regardless of scale, so parsing {@code Double.toString(d)} (itself
     *  possibly already in scientific form) through a {@link BigDecimal} and back out gives the
     *  same VALUE in DuckDB's always-plain-decimal convention. */
    private static String formatDouble(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) return Double.toString(d);
        String plain = new BigDecimal(Double.toString(d)).stripTrailingZeros().toPlainString();
        return plain.contains(".") ? plain : plain + ".0";
    }

    /** DuckDB's TIMESTAMP display convention — see {@link #formatCell}'s own javadoc for the real
     *  mismatch this fixes: a space separator (never Java's {@code T}), always a full {@code
     *  HH:mm:ss}, and a fractional-second suffix only when non-zero. */
    private static String formatTimestamp(LocalDateTime dt) {
        StringBuilder text = new StringBuilder(26);
        text.append(String.format(Locale.ROOT, "%04d-%02d-%02d %02d:%02d:%02d",
                dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                dt.getHour(), dt.getMinute(), dt.getSecond()));
        appendFractionalSeconds(text, dt.getNano());
        return text.toString();
    }

    /** Appends {@code .<digits>} to {@code text} for a non-zero fractional second, at up to
     *  microsecond precision (DuckDB's own TIMESTAMP resolution) with trailing zeros stripped
     *  entirely — e.g. nano-of-second {@code 100_000_000} (a tenth of a second) becomes {@code
     *  .1}, not Java's own 3-digit-grouped {@code .100} — confirmed against a real {@code duckdb}
     *  CLI session (see {@link #formatCell}'s javadoc). Appends nothing at all when the value has
     *  no fractional second, or when it rounds away to nothing at microsecond precision. */
    private static void appendFractionalSeconds(StringBuilder text, int nanoOfSecond) {
        if (nanoOfSecond == 0) return;
        int micros = nanoOfSecond / 1000;
        String sixDigits = String.format(Locale.ROOT, "%06d", micros);
        int end = sixDigits.length();
        while (end > 0 && sixDigits.charAt(end - 1) == '0') end--;
        if (end == 0) return;
        text.append('.').append(sixDigits, 0, end);
    }

    /** DuckDB's plain {@code TIMESTAMP WITH TIME ZONE} offset suffix — {@code +HH} for a
     *  whole-hour offset, {@code +HH:mm} (and, in the (unobserved in the real corpus but handled
     *  for correctness) sub-minute case, {@code +HH:mm:ss}) otherwise — never Java's {@code
     *  ZonedDateTime.toString()} zone-id-bracket suffix. See {@link #formatCell}'s javadoc for the
     *  real {@code duckdb} CLI session that confirmed the bare-vs-colon split. */
    private static String formatOffset(ZoneOffset offset) {
        int totalSeconds = offset.getTotalSeconds();
        char sign = totalSeconds < 0 ? '-' : '+';
        int abs = Math.abs(totalSeconds);
        int hours = abs / 3600;
        int minutes = (abs % 3600) / 60;
        int seconds = abs % 60;
        StringBuilder text = new StringBuilder(9);
        text.append(sign).append(String.format(Locale.ROOT, "%02d", hours));
        if (minutes != 0 || seconds != 0) {
            text.append(':').append(String.format(Locale.ROOT, "%02d", minutes));
            if (seconds != 0) text.append(':').append(String.format(Locale.ROOT, "%02d", seconds));
        }
        return text.toString();
    }

    /** A copy of {@code rows}, sorted into a stable, arbitrary total order — for comparing an
     *  ORDER-BY-less query's result as a multiset (see the {@code QUERY} case in {@link #run}). */
    private static List<List<String>> sortedCopy(List<List<String>> rows) {
        List<List<String>> copy = new ArrayList<>(rows);
        copy.sort(Comparator.comparing(row -> String.join("", row)));
        return copy;
    }

    /** Row-by-row, cell-by-cell comparison via {@link #cellsMatch} rather than a bare {@link
     *  List#equals}, so a DuckDB/Trino type-NAME difference (see {@link #cellsMatch}) doesn't fail
     *  an otherwise-correct result. */
    static boolean rowsMatch(List<List<String>> actual, List<List<String>> expected) {
        if (actual.size() != expected.size()) return false;
        for (int r = 0; r < actual.size(); r++) {
            List<String> actualRow = actual.get(r);
            List<String> expectedRow = expected.get(r);
            if (actualRow.size() != expectedRow.size()) return false;
            for (int c = 0; c < actualRow.size(); c++) {
                if (!cellsMatch(actualRow.get(c), expectedRow.get(c))) return false;
            }
        }
        return true;
    }

    /**
     * DuckDB's {@code TYPEOF(...)} (and similar introspection) returns an UPPERCASE type name
     * ({@code VARCHAR}, {@code BLOB}); Trino's returns lowercase ({@code varchar}) and sometimes a
     * genuinely different name for the same concept ({@code varbinary} vs {@code BLOB}) — real,
     * confirmed mismatches sampled from the census, not a hypothetical. Falls back to a
     * case-insensitive, alias-normalized comparison ONLY when the EXPECTED cell is itself a
     * recognized DuckDB type-name token (see {@link #DUCKDB_TYPE_NAMES}) — deliberately not a
     * blanket case-insensitive comparison, which would risk silently masking a genuine casing bug
     * in actual string DATA (e.g. a broken {@code upper_case()} returning wrong-case text).
     */
    static boolean cellsMatch(String actual, String expected) {
        if (actual.equals(expected)) return true;
        String expectedUpper = expected.toUpperCase(Locale.ROOT);
        if (!DUCKDB_TYPE_NAMES.contains(expectedUpper)) return false;
        String actualUpper = TRINO_TO_DUCKDB_TYPE_ALIAS.getOrDefault(
                actual.toUpperCase(Locale.ROOT), actual.toUpperCase(Locale.ROOT));
        return actualUpper.equals(expectedUpper);
    }

    private static final Set<String> DUCKDB_TYPE_NAMES = Set.of(
            "VARCHAR", "BIGINT", "INTEGER", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "BOOLEAN",
            "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "BLOB", "DECIMAL", "HUGEINT",
            "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "JSON", "STRUCT", "MAP");

    private static final Map<String, String> TRINO_TO_DUCKDB_TYPE_ALIAS = Map.of(
            "VARBINARY", "BLOB", "ROW", "STRUCT");

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
     *   <li>DuckDB's builtin row-generator functions {@code range(...)} and {@code
     *       generate_series(...)}, used bare as a table reference, become Trino's {@code
     *       UNNEST(SEQUENCE(...))} idiom — Trino has no {@code range()} table function of its
     *       own. See {@link #rewriteRangeCalls} for the arithmetic this needs (DuckDB's {@code
     *       range} is exclusive-stop, like Python's; Trino's {@code sequence} is inclusive-stop
     *       and, worse, silently auto-detects ascending-vs-descending when no step is given —
     *       both need correcting, not just the call syntax).</li>
     *   <li>A VGI function called with only {@code catalog.function(args)} (two dotted parts) —
     *       DuckDB resolves a function by name against its own search path with no schema
     *       qualification required; Trino always needs the full {@code catalog.schema.function}
     *       (three parts) to reach a connector-defined function at all, or the call fails with
     *       {@code Function 'catalog.function' not registered} rather than a parse error (a real,
     *       common failure mode found by sampling the census's {@code UNSUPPORTED} bucket — {@code
     *       example.double(...)}, {@code example.sum_values(...)}, and others). See {@link
     *       #insertDefaultSchema} for the {@code DEFAULT_SCHEMA} assumption this rewrite makes.</li>
     *   <li>A call mixing bare positional arguments with {@code name => value} ones — legal in
     *       DuckDB (e.g. {@code sequence(10, batch_size := 0)}), but Trino rejects ANY such mix
     *       outright ({@code "All arguments must be passed by name or all must be passed
     *       positionally"} — confirmed the single most common failure reason across the whole
     *       census before this rewrite existed). See {@link #normalizeMixedPositionalNamedArgs}
     *       for the exact, deliberately conservative shape this handles.</li>
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
        String rewritten = insertDefaultSchema(sql, trinoCatalog);
        rewritten = wrapBareTableFunctionCalls(rewritten, trinoCatalog);
        rewritten = rewriteRangeCalls(rewritten);
        rewritten = rewriteAtClause(rewritten);
        rewritten = rewritten.replace(":=", "=>");
        return normalizeMixedPositionalNamedArgs(rewritten);
    }

    /**
     * One parsed call argument: an optional {@code "name => "} label (empty string if this
     * argument is positional) plus its value text.
     *
     * @param label the exact {@code "name => "} text to keep if this argument stays named, or
     *        {@code ""} if positional
     * @param value the argument's value text, already recursively normalized (see {@link
     *        #normalizeMixedPositionalNamedArgs})
     * @param named whether this argument had a {@code name =>} label at all
     */
    private record NamedArg(String label, String value, boolean named) {}

    /** Splits one already-{@code splitTopLevelArgs}-delimited argument into its {@code name => }
     *  label (if any) and value, recursively normalizing the value's own nested calls. */
    private static NamedArg parseArgument(String arg) {
        String trimmed = arg.strip();
        int idx = 0;
        while (idx < trimmed.length() && isIdentChar(trimmed.charAt(idx))) idx++;
        if (idx > 0) {
            int j = idx;
            while (j < trimmed.length() && Character.isWhitespace(trimmed.charAt(j))) j++;
            if (trimmed.startsWith("=>", j)) {
                String value = trimmed.substring(j + 2).strip();
                return new NamedArg(trimmed.substring(0, idx) + " => ",
                        normalizeMixedPositionalNamedArgs(value), true);
            }
        }
        return new NamedArg("", normalizeMixedPositionalNamedArgs(trimmed), false);
    }

    /**
     * Rewrites {@code some_call(pos1, pos2, name1 => val1, name2 => val2)} — zero or more bare
     * positional arguments followed by zero or more {@code name => value} ones, in that order —
     * into all-positional form (dropping the {@code name => } labels), the shape Trino requires
     * when a call has ANY positional argument at all. Recurses into every argument's own nested
     * calls first, so a mixed-arg call buried inside another call's argument is normalized too.
     *
     * <p>Deliberately does nothing for any OTHER shape: a call with no named arguments (already
     * fine), one with no positional arguments (an all-named call is legal in Trino as-is, no
     * violation to fix), or one where a positional argument appears AFTER a named one (DuckDB
     * allows arbitrary reordering by name; blindly stripping labels there could silently swap two
     * arguments' values into the wrong slots — safer to leave it failing than to guess wrong).
     * Applied to every {@code identifier(...)} call in the whole SQL text, not just ones after
     * {@code FROM}/{@code JOIN} — a plain scalar function call anywhere in a {@code SELECT} list
     * can mix positional/named args exactly the same way a table function call can.
     *
     * <p>Safe to run against arbitrary SQL text even where it doesn't apply: a call with no
     * {@code name => value}-shaped argument (every ordinary Trino/DuckDB function call, since
     * {@code =>} is not standard SQL syntax outside this rewrite's own output) is left completely
     * unchanged.
     */
    static String normalizeMixedPositionalNamedArgs(String sql) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            boolean identStart = isIdentChar(c) && (i == 0 || !isIdentChar(sql.charAt(i - 1)));
            if (!identStart) {
                out.append(c);
                i++;
                continue;
            }
            int callStart = i;
            while (i < n && isIdentChar(sql.charAt(i))) i++;
            if (i >= n || sql.charAt(i) != '(') {
                out.append(sql, callStart, i);
                continue;
            }
            int openParen = i;
            int closeParen = findMatchingParen(sql, openParen);
            if (closeParen < 0) {
                out.append(sql, callStart, i);
                continue;
            }
            List<String> rawArgs = splitTopLevelArgs(sql.substring(openParen + 1, closeParen));
            List<NamedArg> args = new ArrayList<>(rawArgs.size());
            for (String rawArg : rawArgs) args.add(parseArgument(rawArg));

            boolean namedSeenSoFar = false;
            boolean namedAfterPositional = false; // a positional arg appears AFTER a named one
            boolean anyPositional = false;
            boolean anyNamed = false;
            for (NamedArg arg : args) {
                if (arg.named()) {
                    anyNamed = true;
                    namedSeenSoFar = true;
                } else {
                    anyPositional = true;
                    if (namedSeenSoFar) namedAfterPositional = true;
                }
            }
            boolean stripLabels = anyPositional && anyNamed && !namedAfterPositional;

            out.append(sql, callStart, openParen + 1);
            for (int k = 0; k < args.size(); k++) {
                if (k > 0) out.append(", ");
                NamedArg arg = args.get(k);
                out.append(arg.named() && !stripLabels ? arg.label() + arg.value() : arg.value());
            }
            out.append(')');
            i = closeParen + 1;
        }
        return out.toString();
    }

    /**
     * Rewrites DuckDB's time-travel {@code AT (VERSION => expr)}/{@code AT (TIMESTAMP => expr)}
     * clause into Trino's own {@code FOR VERSION AS OF expr}/{@code FOR TIMESTAMP AS OF expr} —
     * this connector's time-travel support (see the README's own "Time travel" section) already
     * implements the LATTER syntax; the corpus just never spoke it. Not a syntax-only nicety: this
     * exercises a real, already-working feature the census previously never got to test at all
     * (every one of these records was failing as a parse error before this rewrite existed).
     */
    private static String rewriteAtClause(String sql) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = sql.length();
        while (i < n) {
            int afterAt = matchKeyword(sql, i, "AT");
            int p = afterAt;
            if (afterAt >= 0) {
                while (p < n && Character.isWhitespace(sql.charAt(p))) p++;
            }
            if (afterAt < 0 || p >= n || sql.charAt(p) != '(') {
                out.append(sql.charAt(i));
                i++;
                continue;
            }
            int openParen = p;
            int afterOpenWs = openParen + 1;
            while (afterOpenWs < n && Character.isWhitespace(sql.charAt(afterOpenWs))) afterOpenWs++;
            int afterKeyword = matchKeyword(sql, afterOpenWs, "VERSION");
            String keyword = "VERSION";
            if (afterKeyword < 0) {
                afterKeyword = matchKeyword(sql, afterOpenWs, "TIMESTAMP");
                keyword = "TIMESTAMP";
            }
            int afterKwWs = afterKeyword;
            if (afterKeyword >= 0) {
                while (afterKwWs < n && Character.isWhitespace(sql.charAt(afterKwWs))) afterKwWs++;
            }
            if (afterKeyword < 0 || !sql.startsWith("=>", afterKwWs)) {
                out.append(sql.charAt(i));
                i++;
                continue;
            }
            int closeParen = findMatchingParen(sql, openParen);
            if (closeParen < 0) {
                out.append(sql.charAt(i));
                i++;
                continue;
            }
            String content = sql.substring(afterKwWs + 2, closeParen).strip();
            out.append("FOR ").append(keyword).append(" AS OF ").append(content);
            i = closeParen + 1;
        }
        return out.toString();
    }

    /**
     * Rewrites {@code CAST('\xHH\xHH...' AS VARBINARY)} — DuckDB's {@code \xHH} byte-escape
     * convention inside a string literal destined for VARBINARY — into Trino's own {@code
     * X'HHHH...'} hex-literal syntax. Trino's string-literal grammar has no {@code \x} escape at
     * all, so the original literal is instead taken completely literally: {@code
     * CAST('\xCA\xFE' AS VARBINARY)} silently casts the 8-character text {@code \xCA\xFE} (a
     * backslash, an x, four hex-digit characters) to its own UTF-8 bytes — an entirely different,
     * wrong value — confirmed via a real {@code QUERY_MISMATCH} sample ({@code binary_packet.test}
     * expected {@code CAFE0102763101}, got the UTF-8 bytes of the literal escape text instead).
     *
     * <p>Deliberately narrow: only rewrites a literal whose ENTIRE content is a (possibly empty)
     * run of {@code \xHH} groups — a mix of plain text and escapes, or any other DuckDB string
     * escape, is left alone rather than guessing at a more general escape-decoding rule the real
     * corpus doesn't need.
     *
     * <p><b>Must run AFTER {@link CastRewriter}, not as part of {@link #rewriteDuckDbOnlySyntax}</b>
     * — the real corpus writes this as DuckDB's postfix {@code '\xCA\xFE'::BLOB} form, which only
     * BECOMES {@code CAST('\xCA\xFE' AS VARBINARY)} once sqlglot's own {@code ::} → {@code CAST}
     * rewrite has already run. Running this method before that step is a real ordering bug found
     * the hard way: it silently matched nothing (the {@code CAST(...)} shape didn't exist yet),
     * and the untranslated {@code ::BLOB} escape text reached Trino verbatim.
     */
    static String rewriteBlobHexLiterals(String sql) {
        String marker = "CAST('";
        StringBuilder out = new StringBuilder();
        int i = 0;
        int n = sql.length();
        while (i < n) {
            if (sql.startsWith(marker, i)) {
                int contentStart = i + marker.length();
                int closeQuote = findClosingQuote(sql, contentStart);
                if (closeQuote >= 0) {
                    String content = sql.substring(contentStart, closeQuote);
                    int afterQuote = closeQuote + 1;
                    int afterWhitespace = afterQuote;
                    while (afterWhitespace < n && Character.isWhitespace(sql.charAt(afterWhitespace))) afterWhitespace++;
                    String asVarbinary = "AS VARBINARY)";
                    if (sql.startsWith(asVarbinary, afterWhitespace) && isPureHexEscapeRun(content)) {
                        out.append("X'").append(decodeHexEscapes(content)).append('\'');
                        i = afterWhitespace + asVarbinary.length();
                        continue;
                    }
                }
            }
            out.append(sql.charAt(i));
            i++;
        }
        return out.toString();
    }

    /** Index of the {@code '} closing a string literal that started right after {@code openIndex - 1}'s
     *  opening quote (i.e. {@code openIndex} is the first content character), honoring {@code ''} as an
     *  escaped quote — or -1 if it never closes. */
    private static int findClosingQuote(String sql, int openIndex) {
        for (int k = openIndex; k < sql.length(); k++) {
            if (sql.charAt(k) == '\'') {
                if (k + 1 < sql.length() && sql.charAt(k + 1) == '\'') { k++; continue; }
                return k;
            }
        }
        return -1;
    }

    private static boolean isPureHexEscapeRun(String content) {
        if (content.length() % 4 != 0) return false;
        for (int i = 0; i < content.length(); i += 4) {
            if (content.charAt(i) != '\\' || content.charAt(i + 1) != 'x'
                    || Character.digit(content.charAt(i + 2), 16) < 0
                    || Character.digit(content.charAt(i + 3), 16) < 0) {
                return false;
            }
        }
        return true;
    }

    private static String decodeHexEscapes(String content) {
        StringBuilder hex = new StringBuilder(content.length());
        for (int i = 0; i < content.length(); i += 4) {
            hex.append(Character.toUpperCase(content.charAt(i + 2)));
            hex.append(Character.toUpperCase(content.charAt(i + 3)));
        }
        return hex.toString();
    }

    /**
     * Every real fixture function this harness exercises lives in the {@code main} schema —
     * confirmed by every three-part call already in the corpus ({@code example.main.multiply(...)},
     * etc.); a two-part {@code catalog.function(args)} call is DuckDB relying on its own
     * schema-less resolution against exactly that same fixture worker, so inserting {@code main}
     * unconditionally (rather than trying to discover the "real" schema per function) is the
     * correct fix for THIS harness's one fixture worker, not a generally-safe assumption a
     * different corpus/worker could reuse as-is.
     *
     * <p>Runs first, before the other rewrites, so a two-part call used as a table reference
     * (e.g. {@code FROM catalog.function(args)}) is already three-part by the time {@link
     * #wrapBareTableFunctionCalls} looks for one immediately after {@code FROM}/{@code JOIN}.
     */
    private static final String DEFAULT_SCHEMA = "main";

    private static String insertDefaultSchema(String sql, String trinoCatalog) {
        StringBuilder out = new StringBuilder();
        String prefix = trinoCatalog + ".";
        int i = 0;
        int n = sql.length();
        boolean inString = false;
        while (i < n) {
            char c = sql.charAt(i);
            if (inString) {
                out.append(c);
                if (c == '\'') {
                    if (i + 1 < n && sql.charAt(i + 1) == '\'') { out.append(sql.charAt(i + 1)); i += 2; continue; }
                    inString = false;
                }
                i++;
                continue;
            }
            if (c == '\'') { inString = true; out.append(c); i++; continue; }

            if (sql.startsWith(prefix, i) && (i == 0 || !isIdentChar(sql.charAt(i - 1)))) {
                int identStart = i + prefix.length();
                int p = identStart;
                while (p < n && isIdentChar(sql.charAt(p))) p++;
                if (p > identStart && p < n && sql.charAt(p) == '(') {
                    // catalog.ident( with no schema segment in between — a two-part function call.
                    out.append(prefix).append(DEFAULT_SCHEMA).append('.').append(sql, identStart, p);
                    i = p;
                    continue;
                }
                // Either not a call at all (no following '(') or already three-part
                // (catalog.schema.ident...) — fall through and copy character-by-character; the
                // next iteration re-attempts the match one position later, which naturally never
                // re-matches inside an already-qualified reference.
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /**
     * Rewrites a bare {@code range(...)}/{@code generate_series(...)} table reference into
     * Trino's {@code UNNEST(SEQUENCE(...))}. Only the 0/1/2-argument forms are handled — the only
     * ones the real corpus actually uses (confirmed by grepping it; no 3-argument, explicit-step
     * call exists) — a 3-argument call is left untouched (still fails afterward, exactly as
     * before this rewrite existed, not a regression).
     *
     * <p>Two arithmetic corrections are needed, not just a name/argument-order change:
     * <ul>
     *   <li>{@code range(stop)}/{@code range(start, stop)} are exclusive of {@code stop} (Python's
     *       {@code range} semantics); {@code sequence(start, stop)} is inclusive of both bounds —
     *       so the translated stop bound is {@code (stop) - 1}, parenthesized so it's correct even
     *       when {@code stop} is itself an expression, not a literal.</li>
     *   <li>Trino's 2-argument {@code sequence(start, stop)} auto-detects direction when no step
     *       is given — {@code sequence(1, 0)} does NOT return zero rows, it returns the
     *       descending {@code [1, 0]}. DuckDB's {@code range(1, 1)} (which the real corpus uses)
     *       must return zero rows. Always emitting an explicit {@code step} of {@code 1} disables
     *       that auto-direction guessing, so an empty range stays empty instead of silently
     *       flipping direction.</li>
     * </ul>
     * {@code generate_series(start, stop)} needs neither correction — DuckDB's version is already
     * inclusive-stop, matching {@code sequence} directly — but still gets the explicit step for
     * the same auto-direction reason.
     */
    private static String rewriteRangeCalls(String sql) {
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

            String funcName;
            int nameEnd;
            int afterRange = matchKeyword(sql, i, "range");
            int afterGenSeries = matchKeyword(sql, i, "generate_series");
            if (afterRange >= 0) { funcName = "range"; nameEnd = afterRange; }
            else if (afterGenSeries >= 0) { funcName = "generate_series"; nameEnd = afterGenSeries; }
            else continue;

            int p = nameEnd;
            while (p < n && Character.isWhitespace(sql.charAt(p))) p++;
            if (p >= n || sql.charAt(p) != '(') continue;

            int closeParen = findMatchingParen(sql, p);
            if (closeParen < 0) continue;

            List<String> callArgs = splitTopLevelArgs(sql.substring(p + 1, closeParen));
            String rewritten = toSequenceCall(funcName, callArgs);
            if (rewritten == null) continue; // unsupported arity — leave the original text alone

            out.append(rewritten);
            i = closeParen + 1;
        }
        return out.toString();
    }

    /** Builds the {@code UNNEST(SEQUENCE(...))} replacement text, or null if this arity isn't handled. */
    private static String toSequenceCall(String funcName, List<String> args) {
        if (funcName.equals("generate_series")) {
            return args.size() == 2
                    ? "UNNEST(SEQUENCE(" + args.get(0) + ", " + args.get(1) + ", 1))"
                    : null;
        }
        return switch (args.size()) {
            case 1 -> "UNNEST(SEQUENCE(0, (" + args.get(0) + ") - 1, 1))";
            case 2 -> "UNNEST(SEQUENCE(" + args.get(0) + ", (" + args.get(1) + ") - 1, 1))";
            default -> null;
        };
    }

    /** Splits a function call's argument text on top-level commas — depth-aware (nested parens
     *  don't split) and quote-aware (a comma inside a {@code '...'} literal doesn't split either).
     *  Returns an empty list for a blank/whitespace-only argument list (a zero-argument call). */
    private static List<String> splitTopLevelArgs(String argsText) {
        List<String> args = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 0;
        for (int k = 0; k < argsText.length(); k++) {
            char c = argsText.charAt(k);
            if (inString) {
                if (c == '\'') {
                    if (k + 1 < argsText.length() && argsText.charAt(k + 1) == '\'') { k++; continue; }
                    inString = false;
                }
                continue;
            }
            if (c == '\'') { inString = true; continue; }
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                args.add(argsText.substring(start, k).strip());
                start = k + 1;
            }
        }
        String last = argsText.substring(start).strip();
        if (!last.isEmpty() || !args.isEmpty()) args.add(last);
        return args;
    }

    /**
     * Wraps a bare {@code trinoCatalog.schema.function(args)} call immediately after {@code FROM}/
     * {@code JOIN} in Trino's required {@code TABLE(...)}, and — since a table-in-out function's
     * {@code TableInput} argument needs the SAME treatment one level down — also rewraps any
     * argument that is itself, in its entirety, a bare {@code (SELECT ...)}/{@code (WITH ...)}
     * subquery into {@code TABLE(...)} (see {@link #wrapTableArgumentIfSubquery}).
     */
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

            // A table-in-out call's TableInput argument is DuckDB's bare `(SELECT ...)`/`(WITH
            // ...)` subquery — always positional (VGI's own resolve_metadata requires this), never
            // named — but Trino requires the wrapper's OWN TABLE(...) argument to say so too:
            // `echo((SELECT 1 AS a))` must become `TABLE(echo(TABLE(SELECT 1 AS a)))`, not just
            // `TABLE(echo((SELECT 1 AS a)))` (confirmed via the real corpus — the latter fails with
            // "Invalid argument DATA. Expected table, got expression", the single most common
            // failure in the whole census before this rewrite existed). A plain scalar argument
            // (`repeat_inputs(3, ...)`) is untouched — wrapTableArgumentIfSubquery only rewrites an
            // argument that IS, in its entirety, a parenthesized SELECT/WITH.
            List<String> args = splitTopLevelArgs(sql.substring(p + 1, closeParen));
            StringBuilder rebuiltArgs = new StringBuilder();
            for (int k = 0; k < args.size(); k++) {
                if (k > 0) rebuiltArgs.append(", ");
                rebuiltArgs.append(wrapTableArgumentIfSubquery(args.get(k)));
            }
            String call = sql.substring(i, p + 1) + rebuiltArgs + ")";
            out.append("TABLE(").append(call).append(")");
            i = closeParen + 1;
        }
        return out.toString();
    }

    /**
     * If {@code arg}, in its ENTIRETY, is a parenthesized {@code SELECT}/{@code WITH} — DuckDB's
     * bare-subquery convention for a table-in-out function's {@code TableInput} argument — rewrite
     * it as Trino's required {@code TABLE(...)} form. Any other argument shape (a plain scalar
     * expression, or an already-{@code TABLE(...)}-wrapped one) is returned unchanged.
     */
    private static String wrapTableArgumentIfSubquery(String arg) {
        String trimmed = arg.strip();
        if (!trimmed.startsWith("(")) return arg;
        int close = findMatchingParen(trimmed, 0);
        if (close != trimmed.length() - 1) return arg; // the parens don't span the whole argument
        String inner = trimmed.substring(1, close).strip();
        if (matchKeyword(inner, 0, "SELECT") >= 0 || matchKeyword(inner, 0, "WITH") >= 0) {
            return "TABLE(" + inner + ")";
        }
        return arg;
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
