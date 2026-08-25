// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Rewrites DuckDB's {@code expr::TYPE} postfix cast syntax (Trino has no equivalent operator at
 * all — confirmed by direct testing, not assumed) into Trino's {@code CAST(expr AS TYPE)}, by
 * shelling out to the real <a href="https://github.com/tobymao/sqlglot">sqlglot</a> Python
 * library rather than hand-rolling an expression-boundary scanner ourselves.
 *
 * <p>Unlike {@link SqlLogicTestRunner}'s own {@code range()}/{@code TABLE()} rewrites, a cast can
 * appear anywhere in an expression — after a literal, an identifier, a parenthesized expression, a
 * nested cast, a qualified function call — not just immediately after a fixed keyword, and the
 * type name itself sometimes needs translating too ({@code BLOB} → {@code VARBINARY}, {@code
 * TIMESTAMPTZ} → {@code TIMESTAMP WITH TIME ZONE}). That's a real (if small) expression grammar,
 * exactly the kind of thing a maintained SQL-dialect library gets right far more reliably than a
 * bespoke scanner would.
 *
 * <p><b>sqlglot's own {@code duckdb -> trino} rule for bare {@code range()} table calls is
 * confirmed buggy</b> (verified by direct testing): it drops the query's column alias and reuses
 * the table alias as the column name instead, turning {@code SELECT i FROM range(10) t(i)} into
 * an unresolvable-column query. This class is never used for that rewrite — {@link
 * SqlLogicTestRunner} converts {@code range()}/{@code generate_series()}/bare table-function calls
 * to Trino syntax itself, FIRST, before this class ever sees the SQL. By the time sqlglot runs,
 * there's no more bare {@code range()} call left for its buggy rule to match — confirmed by
 * testing the exact composed pipeline: sqlglot leaves an already-rewritten {@code
 * TABLE(...)}/{@code UNNEST(SEQUENCE(...))} query untouched while still correctly rewriting a
 * remaining {@code ::} cast in the same query.
 *
 * <p>Best-effort, not a hard dependency: if {@code python3} or the {@code sqlglot} package isn't
 * available, {@link #rewriteBatch} returns its input unchanged (one WARN, once) rather than
 * failing the test — the {@code ::}-using records that needed this simply keep failing exactly as
 * they did before this class existed, not a regression.
 *
 * <p>Resolves the Python executable to use, in order: {@code -Dvgitrino.sqlglot.python=<path>} if
 * set; else {@code ~/.venvs/vgitrino-sqlglot/bin/python} if that venv exists (create it with
 * {@code python3 -m venv ~/.venvs/vgitrino-sqlglot && ~/.venvs/vgitrino-sqlglot/bin/pip install
 * sqlglot} — kept out of the system Python deliberately: Homebrew's Python refuses a bare {@code
 * pip install} under PEP 668); else plain {@code python3} on {@code PATH}, which works if sqlglot
 * happens to be installed there already.
 */
final class CastRewriter {

    private static final String PYTHON = resolvePython();

    private static String resolvePython() {
        String override = System.getProperty("vgitrino.sqlglot.python");
        if (override != null) return override;
        File venvPython = new File(System.getProperty("user.home"), ".venvs/vgitrino-sqlglot/bin/python");
        return venvPython.isFile() ? venvPython.getPath() : "python3";
    }

    /** One line per input string: base64-encode/decode rather than a JSON/text framing, since a
     *  raw SQL string can itself contain newlines and arbitrary bytes — this sidesteps needing
     *  any escaping (or a JSON library on the test compile classpath, which isn't guaranteed —
     *  Jackson is only a transitive test-runtime dependency here). */
    private static final String PYTHON_SCRIPT = """
            import base64
            import sys

            try:
                import sqlglot
            except ImportError:
                for line in sys.stdin:
                    sys.stdout.write(line)
                sys.exit(0)

            for line in sys.stdin:
                sql = base64.b64decode(line.strip()).decode("utf-8")
                try:
                    rewritten = sqlglot.transpile(sql, read="duckdb", write="trino")[0]
                except Exception:
                    rewritten = sql
                sys.stdout.write(base64.b64encode(rewritten.encode("utf-8")).decode("ascii") + "\\n")
            """;

    private static volatile Boolean available;

    private CastRewriter() {}

    /** Probes once (cached) whether {@code python3 -c "import sqlglot"} succeeds. */
    static boolean available() {
        Boolean cached = available;
        if (cached != null) return cached;
        boolean result;
        try {
            Process p = new ProcessBuilder(PYTHON, "-c", "import sqlglot").start();
            result = p.waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            result = false;
        }
        if (!result) {
            System.err.println("CastRewriter: '" + PYTHON + " -c \"import sqlglot\"' failed — "
                    + "the :: cast rewrite is disabled for this run (pip install sqlglot to enable it)");
        }
        available = result;
        return result;
    }

    /**
     * Rewrites each SQL string's {@code ::} casts to {@code CAST(... AS ...)}, preserving order
     * and count (one output per input, always — a per-item failure falls back to that item's
     * original text, not an exception). Returns {@code sqls} unchanged if sqlglot isn't available.
     */
    static List<String> rewriteBatch(List<String> sqls) {
        if (sqls.isEmpty() || !available()) return sqls;

        try {
            Process process = new ProcessBuilder(PYTHON, "-c", PYTHON_SCRIPT).start();
            Thread writer = new Thread(() -> {
                try (OutputStream out = process.getOutputStream()) {
                    for (String sql : sqls) {
                        out.write(Base64.getEncoder().encode(sql.getBytes(StandardCharsets.UTF_8)));
                        out.write('\n');
                    }
                } catch (IOException e) {
                    // The reader loop below will see a short/empty stream and fall back per-item.
                }
            });
            writer.start();

            List<String> results = new ArrayList<>(sqls.size());
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null && results.size() < sqls.size()) {
                    results.add(new String(Base64.getDecoder().decode(line.strip()), StandardCharsets.UTF_8));
                }
            }
            writer.join();
            process.waitFor();

            // If the subprocess died early / produced fewer lines than requested, fall back to the
            // original text for whatever's missing rather than silently misaligning the rest.
            while (results.size() < sqls.size()) {
                results.add(sqls.get(results.size()));
            }
            return results;
        } catch (IOException | InterruptedException e) {
            System.err.println("CastRewriter: batch rewrite failed (" + e + ") — falling back to unrewritten SQL");
            return sqls;
        }
    }
}
