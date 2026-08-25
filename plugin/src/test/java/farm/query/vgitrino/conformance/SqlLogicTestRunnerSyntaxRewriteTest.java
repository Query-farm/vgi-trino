// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Fast, worker-free unit tests for {@link SqlLogicTestRunner#rewriteDuckDbOnlySyntax}'s two
 * textual rewrites, isolated from the expensive {@link VgiSqlLogicTestCensusTest}/{@link
 * VgiSqlLogicTestConformanceTest} runs that exercise it against a live worker.
 */
final class SqlLogicTestRunnerSyntaxRewriteTest {

    private static final String CATALOG = "vgi_example";

    @Test
    void wrapsABareTableFunctionCallAfterFrom() {
        assertEquals(
                "SELECT * FROM TABLE(vgi_example.main.sequence(10))",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT * FROM vgi_example.main.sequence(10)", CATALOG));
    }

    @Test
    void rewritesNamedArgumentSyntax() {
        assertEquals(
                "SELECT * FROM TABLE(vgi_example.main.sequence(10, batch_size => 0))",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax(
                        "SELECT * FROM vgi_example.main.sequence(10, batch_size := 0)", CATALOG));
    }

    @Test
    void wrapsABareTableFunctionCallAfterJoin() {
        assertEquals(
                "SELECT * FROM t JOIN TABLE(vgi_example.main.sequence(10)) ON true",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax(
                        "SELECT * FROM t JOIN vgi_example.main.sequence(10) ON true", CATALOG));
    }

    @Test
    void leavesAnAlreadyWrappedCallAlone() {
        String sql = "SELECT * FROM TABLE(vgi_example.main.sequence(10))";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }

    @Test
    void leavesAPlainScalarCallInTheSelectListAlone() {
        String sql = "SELECT vgi_example.main.double(21)";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }

    @Test
    void handlesNestedParensAndStringLiteralsInArguments() {
        assertEquals(
                "SELECT * FROM TABLE(vgi_example.main.f('a)b', g(1)))",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT * FROM vgi_example.main.f('a)b', g(1))", CATALOG));
    }

    @Test
    void doesNotTouchACallFromAnUnrelatedCatalog() {
        String sql = "SELECT * FROM other_catalog.main.sequence(10)";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }

    @Test
    void handlesMultipleCallsInOneQuery() {
        assertEquals(
                "SELECT * FROM TABLE(vgi_example.main.a(1)) JOIN TABLE(vgi_example.main.b(2)) ON true",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax(
                        "SELECT * FROM vgi_example.main.a(1) JOIN vgi_example.main.b(2) ON true", CATALOG));
    }

    @Test
    void rewritesOneArgRangeToUnnestSequence() {
        assertEquals(
                "SELECT i FROM UNNEST(SEQUENCE(0, (10) - 1, 1)) t(i)",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT i FROM range(10) t(i)", CATALOG));
    }

    @Test
    void rewritesTwoArgRangeToUnnestSequence() {
        assertEquals(
                "SELECT i FROM UNNEST(SEQUENCE(5, (10) - 1, 1)) t(i)",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT i FROM range(5, 10) t(i)", CATALOG));
    }

    @Test
    void preservesTheColumnAliasUnlikeSqlglotsBuiltinRule() {
        // The whole point of hand-rolling this: sqlglot's own duckdb->trino range() rule drops
        // the column alias and reuses the table alias as the column name instead (confirmed by
        // direct testing), so "SELECT i FROM range(10) t(i)" becomes an unresolvable "i" column
        // reference. This rewrite never touches the alias clause at all, so it can't break it.
        String rewritten = SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT i FROM range(10) t(i)", CATALOG);
        assertEquals("SELECT i FROM UNNEST(SEQUENCE(0, (10) - 1, 1)) t(i)", rewritten);
    }

    @Test
    void rewritesRangeWithANonLiteralStopExpression() {
        assertEquals(
                "SELECT i FROM UNNEST(SEQUENCE(0, (i % 5) - 1, 1)) t(i)",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT i FROM range(i % 5) t(i)", CATALOG));
    }

    @Test
    void rewritesGenerateSeriesWithoutTheMinusOneAdjustment() {
        assertEquals(
                "SELECT x FROM UNNEST(SEQUENCE(1, 20, 1)) t(x)",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT x FROM generate_series(1, 20) t(x)", CATALOG));
    }

    @Test
    void insertsTheDefaultSchemaForATwoPartFunctionCall() {
        assertEquals(
                "SELECT vgi_example.main.double(3)",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT vgi_example.double(3)", CATALOG));
    }

    @Test
    void leavesAnAlreadyThreePartCallAlone() {
        String sql = "SELECT vgi_example.main.double(3)";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }

    @Test
    void insertsTheDefaultSchemaInsideAnAggregateCallTooNotJustAfterFrom() {
        assertEquals(
                "SELECT sum(vgi_example.main.double(v)) FROM t",
                SqlLogicTestRunner.rewriteDuckDbOnlySyntax("SELECT sum(vgi_example.double(v)) FROM t", CATALOG));
    }

    @Test
    void doesNotTouchATwoPartReferenceWithNoFollowingParen() {
        // Not a function call at all — out of scope (see insertDefaultSchema's own javadoc).
        String sql = "SELECT vgi_example.somecolumn FROM t";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }

    @Test
    void leavesAThreeArgRangeCallAlone() {
        // Not observed in the real corpus, and the explicit-step arithmetic is genuinely harder
        // to get right (DuckDB's step-aware exclusive-stop boundary isn't just "stop - step" in
        // general) — left unrewritten rather than risk a silently wrong translation.
        String sql = "SELECT i FROM range(0, 10, 2) t(i)";
        assertEquals(sql, SqlLogicTestRunner.rewriteDuckDbOnlySyntax(sql, CATALOG));
    }
}
