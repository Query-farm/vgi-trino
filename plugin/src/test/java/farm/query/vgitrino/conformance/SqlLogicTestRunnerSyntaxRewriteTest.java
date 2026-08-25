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
}
