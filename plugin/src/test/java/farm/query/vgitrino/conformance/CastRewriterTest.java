// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises {@link CastRewriter} against the real sqlglot subprocess. Skips (not fails) if
 * sqlglot isn't available in this environment — see the class javadoc for how to install it.
 */
final class CastRewriterTest {

    @Test
    void rewritesSimpleCastsAndPreservesOrder() {
        Assumptions.assumeTrue(CastRewriter.available(), "sqlglot not available — see CastRewriter javadoc");

        List<String> input = List.of(
                "SELECT vgi_example.main.vgi_sum(i::BIGINT) FROM UNNEST(SEQUENCE(0, (10) - 1, 1)) t(i)",
                "SELECT 1.00::DECIMAL(10,2)",
                "SELECT '2026-05-06 14:00:00+02'::TIMESTAMPTZ",
                "SELECT * FROM TABLE(vgi_example.main.sequence(10, batch_size => 0))"); // no cast at all

        List<String> out = CastRewriter.rewriteBatch(input);

        assertEquals(4, out.size());
        assertEquals(
                "SELECT vgi_example.main.vgi_sum(CAST(i AS BIGINT)) FROM UNNEST(SEQUENCE(0, (10) - 1, 1)) AS t(i)",
                out.get(0));
        assertEquals("SELECT CAST(1.00 AS DECIMAL(10, 2))", out.get(1));
        assertEquals("SELECT CAST('2026-05-06 14:00:00+02' AS TIMESTAMP WITH TIME ZONE)", out.get(2));
        // Untouched — no :: cast present, and the already-Trino TABLE(...)/=> syntax must survive.
        assertEquals("SELECT * FROM TABLE(vgi_example.main.sequence(10, batch_size => 0))", out.get(3));
    }

    @Test
    void emptyBatchReturnsEmptyWithoutSpawningAProcess() {
        assertEquals(List.of(), CastRewriter.rewriteBatch(List.of()));
    }
}
