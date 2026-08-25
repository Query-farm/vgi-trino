// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fast, worker-free unit tests for {@link SqlLogicTestRunner}'s result-comparison/formatting
 * helpers — the fixes that came out of sampling real {@code QUERY_MISMATCH} failures from the
 * census (see each method's own javadoc for the specific real mismatch that motivated it).
 */
final class SqlLogicTestRunnerComparisonTest {

    @Test
    void replaceOutsideStringsSkipsStringLiteralContent() {
        assertEquals(
                "SELECT vgi_example.main.upper_case('test@example.com')",
                SqlLogicTestRunner.replaceOutsideStrings(
                        "SELECT example.main.upper_case('test@example.com')", "example.", "vgi_example."));
    }

    @Test
    void replaceOutsideStringsHandlesEscapedQuotes() {
        assertEquals(
                "SELECT vgi_example.main.f('it''s example.com')",
                SqlLogicTestRunner.replaceOutsideStrings(
                        "SELECT example.main.f('it''s example.com')", "example.", "vgi_example."));
    }

    @Test
    void formatCellRendersNullAndEmptyString() {
        assertEquals("NULL", SqlLogicTestRunner.formatCell(VarcharType.VARCHAR, null));
        assertEquals("(empty)", SqlLogicTestRunner.formatCell(VarcharType.VARCHAR, ""));
        assertEquals("hello", SqlLogicTestRunner.formatCell(VarcharType.VARCHAR, "hello"));
    }

    @Test
    void formatCellRendersVarbinaryAsHexEscapes() {
        assertEquals("\\xFF\\xEE\\xDD",
                SqlLogicTestRunner.formatCell(null, new byte[] {(byte) 0xFF, (byte) 0xEE, (byte) 0xDD}));
    }

    @Test
    void formatCellRendersARowAsAStructLiteral() {
        RowType rowType = RowType.from(List.of(
                RowType.field("lat", DoubleType.DOUBLE), RowType.field("lon", DoubleType.DOUBLE)));
        assertEquals("{'lat': 3.0, 'lon': 4.0}",
                SqlLogicTestRunner.formatCell(rowType, List.of(3.0, 4.0)));
    }

    @Test
    void cellsMatchAcceptsDuckDbUppercaseTypeNames() {
        assertTrue(SqlLogicTestRunner.cellsMatch("varchar", "VARCHAR"));
        assertTrue(SqlLogicTestRunner.cellsMatch("bigint", "BIGINT"));
    }

    @Test
    void cellsMatchAcceptsVarbinaryAsBlobAlias() {
        assertTrue(SqlLogicTestRunner.cellsMatch("varbinary", "BLOB"));
    }

    @Test
    void cellsMatchDoesNotBlanketCaseFoldOrdinaryStringData() {
        // A real casing bug (e.g. broken upper_case()) must still fail — the type-name fallback
        // only fires when the EXPECTED cell is itself a recognized DuckDB type-name token.
        assertFalse(SqlLogicTestRunner.cellsMatch("hello", "HELLO"));
        assertFalse(SqlLogicTestRunner.cellsMatch("Hello", "HELLO"));
    }

    @Test
    void cellsMatchStillRequiresExactMatchForNonTypeNameCells() {
        assertFalse(SqlLogicTestRunner.cellsMatch("42", "43"));
        assertTrue(SqlLogicTestRunner.cellsMatch("42", "42"));
    }

    @Test
    void formatCellRendersAStructsStringFieldUnquoted() {
        // Confirmed against a real sample (table/rowid.test): {'a': 0, 'b': s_0}, not
        // {'a': 0, 'b': 's_0'} -- DuckDB's struct display does not quote string field values.
        RowType rowType = RowType.from(List.of(
                RowType.field("a", BigintType.BIGINT), RowType.field("b", VarcharType.VARCHAR)));
        assertEquals("{'a': 0, 'b': s_0}", SqlLogicTestRunner.formatCell(rowType, List.of(0L, "s_0")));
    }

    @Test
    void formatCellNeverUsesScientificNotationForADouble() {
        // Confirmed against a real sample (table/projected_data.test): 11994000.0, not 1.1997E7.
        assertEquals("11994000.0", SqlLogicTestRunner.formatCell(DoubleType.DOUBLE, 1.1994E7));
        assertEquals("2.5", SqlLogicTestRunner.formatCell(DoubleType.DOUBLE, 2.5));
        assertEquals("3.0", SqlLogicTestRunner.formatCell(DoubleType.DOUBLE, 3.0));
    }

    @Test
    void sortedCopyMakesAnUnorderedGroupByComparisonOrderInsensitive() {
        // Same rows, different order -- both are equally valid results for a query with no
        // ORDER BY (confirmed against a real sample, table/partition_columns.test).
        List<List<String>> actual = List.of(List.of("CA", "2"), List.of("AU", "1"));
        List<List<String>> expected = List.of(List.of("AU", "1"), List.of("CA", "2"));
        assertFalse(SqlLogicTestRunner.rowsMatch(actual, expected), "positional comparison should differ");
        assertTrue(SqlLogicTestRunner.rowsMatch(sortedRows(actual), sortedRows(expected)));
    }

    private static List<List<String>> sortedRows(List<List<String>> rows) {
        List<List<String>> copy = new java.util.ArrayList<>(rows);
        copy.sort(java.util.Comparator.comparing(row -> String.join("", row)));
        return copy;
    }
}
