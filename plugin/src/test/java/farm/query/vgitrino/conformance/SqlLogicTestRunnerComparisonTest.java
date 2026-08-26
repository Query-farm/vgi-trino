// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.conformance;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
    void formatCellRendersAMaterializedRowAsAStructLiteralToo() {
        // A REAL query executed via DistributedQueryRunner materializes a RowType value as
        // io.trino.testing.MaterializedRow, NOT a plain java.util.List (MaterializedRow is its own
        // wrapper class -- getFields()/getField(int), no List superinterface at all). The
        // List-only check above missed this entirely, so every live struct-returning query fell
        // through to MaterializedRow's OWN toString() ("[3.0, 4.0]") -- confirmed against the real
        // fixture (scalar/geo_centroid.test's geo_centroid_struct/geo_centroid_list), which is
        // exactly the originally-reported "returns an array instead of a struct" symptom.
        RowType rowType = RowType.from(List.of(
                RowType.field("lat", DoubleType.DOUBLE), RowType.field("lon", DoubleType.DOUBLE)));
        MaterializedRow row = new MaterializedRow(List.<Object>of(3.0, 4.0));
        assertEquals("{'lat': 3.0, 'lon': 4.0}", SqlLogicTestRunner.formatCell(rowType, row));
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
    void formatCellRendersATimestampWithWholeSecondsSpaceSeparatedAndFullHms() {
        // Confirmed against a real sample (filter_pushdown/temporal.test, via a live-worker
        // replay): expected "2026-05-06 12:00:00", this harness previously rendered
        // "2026-05-06T12:00" (Java's LocalDateTime#toString() uses a 'T' separator and drops
        // trailing-zero seconds/minutes components entirely).
        assertEquals("2026-05-06 12:00:00",
                SqlLogicTestRunner.formatCell(null, LocalDateTime.of(2026, 5, 6, 12, 0, 0)));
    }

    @Test
    void formatCellRendersATimestampsMicrosecondFractionUntruncated() {
        // Confirmed against a real sample (filter_pushdown/temporal.test): expected
        // "2026-05-06 12:34:56.123456", this harness previously rendered
        // "2026-05-06T12:34:56.123" — not just the 'T'/space difference, but real precision loss
        // from Java's LocalDateTime#toString() 3/6/9-digit grouping.
        assertEquals("2026-05-06 12:34:56.123456",
                SqlLogicTestRunner.formatCell(null,
                        LocalDateTime.of(2026, 5, 6, 12, 34, 56, 123_456_000)));
    }

    @Test
    void formatCellRendersATimestampsMillisecondFractionWithoutPaddingToMicroseconds() {
        // Confirmed against a real sample (filter_pushdown/temporal.test's TIMESTAMP_MS case) and
        // a real `duckdb` CLI session (CAST('...00.100' AS TIMESTAMP) displays as ".1", not
        // ".100000"): DuckDB strips trailing zeros down to the significant digits rather than
        // padding a millisecond-precision value out to six digits.
        assertEquals("2026-05-06 12:00:00.456",
                SqlLogicTestRunner.formatCell(null,
                        LocalDateTime.of(2026, 5, 6, 12, 0, 0, 456_000_000)));
    }

    @Test
    void formatCellRendersATimestampWithNoFractionalSecondAtAll() {
        assertEquals("2026-05-06 12:34:56",
                SqlLogicTestRunner.formatCell(null, LocalDateTime.of(2026, 5, 6, 12, 34, 56)));
    }

    @Test
    void formatCellRendersATimestampWithTimeZoneAtAWholeHourOffsetWithABareOffset() {
        // Confirmed against a real sample (filter_pushdown/temporal.test): expected
        // "2026-05-06 12:00:00+00", this harness previously rendered "2026-05-06T12:00Z[UTC]" for
        // a UTC offset and "2026-05-06T13:00+01:00" for a real Java ZonedDateTime#toString() at
        // +01:00 -- DuckDB never shows a zone-id bracket, and never pads a whole-hour offset with
        // ":00" minutes.
        assertEquals("2026-05-06 12:00:00+00",
                SqlLogicTestRunner.formatCell(null,
                        ZonedDateTime.of(2026, 5, 6, 12, 0, 0, 0, ZoneOffset.UTC)));
        assertEquals("2026-05-06 13:00:00+01",
                SqlLogicTestRunner.formatCell(null,
                        ZonedDateTime.of(2026, 5, 6, 13, 0, 0, 0, ZoneOffset.ofHours(1))));
    }

    @Test
    void formatCellRendersATimestampWithTimeZoneAtANonWholeHourOffsetWithMinutes() {
        // Confirmed against a real `duckdb` CLI session with TimeZone set to Asia/Kolkata: a
        // +05:30 offset prints with minutes, unlike a whole-hour offset's bare form.
        assertEquals("2026-05-06 12:00:00+05:30",
                SqlLogicTestRunner.formatCell(null,
                        ZonedDateTime.of(2026, 5, 6, 12, 0, 0, 0, ZoneOffset.ofHoursMinutes(5, 30))));
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
