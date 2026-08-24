// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import org.apache.arrow.vector.types.pojo.Field;

/**
 * The SQL-facing name and visibility for one Arrow field.
 *
 * <p>A field carrying VGI's {@code is_row_id} metadata (any value, presence
 * is all that matters — see {@code vgi.schema_utils.schema()}'s
 * {@code row_id=(type, {b"is_row_id": b""})} convention) is DuckDB's own
 * {@code rowid} pseudo-column: renamed to the literal name {@code rowid}
 * regardless of its underlying Arrow field name, and hidden from
 * {@code SELECT *}. Trino has an equivalent mechanism —
 * {@code ColumnMetadata.builder().setHidden(true)} — built for exactly this
 * kind of connector-native pseudo-column (Hive's {@code $path}, for
 * instance), so this mirrors DuckDB's behaviour rather than inventing a
 * different one.
 */
public final class VgiColumnNames {

    private static final String ROW_ID_METADATA_KEY = "is_row_id";
    private static final String ROW_ID_DISPLAY_NAME = "rowid";

    private VgiColumnNames() {}

    /**
     * @param field the Arrow field
     * @return {@code true} if this field carries VGI's row-id metadata
     */
    public static boolean isRowId(Field field) {
        return field.getMetadata() != null && field.getMetadata().containsKey(ROW_ID_METADATA_KEY);
    }

    /**
     * The SQL-facing column name: {@code rowid} for a row-id field,
     * otherwise the field's own name unchanged.
     *
     * @param field the Arrow field
     * @return the display name
     */
    public static String displayName(Field field) {
        return isRowId(field) ? ROW_ID_DISPLAY_NAME : field.getName();
    }
}
