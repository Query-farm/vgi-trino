// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.metadata;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

/**
 * A bound VGI table: which schema/table, and the scan function + arguments
 * {@code catalog_table_scan_function_get} resolved for it.
 *
 * <p>A plain record — Jackson's built-in {@code java.lang.Record} support
 * (active since jackson-databind 2.12, no module needed) (de)serialises it
 * across the coordinator/worker boundary with no {@code @JsonCreator}
 * boilerplate, the same way Trino's own newer connectors do.
 *
 * @param schemaName the VGI schema this table lives in
 * @param tableName the table name
 * @param scanFunctionName the table function {@code catalog_table_scan_function_get}
 *        named to actually perform the scan
 * @param scanFunctionArguments the scan function's bound arguments, already
 *        re-encoded as {@code BindRequest.arguments} bytes (see
 *        {@code farm.query.vgi.client.ScanFunctionArguments#toBindArguments})
 * @param outputSchema the table's full (unprojected) Arrow schema, IPC-encoded
 * @param cardinalityEstimate the worker's own row-count estimate
 *        ({@code TableInfo.cardinality_estimate}), or {@code null} if it
 *        offered none — fed straight to {@code getTableStatistics} with no
 *        extra RPC, since {@code catalog_table_get} already returned it
 * @param constraint the predicate {@code applyFilter} has accepted so far
 *        (informational only — never declared exactly applied, so Trino
 *        always re-checks every row regardless of what the worker did with
 *        it; see {@code farm.query.vgitrino.filter.VgiFilterTranslator})
 * @param atUnit the resolved {@code FOR VERSION/TIMESTAMP AS OF} clause's unit
 *        ({@code "VERSION"}/{@code "TIMESTAMP"}), or {@code null} for a plain
 *        (non-time-travel) read — see {@code VgiTimeTravel}
 * @param atValue the resolved AT clause's value, or {@code null} exactly when
 *        {@code atUnit} is {@code null} (VGI requires both or neither)
 */
public record VgiTableHandle(
        String schemaName,
        String tableName,
        String scanFunctionName,
        byte[] scanFunctionArguments,
        byte[] outputSchema,
        Long cardinalityEstimate,
        TupleDomain<VgiColumnHandle> constraint,
        String atUnit,
        String atValue) implements ConnectorTableHandle {
}
