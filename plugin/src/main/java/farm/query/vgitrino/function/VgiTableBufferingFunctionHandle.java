// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

/**
 * A bound call of a VGI {@code TableBufferingFunction} — the third, distinct VGI table-in-out
 * kind (alongside blended {@link VgiTableInOutFunction} and classic {@link
 * VgiTableInOutTableFunction}), e.g. {@code SELECT * FROM
 * TABLE(cat.main.sum_all_columns(TABLE(some_query)))}. See {@link VgiTableBufferingFunction}'s own
 * javadoc for the Sink+Combine+Source protocol this drives.
 *
 * <p>A plain record — see {@link farm.query.vgitrino.metadata.VgiTableHandle} for why no Jackson
 * annotations are needed.
 *
 * @param bindCall the serialised {@code BindRequest} this call was bound with — its {@code
 *        input_schema} is the TABLE argument's required-columns schema
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param outputSchema this call's IPC-encoded output schema
 * @param inputSchema the IPC-encoded schema every page {@code VgiTableBufferingDataProcessor}
 *        receives must be converted against — the same schema bytes already embedded in {@code
 *        bindCall.input_schema}, carried separately so the processor doesn't need to re-parse the
 *        whole {@code BindRequest} just to recover it
 * @param schemaName the VGI schema this function is registered in — required on every unary
 *        {@code table_buffering_process}/{@code table_buffering_combine}/{@code
 *        table_buffering_destructor} request as a TOP-LEVEL field (unlike {@code init}'s {@code
 *        InitRequest}, which resolves it implicitly from the embedded {@code bind_call}; see the
 *        real worker's {@code _load_table_buffering_params}, which reads {@code schema_name} off
 *        the request itself for these three RPCs specifically)
 * @param functionName the function name, threaded the same way as {@code schemaName} for the same
 *        reason — every unary {@code table_buffering_*} request names it directly
 */
public record VgiTableBufferingFunctionHandle(
        byte[] bindCall, byte[] bindOpaqueData, byte[] outputSchema, byte[] inputSchema,
        String schemaName, String functionName)
        implements ConnectorTableFunctionHandle {
}
