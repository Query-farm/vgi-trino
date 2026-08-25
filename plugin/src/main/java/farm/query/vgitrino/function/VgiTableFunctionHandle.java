// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

/**
 * A bound call of a VGI table function: the result of {@code bind()} for this
 * specific invocation's arguments, ready to plan/redeem splits against.
 *
 * <p>A plain record — see {@link farm.query.vgitrino.metadata.VgiTableHandle}
 * for why no Jackson annotations are needed.
 *
 * @param bindCall the serialised {@code BindRequest} this call was bound with
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param outputSchema this call's IPC-encoded output schema (may depend on
 *        the arguments — {@code constant_columns(n, *values)}'s column types
 *        follow {@code values}, for instance — so this is bind()'s own
 *        answer, not the function's static {@code FunctionInfo.output_schema})
 */
public record VgiTableFunctionHandle(
        byte[] bindCall, byte[] bindOpaqueData, byte[] outputSchema)
        implements ConnectorTableFunctionHandle {
}
