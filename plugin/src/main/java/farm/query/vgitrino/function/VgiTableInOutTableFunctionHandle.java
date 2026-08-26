// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

/**
 * A bound call of a VGI CLASSIC (non-blended) table-in-out function — one
 * with a real {@code TableInput} argument, e.g. {@code SELECT * FROM
 * cat.main.echo(input => TABLE(some_query))}. See {@link
 * VgiTableInOutTableFunction}'s own javadoc for how this differs from both a
 * regular {@link VgiTableFunction} (producer-mode scan) and a blended {@link
 * VgiTableInOutFunction} (literal-call exchange).
 *
 * <p>A plain record — see {@link farm.query.vgitrino.metadata.VgiTableHandle}
 * for why no Jackson annotations are needed.
 *
 * @param bindCall the serialised {@code BindRequest} this call was bound with — its
 *        {@code input_schema} is the TABLE argument's required-columns schema
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param outputSchema this call's IPC-encoded output schema
 * @param inputSchema the IPC-encoded schema every page {@code
 *        VgiTableInOutDataProcessor} receives must be converted against — the
 *        same schema bytes already embedded in {@code bindCall.input_schema},
 *        carried separately so the processor doesn't need to re-parse the
 *        whole {@code BindRequest} just to recover it
 */
public record VgiTableInOutTableFunctionHandle(
        byte[] bindCall, byte[] bindOpaqueData, byte[] outputSchema, byte[] inputSchema)
        implements ConnectorTableFunctionHandle {
}
