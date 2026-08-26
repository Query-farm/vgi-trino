// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;

/**
 * A bound call of a VGI table-in-out (blended) function's LITERAL call shape —
 * {@code SELECT * FROM cat.main.forecast_current(52.52, 13.41)}, constant
 * arguments only. See {@link VgiTableInOutFunction}'s own javadoc for why this
 * is a materially different wire interaction than a regular {@link
 * VgiTableFunction}'s producer-mode scan, and {@link VgiTableInOutFunctions}
 * for why only the literal shape (never column/LATERAL streaming) is
 * representable through Trino's table-function SPI at all.
 *
 * <p>A plain record — see {@link farm.query.vgitrino.metadata.VgiTableHandle}
 * for why no Jackson annotations are needed.
 *
 * @param bindCall the serialised {@code BindRequest} this call was bound with (with a real,
 *        non-null {@code input_schema} — the positional arguments' declared row shape)
 * @param bindOpaqueData the matching {@code BindResponse.opaque_data}, or {@code null}
 * @param outputSchema this call's IPC-encoded output schema
 * @param literalInputBatch the ONE-ROW input batch — schema plus real data, IPC-encoded via
 *        {@link farm.query.vgitrino.types.ArrowSchemaCodec#serializeBatch} — built from the
 *        positional arguments' actual bound VALUES at {@code analyze()} time (see {@link
 *        VgiTableInOutFunction#analyze}); {@code Argument.getValue()} is only available then, so
 *        it cannot be deferred to split-processor time the way a regular table function's
 *        producer-mode scan defers all its row production
 */
public record VgiTableInOutFunctionHandle(
        byte[] bindCall, byte[] bindOpaqueData, byte[] outputSchema, byte[] literalInputBatch)
        implements ConnectorTableFunctionHandle {
}
