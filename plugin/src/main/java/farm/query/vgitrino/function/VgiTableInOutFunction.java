// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ArgumentSpecification;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One VGI table-in-out ("blended") function's LITERAL call shape, callable
 * from Trino as {@code SELECT * FROM TABLE(catalog.schema.name(args))} —
 * confirmed against the real fixture worker that Trino's grammar requires the
 * explicit {@code TABLE(...)} wrapper even though every argument here is a
 * plain scalar (a bare {@code catalog.schema.name(args)} in a {@code FROM}
 * clause is a parse error, not an alternate call syntax).
 *
 * <h2>How this differs from a regular {@link VgiTableFunction}</h2>
 *
 * <p>A regular VGI table function is a producer: {@code bind()} once, then a
 * paginated {@code table_function_plan}/{@code init()}/tick loop scans
 * however many rows the underlying data has, across however many splits. A
 * blended function's literal call is the opposite shape — the POSITIONAL
 * arguments themselves ARE the one input row (no scan, no splits, no
 * pagination), and the worker's single {@code process()} answers with
 * whatever it computes (legally 0, 1, or many output rows — see {@code
 * VgiTableInOutSplitProcessor}) in exactly one exchange turn, since a blended
 * function is guaranteed to have no finalize phase (see {@link
 * VgiTableInOutFunctions#discover}).
 *
 * <p>Because {@link ScalarArgument#getValue()} only exists at {@code
 * analyze()}/bind time (verified against the real {@code
 * ConnectorTableFunction} SPI — same as {@link VgiTableFunction#analyze}
 * already relies on for regular table functions' bind-time constants), the
 * positional arguments' real values are resolved HERE, eagerly, and encoded
 * into a real one-row Arrow batch there and then — {@link
 * VgiTableInOutFunctionHandle#literalInputBatch()} carries the already-built
 * bytes (schema AND data) across to wherever Trino ends up running the split
 * processor, since a table function's bound handle is what actually survives
 * the coordinator/worker boundary, not any live Java state kept here.
 *
 * <p>Named arguments (VGI's {@code vgi_arg=named} convention — e.g. {@code
 * geo_encode}'s optional {@code precision}) are NOT part of the row: they
 * travel via {@code BindRequest.arguments} exactly like a regular table
 * function's arguments do, resolved once at bind time. Positional arguments
 * are explicitly never sent that way (VGI's own {@code
 * resolve_metadata}/{@code metadata.py} rejects a positional {@code
 * vgi_const} argument for exactly this reason — indistinguishable from a
 * real input column) — see {@link VgiArgSpec#positional()}.
 */
public final class VgiTableInOutFunction extends AbstractConnectorTableFunction {

    private final VgiWorkerClient client;
    private final List<VgiArgSpec> argSpecs;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param schemaName the VGI schema this function is registered in
     * @param functionName the function name
     * @param argSpecs the function's decoded, supported arguments, in
     *        declaration order (positional-then-named, per VGI's own wire
     *        convention — see {@link VgiArgSpec})
     */
    public VgiTableInOutFunction(VgiWorkerClient client, String schemaName, String functionName,
            List<VgiArgSpec> argSpecs) {
        super(schemaName, functionName, toArgumentSpecifications(argSpecs),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.client = client;
        this.argSpecs = argSpecs;
    }

    private static List<ArgumentSpecification> toArgumentSpecifications(List<VgiArgSpec> argSpecs) {
        List<ArgumentSpecification> out = new ArrayList<>(argSpecs.size());
        for (VgiArgSpec spec : argSpecs) {
            ScalarArgumentSpecification.Builder builder = ScalarArgumentSpecification.builder()
                    .name(spec.name().toUpperCase(Locale.ROOT))
                    .type(spec.type());
            if (spec.hasDefault()) builder.defaultValue(spec.defaultValue());
            out.add(builder.build());
        }
        return out;
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl) {
        return client.withConnection(a -> {
            // Positional args become the literal input row's columns; named
            // args stay on BindRequest.arguments, exactly like a regular
            // VgiTableFunction's call — see this class's own javadoc.
            List<Field> rowFields = new ArrayList<>();
            List<VgiArgSpec> rowSpecs = new ArrayList<>();
            List<Object> rowValues = new ArrayList<>();
            ArgumentsEncoder namedEncoder = ArgumentsEncoder.builder();
            for (VgiArgSpec spec : argSpecs) {
                Argument argument = arguments.get(spec.name().toUpperCase(Locale.ROOT));
                Object value = argument instanceof ScalarArgument scalar ? scalar.getValue() : null;
                if (spec.positional()) {
                    rowFields.add(VgiTypeMapping.toArrowField(spec.type(), spec.name()));
                    rowSpecs.add(spec);
                    rowValues.add(value);
                } else if (value != null) {
                    namedEncoder.named(spec.name(), toEncodableValue(spec.type(), value));
                }
            }
            Schema inputSchema = new Schema(rowFields);
            byte[] literalInputBatch;
            try (VectorSchemaRoot input = VectorSchemaRoot.create(inputSchema, Allocators.root())) {
                input.allocateNew();
                for (int i = 0; i < rowSpecs.size(); i++) {
                    FieldVector vector = input.getVector(i);
                    VgiTypeMapping.writeValue(rowSpecs.get(i).type(), vector, 0, rowValues.get(i));
                }
                for (FieldVector vector : input.getFieldVectors()) vector.setValueCount(1);
                input.setRowCount(1);
                literalInputBatch = ArrowSchemaCodec.serializeBatch(input);
            }

            BindRequest bindRequest = new BindRequest(
                    getName(),
                    namedEncoder.encode(),
                    "TABLE",
                    ArrowSchemaCodec.serializeSchema(inputSchema), // input_schema — the literal row's shape
                    null,           // settings
                    null,           // secrets
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    false,          // resolved_secrets_provided
                    null, null,     // at_unit / at_value — no time travel for table-in-out
                    null, null,     // copy_from / copy_to
                    getSchema());
            BindResponse bound = a.service().bind(bindRequest, null);
            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);

            Schema outputSchema = ArrowSchemaCodec.deserializeSchema(bound.output_schema());
            List<String> columnNames = new ArrayList<>(outputSchema.getFields().size());
            List<io.trino.spi.type.Type> columnTypes = new ArrayList<>(outputSchema.getFields().size());
            for (Field field : outputSchema.getFields()) {
                columnNames.add(field.getName());
                columnTypes.add(VgiTypeMapping.toTrinoType(field));
            }

            VgiTableInOutFunctionHandle handle = new VgiTableInOutFunctionHandle(
                    serializedBindCall, bound.opaque_data(), bound.output_schema(), literalInputBatch);
            return TableFunctionAnalysis.builder()
                    .returnedType(Descriptor.descriptor(columnNames, columnTypes))
                    .handle(handle)
                    .build();
        });
    }

    /**
     * {@code ScalarArgument.getValue()} comes back in the type's own
     * native/internal representation ({@code Slice} for VARCHAR, raw int bits
     * as a {@code long} for REAL) — not the plain Java value {@code
     * ArgumentsEncoder}/{@code ScalarValue.of} know how to infer an Arrow type
     * from. Mirrors {@link VgiTableFunction#toEncodableValue} exactly (named
     * args only — a positional arg's value goes through {@link
     * VgiTypeMapping#writeValue} instead, which already expects this same
     * native representation as-is).
     */
    private static Object toEncodableValue(io.trino.spi.type.Type type, Object value) {
        if (type instanceof io.trino.spi.type.VarcharType && value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type instanceof io.trino.spi.type.RealType && value instanceof Long bits) {
            return (double) Float.intBitsToFloat(bits.intValue());
        }
        return value;
    }
}
