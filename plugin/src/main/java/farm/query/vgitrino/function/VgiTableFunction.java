// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgirpc.marshal.RecordCodec;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One VGI table function, callable from Trino as
 * {@code TABLE(catalog.schema.name(args))}.
 *
 * <p>v1 scope: every argument must be a plain scalar (no {@code any}-typed,
 * TABLE-input, or varargs argument — {@link VgiArgSpec#decode} returns
 * {@code null} for those, and {@link VgiTableFunctions#discover} skips
 * registering the whole function rather than register a wrong signature).
 * The return type is declared {@link ReturnTypeSpecification.GenericTable}
 * because a function's actual output columns can depend on its arguments
 * (e.g. {@code constant_columns(n, *values)}'s column types follow
 * {@code values}) — {@link #analyze} calls the real {@code bind()} for this
 * invocation and reports the real {@link Descriptor} there.
 */
public final class VgiTableFunction extends AbstractConnectorTableFunction {

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
    public VgiTableFunction(VgiWorkerClient client, String schemaName, String functionName,
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
            ArgumentsEncoder encoder = ArgumentsEncoder.builder();
            // Iterate OUR declared order, not the (unordered) arguments map:
            // a positional arg must reach ArgumentsEncoder.positional() in
            // position order regardless of how the Trino caller wrote it —
            // `count => 10` is a NAME at the SQL call site, but VGI's own
            // ArgumentsParser still expects it in the positional wire slot
            // ("index out of range" otherwise), since VGI's wire shape
            // distinguishes positional/named independently of Trino's.
            for (VgiArgSpec spec : argSpecs) {
                Argument argument = arguments.get(spec.name().toUpperCase(Locale.ROOT));
                if (!(argument instanceof ScalarArgument scalar) || scalar.getValue() == null) continue;
                Object value = toEncodableValue(scalar);
                if (spec.positional()) {
                    encoder.positional(value);
                } else {
                    encoder.named(spec.name(), value);
                }
            }
            BindRequest bindRequest = new BindRequest(
                    getName(),
                    encoder.encode(),
                    "TABLE",
                    null,           // input_schema — producer-mode table function
                    null,           // settings
                    null,           // secrets
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    false,          // resolved_secrets_provided
                    null, null,     // at_unit / at_value
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

            VgiTableFunctionHandle handle = new VgiTableFunctionHandle(
                    serializedBindCall, bound.opaque_data(), bound.output_schema());
            return TableFunctionAnalysis.builder()
                    .returnedType(Descriptor.descriptor(columnNames, columnTypes))
                    .handle(handle)
                    .build();
        });
    }

    /**
     * {@code ScalarArgument.getValue()} comes back in the type's own
     * native/internal representation ({@code Slice} for VARCHAR, raw int bits
     * as a {@code long} for REAL — the same representation
     * {@code Type.getJavaType()} names) — not the plain Java value
     * {@code ArgumentsEncoder}/{@code ScalarValue.of} know how to infer an
     * Arrow type from. Convert the handful of shapes VGI's own arg specs use.
     */
    private static Object toEncodableValue(ScalarArgument scalar) {
        Object value = scalar.getValue();
        io.trino.spi.type.Type type = scalar.getType();
        if (type instanceof io.trino.spi.type.VarcharType && value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type instanceof io.trino.spi.type.RealType && value instanceof Long bits) {
            return (double) Float.intBitsToFloat(bits.intValue());
        }
        return value;
    }
}
