// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionRequiredSecret;
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
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One VGI CLASSIC (non-blended) table-in-out function, callable from Trino as
 * {@code SELECT * FROM TABLE(catalog.schema.name(input => TABLE(some_query)))}
 * — a real {@code TableInput} argument (VGI's other table-in-out kind,
 * alongside the blended/literal-call one already implemented in {@link
 * VgiTableInOutFunction}).
 *
 * <h2>How this differs from the other two table-function kinds</h2>
 *
 * <p>A regular {@link VgiTableFunction} is a producer with no input at all —
 * {@code table_function_plan}/split/tick. A blended {@link
 * VgiTableInOutFunction}'s literal call resolves its ENTIRE input (the
 * positional arguments' constant values) once, eagerly, at {@code analyze()}
 * time. This class's classic shape is neither: its {@code TableInput}
 * argument's real rows are streamed in incrementally, DURING execution, from
 * a real (and, to this connector, entirely opaque — see below) upstream
 * subplan — the same fact that makes this connector's implementation route
 * through Trino's OTHER table-function SPI surface entirely,
 * {@code TableFunctionDataProcessor} (via {@link VgiTableInOutDataProcessor}),
 * rather than {@code TableFunctionSplitProcessor}: verified directly against
 * the real Trino engine (io.trino.operator.LocalExecutionPlanner
 * .visitTableFunctionProcessor) that a function with ANY {@code
 * TableArgumentSpecification} is ALWAYS routed through the data-processor/
 * page-driven path, never the split-driven one — there is no split
 * enumeration for this kind of call at all, so {@code VgiSplitManager} is
 * never involved.
 *
 * <p>Because {@code TableArgument} (verified against the real SPI) carries
 * only a {@code RowType} and {@code PARTITION BY}/{@code ORDER BY} column
 * names — no {@code ConnectorTableHandle}, no reference to the source
 * connector at all — {@code analyze()} can declare which columns it needs
 * ({@link TableFunctionAnalysis#getRequiredColumns()}, mapped onto VGI's own
 * bind-time {@code input_schema}) but cannot push anything else (filters,
 * limits) into wherever those rows actually come from; the engine delivers
 * them later as plain {@link io.trino.spi.Page}s, already narrowed to just
 * those columns. V1 scope always requires every column VGI's own {@code
 * TableInput} declares — VGI's wire protocol has no partial-projection
 * concept of its own for this input shape.
 *
 * <p>Every VGI classic table-in-out function has EXACTLY ONE {@code
 * TableInput} argument (VGI's own {@code resolve_metadata} enforces this),
 * possibly alongside ordinary scalar/named arguments in any position (e.g.
 * {@code repeat_inputs(repeat_count, data)} — the {@code TableInput} isn't
 * always argument 0) — {@link VgiArgSpec#tableArg} marks which one, decoded
 * exactly like a regular table function's other arguments (see {@link
 * VgiTableFunctions#discover}'s doc for the discovery-time skip that routes a
 * function here instead of there).
 *
 * <p>The registered {@link TableArgumentSpecification} uses {@code
 * .keepWhenEmpty()} (not {@code .rowSemantics()}) — matching Trino's own
 * reference {@code IdentityFunction}/{@code RepeatFunction} test
 * implementations exactly: no {@code PARTITION BY}/{@code ORDER BY}
 * requirement or restriction on the caller, and the whole relation becomes
 * one logical partition when the caller specifies neither (VGI's own
 * classic table-in-out has no partition/order concept at all, so this is
 * the closest honest match). No {@code passThroughColumns()} either — a VGI
 * worker computes and emits real output values itself (echo, filter,
 * repeat), never Trino's index-based pass-through splicing.
 *
 * <p>{@code required_settings}/{@code required_secrets} (e.g. {@code
 * filter_by_setting}'s {@code threshold}) resolve here, in {@link #analyze},
 * exactly like a scalar function's — reusing {@link
 * VgiScalarFunctions.BindCache#resolveSettings}/{@link
 * VgiScalarFunctions.BindCache#resolveSecretFields} verbatim, since {@code
 * analyze()} already receives a real {@link ConnectorSession} directly (a
 * scalar function's {@code FunctionProvider} does not, which is the entire
 * reason {@code BindCache} exists there — no such cache is needed here).
 * {@code has_finalize=true} functions register too — {@link
 * VgiTableInOutDataProcessor} drives the second {@code
 * init(phase=FINALIZE)} turn.
 */
public final class VgiTableInOutTableFunction extends AbstractConnectorTableFunction {

    private final VgiWorkerClient client;
    private final List<VgiArgSpec> argSpecs;
    private final boolean hasFinalize;
    private final List<String> requiredSettings;
    private final List<FunctionRequiredSecret> requiredSecrets;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param schemaName the VGI schema this function is registered in
     * @param functionName the function name
     * @param argSpecs the function's decoded, supported arguments, in
     *        declaration order — exactly one of which has {@link
     *        VgiArgSpec#tableArg()} set
     * @param hasFinalize whether this function has a finalize phase (see
     *        {@link VgiTableInOutTableFunctionHandle#hasFinalize})
     * @param requiredSettings {@code FunctionInfo.required_settings} verbatim
     * @param requiredSecrets {@code FunctionInfo.required_secrets} verbatim
     */
    public VgiTableInOutTableFunction(VgiWorkerClient client, String schemaName, String functionName,
            List<VgiArgSpec> argSpecs, boolean hasFinalize, List<String> requiredSettings,
            List<FunctionRequiredSecret> requiredSecrets) {
        super(schemaName, functionName, toArgumentSpecifications(argSpecs),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.client = client;
        this.argSpecs = argSpecs;
        this.hasFinalize = hasFinalize;
        this.requiredSettings = requiredSettings;
        this.requiredSecrets = requiredSecrets;
    }

    /**
     * @return this function's {@code required_settings} names — what {@link
     *         VgiConnector#getSessionProperties()} unions across every
     *         discovered function (scalar and classic table-in-out alike) to
     *         declare as this catalog's Trino session properties
     */
    public List<String> requiredSettingNames() {
        return requiredSettings;
    }

    private static List<ArgumentSpecification> toArgumentSpecifications(List<VgiArgSpec> argSpecs) {
        List<ArgumentSpecification> out = new ArrayList<>(argSpecs.size());
        for (VgiArgSpec spec : argSpecs) {
            String upperName = spec.name().toUpperCase(Locale.ROOT);
            if (spec.tableArg()) {
                out.add(TableArgumentSpecification.builder().name(upperName).keepWhenEmpty().build());
                continue;
            }
            ScalarArgumentSpecification.Builder builder = ScalarArgumentSpecification.builder()
                    .name(upperName)
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
            ArgumentsEncoder scalarEncoder = ArgumentsEncoder.builder();
            Schema inputSchema = null;
            String tableArgName = null;
            for (VgiArgSpec spec : argSpecs) {
                String upperName = spec.name().toUpperCase(Locale.ROOT);
                Argument argument = arguments.get(upperName);
                if (spec.tableArg()) {
                    // Every VGI classic function has exactly one of these (enforced server-side) —
                    // its real row shape, not any single call's actual data (that arrives later, via
                    // pages, at VgiTableInOutDataProcessor).
                    inputSchema = toArrowSchema(((TableArgument) argument).getRowType());
                    tableArgName = upperName;
                    continue;
                }
                if (!(argument instanceof ScalarArgument scalar) || scalar.getValue() == null) continue;
                Object value = toEncodableValue(spec.type(), scalar.getValue());
                if (spec.positional()) scalarEncoder.positional(value);
                else scalarEncoder.named(spec.name(), value);
            }

            byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(inputSchema);

            Map<String, String> resolvedSettings =
                    VgiScalarFunctions.BindCache.resolveSettings(requiredSettings, session);
            Map<String, String> resolvedSecretFields =
                    VgiScalarFunctions.BindCache.resolveSecretFields(requiredSecrets, session);
            byte[] settingsBytes = resolvedSettings.isEmpty() ? null : SettingsEncoder.of(resolvedSettings);
            byte[] secretsBytes = VgiScalarFunctions.BindCache.encodeSecrets(resolvedSecretFields);

            BindRequest bindRequest = new BindRequest(
                    getName(),
                    scalarEncoder.encode(),
                    "TABLE",
                    inputSchemaBytes,   // the TableInput argument's required-columns row shape
                    settingsBytes,
                    secretsBytes,
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    secretsBytes != null, // resolved_secrets_provided — see BindCache's own caveat
                    null, null,     // at_unit / at_value — no time travel for table-in-out
                    null, null,     // copy_from / copy_to
                    getSchema());
            BindResponse bound = a.service().bind(bindRequest, null);
            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);

            Schema outputSchema = ArrowSchemaCodec.deserializeSchema(bound.output_schema());
            List<String> columnNames = new ArrayList<>(outputSchema.getFields().size());
            List<Type> columnTypes = new ArrayList<>(outputSchema.getFields().size());
            for (Field field : outputSchema.getFields()) {
                columnNames.add(field.getName());
                columnTypes.add(VgiTypeMapping.toTrinoType(field));
            }

            VgiTableInOutTableFunctionHandle handle = new VgiTableInOutTableFunctionHandle(
                    serializedBindCall, bound.opaque_data(), bound.output_schema(), inputSchemaBytes, hasFinalize);

            // VGI's classic table-in-out has no partial-projection concept of its own — the whole
            // TableInput schema is always required (see this class's own javadoc).
            List<Integer> requiredColumns = new ArrayList<>(inputSchema.getFields().size());
            for (int i = 0; i < inputSchema.getFields().size(); i++) requiredColumns.add(i);

            return TableFunctionAnalysis.builder()
                    .returnedType(Descriptor.descriptor(columnNames, columnTypes))
                    .requiredColumns(tableArgName, requiredColumns)
                    .handle(handle)
                    .build();
        });
    }

    /** The {@code TableInput} argument's real column shape, from its bound {@link RowType}. */
    private static Schema toArrowSchema(RowType rowType) {
        List<RowType.Field> fields = rowType.getFields();
        List<Field> arrowFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            arrowFields.add(VgiTypeMapping.toArrowField(field.getType(), field.getName().orElse("field" + i)));
        }
        return new Schema(arrowFields);
    }

    /**
     * {@code ScalarArgument.getValue()} comes back in the type's own
     * native/internal representation ({@code Slice} for VARCHAR, raw int bits
     * as a {@code long} for REAL) — not the plain Java value {@code
     * ArgumentsEncoder}/{@code ScalarValue.of} know how to infer an Arrow type
     * from. Mirrors {@link VgiTableFunction#toEncodableValue}/{@link
     * VgiTableInOutFunction#toEncodableValue} exactly.
     */
    private static Object toEncodableValue(Type type, Object value) {
        if (type instanceof io.trino.spi.type.VarcharType && value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type instanceof io.trino.spi.type.RealType && value instanceof Long bits) {
            return (double) Float.intBitsToFloat(bits.intValue());
        }
        return value;
    }
}
