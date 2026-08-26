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
 * One VGI {@code TableBufferingFunction} — the third, distinct VGI table-in-out kind, callable
 * from Trino as {@code SELECT * FROM TABLE(catalog.schema.name(input => TABLE(some_query)))} —
 * e.g. {@code sum_all_columns}/{@code sum_all_columns_simple_distributed}.
 *
 * <h2>How this differs from the other two table-in-out kinds</h2>
 *
 * <p>A blended {@link VgiTableInOutFunction} and a classic {@link VgiTableInOutTableFunction}
 * both drive VGI's {@code INPUT}/{@code FINALIZE} {@code init} phases over the SAME long-lived
 * streaming {@code exchange()} session — the worker sees one substream's rows and can
 * incrementally answer as they arrive. A {@code TableBufferingFunction}, in contrast, is routed by
 * the C++ extension through an entirely different DuckDB physical operator
 * ({@code PhysicalVgiTableBufferingFunction}, a Sink+Combine+Source operator) BECAUSE the function
 * must see every input row before producing any output at all (buffer-then-emit, global
 * aggregation, sort-then-emit). Confirmed directly from the real worker SDK
 * (~/Development/vgi-python/vgi/table_buffering_function.py and vgi/worker.py): the wire protocol
 * is genuinely different, not just a different phase name —
 *
 * <ul>
 *   <li><b>Sink</b> — {@code table_buffering_process} is a plain UNARY RPC (one input batch in,
 *       an opaque worker-chosen {@code state_id} back), NOT a streaming exchange turn. Called once
 *       per input batch.</li>
 *   <li><b>Combine</b> — {@code table_buffering_combine} is another unary RPC, called EXACTLY ONCE
 *       after all input, handing the worker every {@code state_id} collected from every Sink call;
 *       the worker's {@code combine()} callback merges/groups them into {@code
 *       finalize_state_ids}.</li>
 *   <li><b>Source</b> — one streaming producer-mode {@code init(phase=TABLE_BUFFERING_FINALIZE,
 *       finalize_state_id=...)} call PER returned {@code finalize_state_id}, drained via {@code
 *       tick()} exactly like a classic table-in-out's {@code FINALIZE} phase.</li>
 * </ul>
 *
 * <p>This mirrors {@code VgiAggregateFunctions}' unary-RPC-per-call design ({@code
 * aggregate_update}/{@code aggregate_combine}/{@code aggregate_finalize}) far more than it mirrors
 * {@link VgiTableInOutDataProcessor}'s streaming-session model — see {@link
 * VgiTableBufferingDataProcessor}'s own javadoc for the full wire sequence this connector drives.
 *
 * <h2>Distributed combine — genuinely supported, not merely non-decomposable</h2>
 *
 * <p>Unlike {@code VgiAggregateFunctions}' {@code aggregate_combine} (which this connector's own
 * research found has no portable cross-node state-shipping mechanism, so aggregates are
 * deliberately registered non-decomposable, single-stage), {@code TableBufferingFunction}'s {@code
 * combine()} is a REAL, first-class RPC the worker always runs, merging every {@code state_id}
 * collected from every Sink call into the {@code finalize_state_ids} the Source phase drains. This
 * connector calls it unconditionally, on the full, real list of {@code state_id}s it collected —
 * there is no scoped-down or partial version of the protocol here. What IS scoped down is
 * concurrency: since Trino only creates multiple partitions for a table-function call under an
 * explicit {@code PARTITION BY} (which this registration, like the classic table-in-out one, does
 * not request), this connector's {@link VgiTableBufferingDataProcessor} always drives the ENTIRE
 * Sink phase serially from one partition — the "single-worker" case the real C++ extension also
 * exercises whenever a query runs with a single DuckDB thread. That is a genuine, complete
 * implementation of the wire protocol, not a fallback: {@code combine()} still runs, over however
 * many {@code state_id}s a serial Sink phase produced (one per input batch, not necessarily one in
 * total), so a worker's {@code combine()} callback is exercised exactly as designed. What this
 * connector does NOT attempt is fanning the Sink phase itself out across multiple concurrent
 * connections the way the C++ operator's multi-threaded {@code Sink()} does — nothing in Trino's
 * {@code TableFunctionDataProcessor} SPI offers this connector a parallel-partition hook for a
 * data-driven ({@code TableArgumentSpecification}) call to fan out on (see {@link
 * VgiTableInOutTableFunction}'s own javadoc for why: any {@code TableArgumentSpecification} routes
 * through the single, serial {@code TableFunctionDataProcessor} path, never a split-based one).
 *
 * <p>{@code required_settings}/{@code required_secrets} resolve here, in {@link #analyze}, exactly
 * like a classic table-in-out function's — reusing {@link
 * VgiScalarFunctions.BindCache#resolveSettings}/{@link VgiScalarFunctions.BindCache#resolveSecretFields}
 * verbatim.
 */
public final class VgiTableBufferingFunction extends AbstractConnectorTableFunction {

    private final VgiWorkerClient client;
    private final List<VgiArgSpec> argSpecs;
    private final List<String> requiredSettings;
    private final List<FunctionRequiredSecret> requiredSecrets;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param schemaName the VGI schema this function is registered in
     * @param functionName the function name
     * @param argSpecs the function's decoded, supported arguments, in declaration order —
     *        exactly one of which has {@link VgiArgSpec#tableArg()} set
     * @param requiredSettings {@code FunctionInfo.required_settings} verbatim
     * @param requiredSecrets {@code FunctionInfo.required_secrets} verbatim
     */
    public VgiTableBufferingFunction(VgiWorkerClient client, String schemaName, String functionName,
            List<VgiArgSpec> argSpecs, List<String> requiredSettings,
            List<FunctionRequiredSecret> requiredSecrets) {
        super(schemaName, functionName, toArgumentSpecifications(argSpecs),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.client = client;
        this.argSpecs = argSpecs;
        this.requiredSettings = requiredSettings;
        this.requiredSecrets = requiredSecrets;
    }

    /**
     * @return this function's {@code required_settings} names — what {@link
     *         VgiConnectorFactory} unions across every discovered table-in-out-family function
     *         (classic and buffering alike) to declare as this catalog's Trino session properties
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
                    // Every VGI table-buffering function has exactly one of these (enforced
                    // server-side) — its real row shape, not any single call's actual data (that
                    // arrives later, via pages, at VgiTableBufferingDataProcessor).
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

            VgiTableBufferingFunctionHandle handle = new VgiTableBufferingFunctionHandle(
                    serializedBindCall, bound.opaque_data(), bound.output_schema(), inputSchemaBytes,
                    getSchema(), getName());

            // VGI's table-buffering functions have no partial-projection concept of their own —
            // the whole TableInput schema is always required (same as classic table-in-out).
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
     * {@code ScalarArgument.getValue()} comes back in the type's own native/internal
     * representation — not the plain Java value {@code ArgumentsEncoder}/{@code ScalarValue.of}
     * know how to infer an Arrow type from. Mirrors {@link VgiTableInOutTableFunction#toEncodableValue}
     * exactly.
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
