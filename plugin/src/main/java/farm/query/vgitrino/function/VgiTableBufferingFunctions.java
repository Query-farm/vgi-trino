// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import io.airlift.log.Logger;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Discovers a catalog's {@code TableBufferingFunction}-backed table functions — VGI's third,
 * distinct table-in-out kind (e.g. {@code sum_all_columns}/{@code
 * sum_all_columns_simple_distributed}) — and builds a {@link VgiTableBufferingFunction} per one
 * this connector can support.
 *
 * <p>A {@code TableBufferingFunction} has the SAME wire shape at discovery time as a classic
 * table-in-out function — {@code input_from_args=false} and exactly one real {@code TableInput}
 * argument — so the only field that tells the two kinds apart is {@code
 * FunctionInfo.function_type}: {@code "TABLE"} for a classic {@code TableInOutGenerator}/{@code
 * TableInOutFunction}, {@code "TABLE_BUFFERING"} for a {@code TableBufferingFunction} (confirmed
 * against the real worker SDK's {@code CatalogFunctionType} enum and {@code _infer_function_type}
 * — the class hierarchy is the dispatch key, set automatically). Before this class existed, {@link
 * VgiTableInOutTableFunctions#discover} had no way to tell the two apart and mis-registered every
 * {@code TableBufferingFunction} as a classic one, which then crashed at query time with {@code
 * ValueError: Unsupported init phase for TableBufferingFunction} the first time the C++ extension
 * (never reached in practice here — this connector short-circuits first) or this connector's own
 * {@code VgiTableInOutDataProcessor} sent phase {@code "FINALIZE"} to a worker method that only
 * understands {@code "TABLE_BUFFERING"}/{@code "TABLE_BUFFERING_FINALIZE"}. {@link
 * VgiTableInOutTableFunctions#discover} now explicitly skips {@code function_type ==
 * "TABLE_BUFFERING"} so the two discovery classes partition disjointly.
 *
 * @see VgiTableBufferingFunction
 * @see VgiTableBufferingDataProcessor
 */
public final class VgiTableBufferingFunctions {

    private static final Logger LOG = Logger.get(VgiTableBufferingFunctions.class);

    /** The {@code FunctionInfo.function_type} wire value a {@code TableBufferingFunction} reports
     *  (dictionary-encoded string; the real worker SDK writes the enum member's {@code .name},
     *  e.g. {@code "TABLE_BUFFERING"}, and the C++ extension's own parser accepts either case —
     *  matched case-insensitively here for the same robustness). */
    private static final String TABLE_BUFFERING_FUNCTION_TYPE = "TABLE_BUFFERING";

    private VgiTableBufferingFunctions() {}

    /**
     * List every {@code TABLE_FUNCTION} across every schema and build a {@link
     * VgiTableBufferingFunction} for each {@code TableBufferingFunction}-backed one this connector
     * can support. Skipped, rather than registered wrong or crashing catalog creation:
     * <ul>
     *   <li>anything that isn't {@code function_type == "TABLE_BUFFERING"} — {@link
     *       VgiTableInOutFunctions#discover}'s, {@link VgiTableFunctions#discover}'s, and {@link
     *       VgiTableInOutTableFunctions#discover}'s jobs respectively;</li>
     *   <li>a function with an unsupported non-table argument (varargs, {@code any}-typed) — see
     *       {@link VgiArgSpec#decode};</li>
     *   <li>more than one {@code TableInput} argument — VGI's own {@code resolve_metadata} already
     *       rejects registering one, so this is a defensive, should-never-happen check;</li>
     *   <li>an OVERLOADED function name — same Trino {@code ConnectorTableFunction}
     *       one-registration-per-name constraint every other discovery class here documents.</li>
     * </ul>
     *
     * @param client the pooled connection to attach and query
     * @return the callable table-buffering functions this connector can support
     */
    public static Set<ConnectorTableFunction> discover(VgiWorkerClient client) {
        return client.withConnection(a -> {
            List<String> schemas = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                schemas.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
            }
            Map<String, List<FunctionInfo>> byKey = new LinkedHashMap<>();
            for (String schemaName : schemas) {
                ItemsResponse functions = a.service().catalog_schema_contents_functions(
                        a.handle(), schemaName, "TABLE_FUNCTION", null, null);
                for (byte[] item : functions.items()) {
                    FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
                    if (!TABLE_BUFFERING_FUNCTION_TYPE.equalsIgnoreCase(info.function_type())) continue;
                    byKey.computeIfAbsent(schemaName + "." + info.name(), k -> new ArrayList<>()).add(info);
                }
            }
            Set<ConnectorTableFunction> out = new HashSet<>();
            for (List<FunctionInfo> overloads : byKey.values()) {
                if (overloads.size() != 1) continue; // overloaded — not representable
                FunctionInfo info = overloads.get(0);
                String context = info.schema_name() + "." + info.name();
                List<VgiArgSpec> argSpecs = decodeArgs(info.arguments());
                if (argSpecs == null) continue; // unsupported argument shape — reason already logged
                long tableArgCount = argSpecs.stream().filter(VgiArgSpec::tableArg).count();
                if (tableArgCount == 0) continue; // no TableInput arg — shouldn't happen for this function_type
                if (tableArgCount > 1) {
                    LOG.warn("VGI table-buffering function %s: skipping registration — %d TableInput "
                            + "arguments (VGI itself only ever registers one)", context, tableArgCount);
                    continue;
                }
                List<String> requiredSettings = info.required_settings() == null ? List.of() : info.required_settings();
                List<farm.query.vgi.protocol.FunctionRequiredSecret> requiredSecrets =
                        info.required_secrets() == null ? List.of() : info.required_secrets();
                out.add(new VgiTableBufferingFunction(client, info.schema_name(), info.name(), argSpecs,
                        requiredSettings, requiredSecrets));
            }
            return out;
        });
    }

    /** @return the decoded specs, or {@code null} if any argument is unsupported */
    private static List<VgiArgSpec> decodeArgs(byte[] argumentsSchemaBytes) {
        Schema schema = ArrowSchemaCodec.deserializeSchema(argumentsSchemaBytes);
        if (schema == null) return List.of();
        List<VgiArgSpec> out = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            VgiArgSpec spec = VgiArgSpec.decode(field);
            if (spec == null) return null;
            out.add(spec);
        }
        return out;
    }
}
