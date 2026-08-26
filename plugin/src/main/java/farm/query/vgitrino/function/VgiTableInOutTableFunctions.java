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
 * Discovers a catalog's CLASSIC (non-blended) VGI table-in-out functions — a
 * real {@code TableInput} argument, e.g. {@code echo}/{@code
 * repeat_inputs}/{@code filter_by_setting} — and builds a {@link
 * VgiTableInOutTableFunction} per one this connector can support.
 *
 * <h2>Finalize-phase and settings/secrets support</h2>
 *
 * <p>A {@code has_finalize=true} function (VGI's {@code
 * SubstreamPartialSumFunction}/{@code MultiBatchFinishFunction}-style
 * cross-batch accumulation) registers too — {@link VgiTableInOutDataProcessor}
 * drives its second {@code init(phase=FINALIZE)} turn (carrying the INPUT
 * phase's own {@code execution_id}/{@code opaque_data}) on the same
 * connection after input end-of-stream. {@code required_settings}/{@code
 * required_secrets} (the real fixture's {@code filter_by_setting}/{@code
 * secret_in_out}) resolve exactly like a scalar function's — see {@link
 * VgiTableInOutTableFunction#analyze}, which reuses {@link
 * VgiScalarFunctions.BindCache#resolveSettings}/{@link
 * VgiScalarFunctions.BindCache#resolveSecretFields} verbatim, since {@code
 * analyze()} already receives a real {@code ConnectorSession} directly (no
 * bind-cache equivalent needed on this side at all).
 *
 * @see VgiTableInOutTableFunction
 */
public final class VgiTableInOutTableFunctions {

    private static final Logger LOG = Logger.get(VgiTableInOutTableFunctions.class);

    private VgiTableInOutTableFunctions() {}

    /**
     * List every {@code TABLE_FUNCTION} across every schema and build a
     * {@link VgiTableInOutTableFunction} for each classic (has a real {@code
     * TableInput} argument, {@code input_from_args=false}) one this connector
     * can support. Skipped, rather than registered wrong or crashing catalog
     * creation:
     * <ul>
     *   <li>anything blended ({@code input_from_args=true}) or with NO {@code
     *       TableInput} argument at all — those are {@link
     *       VgiTableInOutFunctions#discover}'s and {@link
     *       VgiTableFunctions#discover}'s jobs respectively;</li>
     *   <li>anything whose {@code function_type} is {@code "TABLE_BUFFERING"} — a {@code
     *       TableBufferingFunction} (e.g. {@code sum_all_columns}) has the SAME wire shape at
     *       discovery time ({@code input_from_args=false}, exactly one real {@code TableInput}
     *       argument) but drives a genuinely different Sink+Combine+Source protocol
     *       ({@code TABLE_BUFFERING}/{@code TABLE_BUFFERING_FINALIZE} init phases, not this
     *       kind's {@code INPUT}/{@code FINALIZE}) — registering it here would send phase
     *       {@code "FINALIZE"} to a worker method that only understands {@code
     *       "TABLE_BUFFERING"}/{@code "TABLE_BUFFERING_FINALIZE"}, crashing at query time with
     *       {@code ValueError: Unsupported init phase for TableBufferingFunction}. {@link
     *       VgiTableBufferingFunctions#discover} handles it instead.</li>
     *   <li>a function with an unsupported non-table argument (varargs, {@code
     *       any}-typed) — see {@link VgiArgSpec#decode};</li>
     *   <li>more than one {@code TableInput} argument — VGI's own {@code
     *       resolve_metadata} already rejects registering one, so this is a
     *       defensive, should-never-happen check;</li>
     *   <li>an OVERLOADED function name — same Trino {@code ConnectorTableFunction}
     *       one-registration-per-name constraint every other discovery class here
     *       documents.</li>
     * </ul>
     *
     * @param client the pooled connection to attach and query
     * @return the callable classic table-in-out functions this connector can support
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
                    if (info.input_from_args()) continue; // blended — VgiTableInOutFunctions handles it
                    // TableBufferingFunction — same shape at this point (input_from_args=false, one
                    // TableInput arg) but a genuinely different wire protocol; see this method's own
                    // javadoc. VgiTableBufferingFunctions.discover handles it instead.
                    if ("TABLE_BUFFERING".equalsIgnoreCase(info.function_type())) continue;
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
                if (tableArgCount == 0) continue; // no TableInput arg — VgiTableFunctions handles it
                if (tableArgCount > 1) {
                    LOG.warn("VGI table-in-out function %s: skipping registration — %d TableInput "
                            + "arguments (VGI itself only ever registers one)", context, tableArgCount);
                    continue;
                }
                List<String> requiredSettings = info.required_settings() == null ? List.of() : info.required_settings();
                List<farm.query.vgi.protocol.FunctionRequiredSecret> requiredSecrets =
                        info.required_secrets() == null ? List.of() : info.required_secrets();
                out.add(new VgiTableInOutTableFunction(client, info.schema_name(), info.name(), argSpecs,
                        info.has_finalize(), requiredSettings, requiredSecrets));
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
