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
 * <h2>v1 scope</h2>
 *
 * <p>Only a function with NO finalize phase registers at all — {@code
 * has_finalize=true} (VGI's {@code SubstreamPartialSumFunction}/{@code
 * MultiBatchFinishFunction}-style cross-batch accumulation) needs a second
 * {@code init(phase=FINALIZE)} turn on the SAME connection/{@code
 * execution_id} after input end-of-stream, which {@link
 * VgiTableInOutDataProcessor} does not implement yet — skipped, not
 * registered wrong, exactly like every other out-of-scope shape in this
 * connector.
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
     *   <li>{@code has_finalize=true} — see this class's own v1-scope note;</li>
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
                    String context = schemaName + "." + info.name();
                    if (info.has_finalize()) {
                        LOG.warn("VGI table-in-out function %s: skipping registration — has_finalize=true "
                                + "needs a second init(phase=FINALIZE) turn this connector doesn't implement yet",
                                context);
                        continue;
                    }
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
                out.add(new VgiTableInOutTableFunction(client, info.schema_name(), info.name(), argSpecs));
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
