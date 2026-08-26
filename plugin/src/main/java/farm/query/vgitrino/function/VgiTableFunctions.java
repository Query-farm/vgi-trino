// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Discovers a catalog's VGI table functions and builds a {@link VgiTableFunction} per one this connector can support. */
public final class VgiTableFunctions {

    private VgiTableFunctions() {}

    /**
     * List every {@code TABLE_FUNCTION} across every schema and build a
     * {@link VgiTableFunction} for the ones this connector can support.
     * Skipped, rather than registered wrong or crashing catalog creation:
     * <ul>
     *   <li>a function with an unsupported argument (varargs, {@code any}-typed,
     *       TABLE input) — see {@link VgiArgSpec#decode};</li>
     *   <li>an OVERLOADED function name — VGI resolves overloads by argument
     *       count/type at bind time (mirroring Java method overloading), but
     *       Trino's {@code ConnectorTableFunction} model requires exactly one
     *       registration per (schema, name); registering more than one under
     *       the same name throws at catalog-creation time ({@code Multiple
     *       entries with same key}), so every overload of a multiply-declared
     *       name is skipped rather than guessing which one the caller meant.</li>
     * </ul>
     *
     * <p>A {@code TABLE_FUNCTION} entry with {@code input_from_args=true} is a blended
     * ("table-in-out") function — a fundamentally different wire shape ({@code
     * VgiTableInOutFunctions}'s literal-call exchange, not this class's producer-mode
     * {@code table_function_plan}/split scan) — and is skipped here, not registered wrong;
     * {@link VgiTableInOutFunctions#discover} handles it instead.
     *
     * @param client the pooled connection to attach and query
     * @return the callable table functions this connector can support
     */
    public static Set<ConnectorTableFunction> discover(VgiWorkerClient client) {
        return client.withConnection(a -> {
            List<String> schemas = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                schemas.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
            }
            // Group by (schema, name) first so an overloaded name can be
            // detected and skipped entirely, rather than registering
            // whichever overload happened to be seen first.
            Map<String, List<FunctionInfo>> byKey = new LinkedHashMap<>();
            for (String schemaName : schemas) {
                ItemsResponse functions = a.service().catalog_schema_contents_functions(
                        a.handle(), schemaName, "TABLE_FUNCTION", null, null);
                for (byte[] item : functions.items()) {
                    FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
                    if (info.input_from_args()) continue; // blended — VgiTableInOutFunctions handles it
                    byKey.computeIfAbsent(schemaName + "." + info.name(), k -> new ArrayList<>()).add(info);
                }
            }
            Set<ConnectorTableFunction> out = new java.util.HashSet<>();
            for (List<FunctionInfo> overloads : byKey.values()) {
                if (overloads.size() != 1) continue; // overloaded — not representable
                FunctionInfo info = overloads.get(0);
                List<VgiArgSpec> argSpecs = decodeArgs(info.arguments());
                if (argSpecs == null) continue; // unsupported argument shape
                out.add(new VgiTableFunction(client, info.schema_name(), info.name(), argSpecs));
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
