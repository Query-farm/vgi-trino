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
 * Discovers a catalog's VGI table-in-out ("blended") functions and builds a
 * {@link VgiTableInOutFunction} per one this connector can support — the
 * LITERAL call shape only (constant arguments, e.g. {@code
 * cat.main.forecast_current(52.52, 13.41)}).
 *
 * <h2>Why only the literal shape</h2>
 *
 * <p>VGI's own {@code RowTransformFunction} ("blended") kind serves three call
 * shapes from one registration: literal (constant args, ~1 input row),
 * per-row column (a table's columns feed the args, one call per row), and
 * {@code LATERAL} (an outer table's row feeds the args, correlated per-row).
 * The last two need Trino to evaluate a table function's scalar argument once
 * PER ROW of some correlated outer table — verified directly against the real
 * Trino table-function SPI ({@code ScalarArgumentSpecification}/{@code
 * TableArgumentSpecification}/{@code DescriptorArgumentSpecification}):
 * arguments are resolved exactly once, at {@code analyze()}/bind time, with no
 * "this value comes from each row of an outer table" hook anywhere. The
 * literal shape needs no such hook — every argument really is a compile-time
 * constant — so it alone is representable through Trino's existing PTF
 * machinery, via the ordinary {@code ScalarArgumentSpecification}s {@link
 * VgiTableFunction} already uses for a ordinary table function.
 *
 * @see VgiTableInOutFunction
 */
public final class VgiTableInOutFunctions {

    private static final Logger LOG = Logger.get(VgiTableInOutFunctions.class);

    private VgiTableInOutFunctions() {}

    /**
     * List every {@code TABLE_FUNCTION} across every schema and build a
     * {@link VgiTableInOutFunction} for each blended ({@code
     * input_from_args=true}) one this connector can support as a literal
     * call. Skipped, rather than registered wrong or crashing catalog
     * creation:
     * <ul>
     *   <li>anything NOT blended ({@code input_from_args=false}) — that's
     *       {@link VgiTableFunctions#discover}'s job;</li>
     *   <li>a blended function that also advertises {@code has_finalize=true} — the wire
     *       itself rejects this combination at {@code bind()} time in the reference C++
     *       client (a blended function structurally cannot have a finalize phase — see
     *       {@code RowTransformFunction.hasFinalize()}, hard-{@code false} and
     *       non-overridable in every SDK), so this should never actually be observed; skipped
     *       defensively rather than assumed impossible;</li>
     *   <li>a function with an unsupported argument (varargs, {@code any}-typed, TABLE
     *       input) — see {@link VgiArgSpec#decode}. Notably this defers {@code row_sum}'s
     *       varargs positional argument — the same varargs gap {@link VgiTableFunctions}
     *       already has for regular table functions, not a new one;</li>
     *   <li>a blended function with zero positional arguments — VGI's own {@code
     *       resolve_metadata} already rejects registering one, so this is a defensive,
     *       should-never-happen check, not a real observed shape;</li>
     *   <li>an OVERLOADED function name — same Trino {@code ConnectorTableFunction}
     *       one-registration-per-name constraint {@link VgiTableFunctions#discover}
     *       documents.</li>
     * </ul>
     *
     * @param client the pooled connection to attach and query
     * @return the callable table-in-out literal-call functions this connector can support
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
                    if (!info.input_from_args()) continue; // not blended — VgiTableFunctions handles it
                    String context = schemaName + "." + info.name();
                    if (info.has_finalize()) {
                        LOG.warn("VGI table-in-out function %s: skipping registration — advertises "
                                + "input_from_args AND has_finalize, a combination with no literal-call "
                                + "representation (a blended function cannot have a finalize phase)", context);
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
                if (argSpecs.stream().noneMatch(VgiArgSpec::positional)) {
                    LOG.warn("VGI table-in-out function %s: skipping registration — no positional "
                            + "argument (a blended function needs at least one row-input column)", context);
                    continue;
                }
                out.add(new VgiTableInOutFunction(client, info.schema_name(), info.name(), argSpecs));
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
