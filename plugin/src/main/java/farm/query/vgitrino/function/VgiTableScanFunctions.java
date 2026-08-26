// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.client.VgiWorkerClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Discovers every VGI {@code TABLE_FUNCTION}'s {@code required_settings}/{@code
 * required_secrets}, keyed by function name alone — what a DECLARATIVE table's
 * backing scan function needs, looked up once at catalog-attach time so {@link
 * farm.query.vgitrino.metadata.VgiMetadata#getTableHandle} and {@link
 * farm.query.vgitrino.split.VgiSplitManager}'s plain-table-scan {@code
 * getSplits} never need an extra per-query RPC to answer it.
 *
 * <h2>Why this exists: {@code catalog_table_scan_function_get} alone can't answer it</h2>
 *
 * <p>A declarative table's backing scan function is resolved via {@code
 * catalog_table_scan_function_get}, whose response ({@code
 * TableScanFunctionGetResponse}) carries only {@code function_name}, {@code
 * arguments}, and {@code required_extensions} — confirmed against the real wire
 * record (vgi-java's {@code TableScanFunctionGetResponse.java}), no {@code
 * required_settings}/{@code required_secrets} fields exist on it at all. Those
 * two fields exist only on {@link FunctionInfo}, discoverable solely through
 * {@code catalog_schema_contents_functions(type=TABLE_FUNCTION)} — the SAME RPC
 * {@link VgiTableFunctions#discover}/{@link VgiTableInOutTableFunctions#discover}
 * already call to register callable {@code TABLE(...)} functions. This class
 * makes one more full pass over that same data, once, at catalog-attach time.
 *
 * <h2>Keyed by function name alone, not {@code (schema, name)}</h2>
 *
 * <p>{@code TableScanFunctionGetResponse} names only the function, never which
 * schema it is registered in — the reference fixture's {@code data.rowid_first}
 * table scans via {@code main.rowid_sequence}, a DIFFERENT schema than the
 * table's own (see {@code VgiSplitManager}'s own note on why {@code
 * BindRequest.schema_name} is left {@code null} for this exact reason: the wire
 * protocol itself already accepts this ambiguity, resolving purely by name on
 * the worker side). A function name reused across two different schemas is a
 * real, if unlikely, source of imprecision here too — whichever schema's {@link
 * FunctionInfo} is discovered FIRST wins — but it can only ever cause this
 * connector to send (or withhold) the settings/secrets a SAME-NAMED sibling
 * function in another schema declares, never a cross-worker/cross-tenant leak
 * (both belong to the one attached worker either way).
 */
public final class VgiTableScanFunctions {

    private VgiTableScanFunctions() {}

    /** One scan function's out-of-band bind requirements — {@code FunctionInfo.required_settings}/
     *  {@code required_secrets} verbatim. */
    public record Entry(List<String> requiredSettings, List<FunctionRequiredSecret> requiredSecrets) {}

    private static final Entry EMPTY = new Entry(List.of(), List.of());

    /** Every discovered {@code TABLE_FUNCTION}'s requirements, keyed by lower-cased name. */
    public static final class Registry {
        private final Map<String, Entry> byName;
        private final Set<String> requiredSettingNames;

        private Registry(Map<String, Entry> byName) {
            this.byName = byName;
            Set<String> names = new HashSet<>();
            for (Entry entry : byName.values()) names.addAll(entry.requiredSettings());
            this.requiredSettingNames = Set.copyOf(names);
        }

        /**
         * @param functionName a scan function's bare name, e.g. {@code
         *        TableScanFunctionGetResponse.function_name()}
         * @return its discovered {@code required_settings}/{@code required_secrets}, or an entry
         *         with two empty lists when the name matches no discovered {@code TABLE_FUNCTION}
         *         (e.g. an internal-only scan function never separately exposed for a direct
         *         {@code TABLE(...)} call)
         */
        public Entry entryFor(String functionName) {
            Entry entry = byName.get(functionName.toLowerCase(Locale.ROOT));
            return entry == null ? EMPTY : entry;
        }

        /**
         * @return the union of every discovered table function's {@code required_settings} names —
         *         what {@link farm.query.vgitrino.VgiConnector#getSessionProperties()} additionally
         *         unions in, alongside scalar and classic table-in-out functions' own, so a
         *         declarative table's backing function can have its settings supplied via {@code SET
         *         SESSION} too
         */
        public Set<String> requiredSettingNames() {
            return requiredSettingNames;
        }
    }

    /**
     * @param client the pooled connection to attach and query
     * @return every discovered {@code TABLE_FUNCTION}'s requirements, keyed by name
     */
    public static Registry discover(VgiWorkerClient client) {
        return client.withConnection(a -> {
            List<String> schemas = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                schemas.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
            }
            Map<String, Entry> byName = new LinkedHashMap<>();
            for (String schemaName : schemas) {
                ItemsResponse functions = a.service().catalog_schema_contents_functions(
                        a.handle(), schemaName, "TABLE_FUNCTION", null, null);
                for (byte[] item : functions.items()) {
                    FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
                    String key = info.name().toLowerCase(Locale.ROOT);
                    // First schema discovered wins on a name collision — see class javadoc.
                    byName.putIfAbsent(key, new Entry(
                            info.required_settings() == null ? List.of() : info.required_settings(),
                            info.required_secrets() == null ? List.of() : info.required_secrets()));
                }
            }
            return new Registry(byName);
        });
    }
}
