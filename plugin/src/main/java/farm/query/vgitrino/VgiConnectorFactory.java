// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiAggregateFunctions;
import farm.query.vgitrino.function.VgiScalarFunctions;
import farm.query.vgitrino.function.VgiTableFunctions;
import farm.query.vgitrino.function.VgiTableInOutFunctions;
import farm.query.vgitrino.function.VgiTableBufferingFunction;
import farm.query.vgitrino.function.VgiTableBufferingFunctions;
import farm.query.vgitrino.function.VgiTableInOutTableFunction;
import farm.query.vgitrino.function.VgiTableInOutTableFunctions;
import farm.query.vgitrino.function.VgiTableScanFunctions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Entry point Trino calls once per {@code etc/catalog/*.properties} file that
 * declares {@code connector.name=vgi}. No dependency injection framework —
 * {@link VgiConfig#fromProperties} parses the properties map directly and
 * {@link VgiWorkerClient}'s constructor spawns/attaches the connection pool
 * synchronously, so catalog registration blocks until the worker answers.
 */
public final class VgiConnectorFactory implements ConnectorFactory {

    /** The {@code connector.name} this factory answers to. */
    public static final String NAME = "vgi";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        VgiConfig vgiConfig = VgiConfig.fromProperties(config);
        VgiWorkerClient client = new VgiWorkerClient(vgiConfig);
        Set<ConnectorTableFunction> tableFunctions = new HashSet<>(VgiTableFunctions.discover(client));
        // Regular (producer-mode), blended (literal-call), classic (TableInput-argument), and
        // table-buffering (Sink+Combine+Source) table-in-out functions all register through the
        // exact same Connector.getTableFunctions() Set — Trino has no separate surface for a
        // second/third/fourth "kind" of table function; the four discover() calls already
        // partition disjointly on FunctionInfo.input_from_args, whether a TableInput argument is
        // present, and function_type (see each class's own javadoc).
        tableFunctions.addAll(VgiTableInOutFunctions.discover(client));
        Set<ConnectorTableFunction> tableInOutTableFunctions = VgiTableInOutTableFunctions.discover(client);
        tableFunctions.addAll(tableInOutTableFunctions);
        Set<ConnectorTableFunction> tableBufferingFunctions = VgiTableBufferingFunctions.discover(client);
        tableFunctions.addAll(tableBufferingFunctions);
        VgiScalarFunctions.Registry scalarFunctions = VgiScalarFunctions.discover(client);
        VgiAggregateFunctions.Registry aggregateFunctions = VgiAggregateFunctions.discover(client);
        // A declarative table's backing scan function's required_settings/required_secrets live
        // only on FunctionInfo (via catalog_schema_contents_functions), never on
        // catalog_table_scan_function_get's own response — see VgiTableScanFunctions' javadoc.
        // Discovered once here so VgiMetadata#getTableHandle needs no extra per-query RPC for it.
        VgiTableScanFunctions.Registry scanFunctions = VgiTableScanFunctions.discover(client);
        // A classic table-in-out or table-buffering function's required_settings names need to
        // reach VgiConnector.getSessionProperties() too, exactly like a scalar function's —
        // collected here since both discover() calls return a bare Set<ConnectorTableFunction>,
        // not a Registry with its own metadata surface the way scalar/aggregate functions have.
        Set<String> tableInOutRequiredSettingNames = new HashSet<>();
        for (ConnectorTableFunction function : tableInOutTableFunctions) {
            if (function instanceof VgiTableInOutTableFunction t) {
                tableInOutRequiredSettingNames.addAll(t.requiredSettingNames());
            }
        }
        for (ConnectorTableFunction function : tableBufferingFunctions) {
            if (function instanceof VgiTableBufferingFunction t) {
                tableInOutRequiredSettingNames.addAll(t.requiredSettingNames());
            }
        }
        return new VgiConnector(client, vgiConfig, tableFunctions, scalarFunctions, aggregateFunctions,
                tableInOutRequiredSettingNames, scanFunctions);
    }
}
