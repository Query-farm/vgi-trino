// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiScalarFunctions;
import farm.query.vgitrino.function.VgiTableFunctions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.function.table.ConnectorTableFunction;

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
        Set<ConnectorTableFunction> tableFunctions = VgiTableFunctions.discover(client);
        VgiScalarFunctions.Registry scalarFunctions = VgiScalarFunctions.discover(client);
        return new VgiConnector(client, vgiConfig, tableFunctions, scalarFunctions);
    }
}
