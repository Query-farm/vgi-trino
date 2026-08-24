// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Set;

/** Registers {@link VgiConnectorFactory} — the {@code connector.name=vgi} entry point. */
public final class VgiPlugin implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return Set.of(new VgiConnectorFactory());
    }
}
