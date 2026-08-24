// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.page;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import farm.query.vgitrino.metadata.VgiTableHandle;
import farm.query.vgitrino.split.VgiSplit;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

/** Builds a {@link VgiPageSource} per split. */
public final class VgiPageSourceProvider implements ConnectorPageSourceProvider {

    private final VgiWorkerClient client;

    /** @param client the pooled connection to this catalog's VGI worker */
    public VgiPageSourceProvider(VgiWorkerClient client) {
        this.client = client;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        VgiSplit vgiSplit = (VgiSplit) split;
        VgiTableHandle handle = (VgiTableHandle) table;
        List<VgiColumnHandle> vgiColumns = columns.stream().map(VgiColumnHandle.class::cast).toList();
        return new VgiPageSource(client, vgiSplit, vgiColumns, handle.outputSchema());
    }
}
