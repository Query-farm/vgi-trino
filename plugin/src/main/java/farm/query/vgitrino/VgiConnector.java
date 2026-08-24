// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.metadata.VgiMetadata;
import farm.query.vgitrino.page.VgiPageSourceProvider;
import farm.query.vgitrino.split.VgiSplitManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

/**
 * Wires one attached VGI catalog's {@link VgiWorkerClient} to Trino's
 * {@link ConnectorMetadata}/{@link ConnectorSplitManager}/{@link ConnectorPageSourceProvider}.
 *
 * <p>v1 is read-only with no real transaction semantics: {@link #beginTransaction}
 * mints a fresh {@link VgiTransactionHandle} per query and {@link #commit}/
 * {@link #rollback} are no-ops — VGI's {@code catalog_transaction_begin/commit/
 * rollback} aren't wired up yet (see the plan's non-goals).
 */
public final class VgiConnector implements Connector {

    private final VgiWorkerClient client;
    private final VgiConfig config;

    /**
     * @param client the pooled connection to this catalog's VGI worker,
     *        already attached; closed by {@link #shutdown}
     * @param config this catalog's configuration
     */
    public VgiConnector(VgiWorkerClient client, VgiConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return VgiTransactionHandle.create();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new VgiMetadata(client);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new VgiSplitManager(client, config);
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return new VgiPageSourceProvider(client);
    }

    @Override
    public void shutdown() {
        client.close();
    }
}
