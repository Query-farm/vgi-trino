// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiFunctionProvider;
import farm.query.vgitrino.metadata.VgiMetadata;
import farm.query.vgitrino.page.VgiPageSourceProvider;
import farm.query.vgitrino.split.VgiSplitManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;
import java.util.Set;

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
    private final Set<ConnectorTableFunction> tableFunctions;

    /**
     * @param client the pooled connection to this catalog's VGI worker,
     *        already attached; closed by {@link #shutdown}
     * @param config this catalog's configuration
     * @param tableFunctions this catalog's discovered, callable table
     *        functions (see {@link farm.query.vgitrino.function.VgiTableFunctions#discover})
     */
    public VgiConnector(VgiWorkerClient client, VgiConfig config, Set<ConnectorTableFunction> tableFunctions) {
        this.client = client;
        this.config = config;
        this.tableFunctions = tableFunctions;
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
    public Set<ConnectorTableFunction> getTableFunctions() {
        return tableFunctions;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider() {
        return Optional.of(new VgiFunctionProvider(client));
    }

    @Override
    public void shutdown() {
        client.close();
    }
}
