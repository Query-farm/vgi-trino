// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiAggregateFunctions;
import farm.query.vgitrino.function.VgiFunctionProvider;
import farm.query.vgitrino.function.VgiScalarFunctions;
import farm.query.vgitrino.function.VgiTableScanFunctions;
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
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final VgiScalarFunctions.Registry scalarFunctions;
    private final VgiScalarFunctions.BindCache scalarBindCache;
    private final VgiAggregateFunctions.Registry aggregateFunctions;
    private final Set<String> tableInOutRequiredSettingNames;
    private final VgiTableScanFunctions.Registry scanFunctions;

    /**
     * @param client the pooled connection to this catalog's VGI worker,
     *        already attached; closed by {@link #shutdown}
     * @param config this catalog's configuration
     * @param tableFunctions this catalog's discovered, callable table
     *        functions (see {@link farm.query.vgitrino.function.VgiTableFunctions#discover})
     * @param scalarFunctions this catalog's discovered scalar functions (see {@link VgiScalarFunctions#discover})
     * @param aggregateFunctions this catalog's discovered aggregate functions (see {@link VgiAggregateFunctions#discover})
     * @param tableInOutRequiredSettingNames the union of every discovered classic table-in-out
     *        function's {@code required_settings} names (see {@link
     *        farm.query.vgitrino.function.VgiTableInOutTableFunction#requiredSettingNames})
     * @param scanFunctions every discovered {@code TABLE_FUNCTION}'s {@code required_settings}/
     *        {@code required_secrets}, keyed by name — what a DECLARATIVE table's backing scan
     *        function needs (see {@link VgiTableScanFunctions#discover})
     */
    public VgiConnector(VgiWorkerClient client, VgiConfig config, Set<ConnectorTableFunction> tableFunctions,
            VgiScalarFunctions.Registry scalarFunctions, VgiAggregateFunctions.Registry aggregateFunctions,
            Set<String> tableInOutRequiredSettingNames, VgiTableScanFunctions.Registry scanFunctions) {
        this.client = client;
        this.config = config;
        this.tableFunctions = tableFunctions;
        this.scalarFunctions = scalarFunctions;
        this.scalarBindCache = new VgiScalarFunctions.BindCache();
        this.aggregateFunctions = aggregateFunctions;
        this.tableInOutRequiredSettingNames = tableInOutRequiredSettingNames;
        this.scanFunctions = scanFunctions;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return VgiTransactionHandle.create();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return new VgiMetadata(client, scalarFunctions, aggregateFunctions, scanFunctions);
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

    /**
     * One nullable, unhidden string session property per distinct {@code required_settings} name
     * across every discovered scalar, classic table-in-out, OR declarative-table-backing scan
     * function — {@code SET SESSION <catalog>.<name> = '<value>'} is how a query supplies a VGI
     * setting a function needs (see {@code VgiScalarFunctions.BindCache}'s own note on why
     * session-scoped values, not connector-startup ones, are the right Trino analog for VGI's
     * per-call settings).
     */
    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        Set<String> names = new LinkedHashSet<>(scalarFunctions.requiredSettingNames());
        names.addAll(tableInOutRequiredSettingNames);
        names.addAll(scanFunctions.requiredSettingNames());
        return names.stream()
                .map(name -> PropertyMetadata.stringProperty(name, "VGI function setting '" + name + "'",
                        null, false))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider() {
        return Optional.of(new VgiFunctionProvider(client, scalarFunctions, scalarBindCache, aggregateFunctions));
    }

    @Override
    public void shutdown() {
        client.close();
    }
}
