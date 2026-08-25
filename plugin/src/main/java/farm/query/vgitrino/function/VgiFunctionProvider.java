// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.split.VgiSplit;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.Optional;

/**
 * Wires {@link VgiTableFunctionSplitProcessor} in as the way Trino actually
 * reads rows from a table-function split — {@code Connector} itself has no
 * direct hook for this; it's reached via {@code Connector.getFunctionProvider()}.
 */
public final class VgiFunctionProvider implements FunctionProvider {

    private final VgiWorkerClient client;

    /** @param client the pooled connection to this catalog's VGI worker */
    public VgiFunctionProvider(VgiWorkerClient client) {
        this.client = client;
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle handle) {
        return new TableFunctionProcessorProvider() {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(
                    ConnectorSession session,
                    ConnectorTableFunctionHandle functionHandle,
                    Optional<ConnectorTableCredentials> credentials,
                    ConnectorSplit split) {
                VgiTableFunctionHandle h = (VgiTableFunctionHandle) functionHandle;
                return new VgiTableFunctionSplitProcessor(client, (VgiSplit) split, h.outputSchema());
            }
        };
    }
}
