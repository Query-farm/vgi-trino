// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.split.VgiSplit;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
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

    /**
     * SPIKE — see {@link VgiScalarFunctionSpike}'s javadoc. {@code invocationConvention} is
     * intentionally unexamined: this spike always returns the same {@code (Slice) -> Slice},
     * never-null convention regardless of what Trino asks for, to find out empirically which
     * convention a real query actually requests before building the general (possibly
     * multi-convention) case.
     */
    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId, BoundSignature boundSignature,
            FunctionDependencies functionDependencies, InvocationConvention invocationConvention) {
        return ScalarFunctionImplementation.builder()
                .methodHandle(VgiScalarFunctionSpike.methodHandle())
                .instanceFactory(VgiScalarFunctionSpike.instanceFactory(client))
                .build();
    }
}
