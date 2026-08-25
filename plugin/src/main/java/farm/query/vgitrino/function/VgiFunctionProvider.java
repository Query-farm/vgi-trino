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
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Collections.nCopies;

/**
 * Wires {@link VgiTableFunctionSplitProcessor} in as the way Trino actually
 * reads rows from a table-function split — {@code Connector} itself has no
 * direct hook for this; it's reached via {@code Connector.getFunctionProvider()}
 * — and, separately, dispatches connector-defined scalar functions to
 * {@link VgiScalarFunctions} (see its javadoc for the real design).
 */
public final class VgiFunctionProvider implements FunctionProvider {

    private final VgiWorkerClient client;
    private final VgiScalarFunctions.Registry scalarFunctions;
    private final VgiScalarFunctions.BindCache bindCache;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param scalarFunctions this catalog's discovered scalar functions (see {@link VgiScalarFunctions#discover})
     * @param bindCache the shared, catalog-scoped bind-result cache every scalar call site reuses
     */
    public VgiFunctionProvider(VgiWorkerClient client, VgiScalarFunctions.Registry scalarFunctions,
            VgiScalarFunctions.BindCache bindCache) {
        this.client = client;
        this.scalarFunctions = scalarFunctions;
        this.bindCache = bindCache;
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
     * Build the {@link ScalarFunctionImplementation} for one call site.
     *
     * <p>Always builds the raw {@link MethodHandle} against the simplest
     * honest convention this connector can implement directly — {@code
     * BOXED_NULLABLE} arguments, {@code NULLABLE_RETURN} — and lets {@link
     * ScalarFunctionAdapter#adapt} bridge to whatever convention Trino
     * actually requested at this call site, the same pattern real connectors
     * (Iceberg, AI functions) use. Arrow already represents every value as
     * nullable, so handling nulls ourselves this way is the natural fit.
     */
    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId, BoundSignature boundSignature,
            FunctionDependencies functionDependencies, InvocationConvention invocationConvention) {
        VgiScalarFunctions.Entry entry = scalarFunctions.entryFor(functionId);
        if (entry == null) {
            throw new IllegalArgumentException("unknown VGI scalar function id: " + functionId);
        }
        List<Type> argumentTypes = boundSignature.getArgumentTypes();
        Type returnType = boundSignature.getReturnType();
        VgiScalarFunctions.CallConfig callConfig =
                VgiScalarFunctions.buildCallConfig(entry, argumentTypes, returnType);

        MethodHandle rawHandle = VgiScalarFunctions.methodHandle(argumentTypes.size());
        MethodHandle instanceFactory = VgiScalarFunctions.instanceFactory(client, callConfig, bindCache);

        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(argumentTypes.size(), BOXED_NULLABLE), NULLABLE_RETURN, false, true);
        MethodHandle adapted = ScalarFunctionAdapter.adapt(
                rawHandle, returnType, argumentTypes, actualConvention, invocationConvention);

        return ScalarFunctionImplementation.builder()
                .methodHandle(adapted)
                .instanceFactory(instanceFactory)
                .build();
    }
}
