// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.VgiConfig;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.metadata.VgiTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Set;

/**
 * Binds a table's scan function once, then hands off to a {@link VgiSplitSource}
 * that paginates {@code table_function_plan}.
 *
 * <p>The bind happens on whichever pooled connection is available; the
 * resulting {@code bind_call}/{@code bind_opaque_data} are then reused to plan
 * and redeem splits on OTHER pooled connections too. This is intentional, not
 * an oversight — VGI's split tokens are designed to be redeemable "by any
 * worker instance" precisely so a distributed engine's plan phase and its many
 * parallel readers never need to share a connection.
 *
 * <p>v1 does not yet use {@code desiredColumns}/{@code constraint} here — see
 * the plan's Phase 4 for projection/filter pushdown into the plan and bind
 * calls.
 */
public final class VgiSplitManager implements ConnectorSplitManager {

    private final VgiWorkerClient client;
    private final VgiConfig config;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     */
    public VgiSplitManager(VgiWorkerClient client, VgiConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> desiredColumns,
            Constraint constraint) {
        VgiTableHandle handle = (VgiTableHandle) table;
        return client.withConnection(a -> {
            BindRequest bindRequest = new BindRequest(
                    handle.scanFunctionName(),
                    handle.scanFunctionArguments(),
                    "TABLE",
                    null,           // input_schema — producer-mode table function
                    null,           // settings
                    null,           // secrets
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    false,          // resolved_secrets_provided
                    null, null,     // at_unit / at_value — Phase 8 (time travel)
                    null, null,     // copy_from / copy_to
                    handle.schemaName());
            BindResponse bound = a.service().bind(bindRequest, null);
            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);
            return new VgiSplitSource(client, config, serializedBindCall, bound.opaque_data());
        });
    }
}
