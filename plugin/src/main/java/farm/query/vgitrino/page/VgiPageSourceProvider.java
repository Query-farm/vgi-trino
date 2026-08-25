// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.page;

import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.filter.VgiFilterEncoding;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import farm.query.vgitrino.metadata.VgiTableHandle;
import farm.query.vgitrino.split.VgiSplit;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Comparator;
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
        // Sorted by ordinal independently of VgiPageSource's own internal sort
        // for projection_ids — the two must agree on order for the filter's
        // column indices to line up, and "sort this same column set by
        // ordinal" gives the same answer wherever it's computed.
        List<VgiColumnHandle> sortedColumns = vgiColumns.stream()
                .sorted(Comparator.comparingInt(VgiColumnHandle::ordinal))
                .toList();
        Schema fullSchema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        // Merge in whatever of a join's dynamic filter has arrived by the time
        // this SPECIFIC split is redeemed — best-effort, not awaited here: the
        // split source's own getRequestedDynamicFilterWaitTimeoutMillis is what
        // holds the plan phase for the filter, so by the time a split exists to
        // redeem at all, the filter reaching this point is already as complete
        // as it's going to get without blocking twice.
        TupleDomain<VgiColumnHandle> dynamicPredicate =
                dynamicFilter.getCurrentPredicate().transformKeys(VgiColumnHandle.class::cast);
        TupleDomain<VgiColumnHandle> merged = handle.constraint().intersect(dynamicPredicate);
        byte[] pushdownFilters = VgiFilterEncoding.encode(merged, fullSchema, sortedColumns);
        return new VgiPageSource(client, vgiSplit, vgiColumns, handle.outputSchema(), pushdownFilters);
    }
}
