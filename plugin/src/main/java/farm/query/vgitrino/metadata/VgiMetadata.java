// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.metadata;

import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Catalog/schema/table discovery, backed by the VGI catalog RPCs
 * ({@code catalog_schemas}, {@code catalog_schema_contents_tables},
 * {@code catalog_table_get}, {@code catalog_table_scan_function_get}).
 *
 * <p>v1 scope: read-only discovery of declarative/function-backed tables via
 * the legacy single-function scan path. No pushdown yet (Phase 4), no
 * multi-branch tables ({@code catalog_table_scan_branches_get}), no views —
 * see the plan's non-goals.
 */
public final class VgiMetadata implements ConnectorMetadata {

    private final VgiWorkerClient client;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     */
    public VgiMetadata(VgiWorkerClient client) {
        this.client = client;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return client.withConnection(a -> {
            List<String> names = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                names.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
            }
            return names;
        });
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameFilter) {
        return client.withConnection(a -> {
            List<String> schemas;
            if (schemaNameFilter.isPresent()) {
                schemas = List.of(schemaNameFilter.get());
            } else {
                schemas = new ArrayList<>();
                for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                    schemas.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
                }
            }
            List<SchemaTableName> out = new ArrayList<>();
            for (String schema : schemas) {
                ItemsResponse tables = a.service().catalog_schema_contents_tables(
                        a.handle(), schema, null, null);
                for (byte[] item : tables.items()) {
                    TableInfo info = TableInfoDecoder.decode(item);
                    out.add(new SchemaTableName(info.schema_name(), info.name()));
                }
            }
            return out;
        });
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            // Time travel maps onto BindRequest.at_unit/at_value, which VGI
            // supports — just not wired up in this v1 slice yet.
            throw new UnsupportedOperationException("VGI connector does not yet support time travel (FOR VERSION/TIMESTAMP AS OF)");
        }
        return client.withConnection(a -> {
            ItemsResponse tableResp = a.service().catalog_table_get(
                    a.handle(), tableName.getSchemaName(), tableName.getTableName(), null, null, null, null);
            if (tableResp.items().isEmpty()) return null;
            TableInfo info = TableInfoDecoder.decode(tableResp.items().get(0));

            TableScanFunctionGetResponse scan = a.service().catalog_table_scan_function_get(
                    a.handle(), tableName.getSchemaName(), tableName.getTableName(), null, null, null, null);
            byte[] bindArguments = ScanFunctionArguments.toBindArguments(scan.arguments());

            return new VgiTableHandle(info.schema_name(), info.name(),
                    scan.function_name(), bindArguments, info.columns(), info.cardinality_estimate());
        });
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        VgiTableHandle handle = (VgiTableHandle) table;
        Schema schema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        if (schema == null) {
            throw new TableNotFoundException(new SchemaTableName(handle.schemaName(), handle.tableName()));
        }
        List<ColumnMetadata> columns = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            columns.add(new ColumnMetadata(field.getName(), VgiTypeMapping.toTrinoType(field)));
        }
        return new ConnectorTableMetadata(
                new SchemaTableName(handle.schemaName(), handle.tableName()), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        VgiTableHandle handle = (VgiTableHandle) table;
        Schema schema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        Map<String, ColumnHandle> out = new LinkedHashMap<>();
        List<Field> fields = schema == null ? List.of() : schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            out.put(fields.get(i).getName(), new VgiColumnHandle(fields.get(i).getName(), i));
        }
        return out;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        // Row count only for v1 — the worker's own cardinality_estimate from
        // catalog_table_get, at no extra RPC cost. Per-column statistics
        // (table_function_statistics / catalog_table_column_statistics_get,
        // both already decodable client-side via ColumnStatisticsDecoder) are
        // a documented follow-up, not wired up here yet.
        VgiTableHandle handle = (VgiTableHandle) table;
        if (handle.cardinalityEstimate() == null) {
            return TableStatistics.empty();
        }
        return TableStatistics.builder()
                .setRowCount(Estimate.of(handle.cardinalityEstimate()))
                .build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle column) {
        VgiTableHandle handle = (VgiTableHandle) table;
        VgiColumnHandle columnHandle = (VgiColumnHandle) column;
        Schema schema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        Field field = schema.getFields().get(columnHandle.ordinal());
        return new ColumnMetadata(field.getName(), VgiTypeMapping.toTrinoType(field));
    }
}
