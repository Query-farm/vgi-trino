// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.metadata;

import farm.query.vgi.client.ColumnStatisticsDecoder;
import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiScalarFunctions;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiColumnNames;
import farm.query.vgitrino.types.VgiTypeMapping;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

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
    private final VgiScalarFunctions.Registry scalarFunctions;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param scalarFunctions this catalog's discovered scalar functions (see {@link VgiScalarFunctions#discover})
     */
    public VgiMetadata(VgiWorkerClient client, VgiScalarFunctions.Registry scalarFunctions) {
        this.client = client;
        this.scalarFunctions = scalarFunctions;
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
        Optional<VgiTimeTravel.AtClause> at = VgiTimeTravel.resolve(startVersion, endVersion);
        String atUnit = at.map(VgiTimeTravel.AtClause::atUnit).orElse(null);
        String atValue = at.map(VgiTimeTravel.AtClause::atValue).orElse(null);
        return client.withConnection(a -> {
            ItemsResponse tableResp = a.service().catalog_table_get(
                    a.handle(), tableName.getSchemaName(), tableName.getTableName(), atUnit, atValue, null, null);
            if (tableResp.items().isEmpty()) return null;
            TableInfo info = TableInfoDecoder.decode(tableResp.items().get(0));

            TableScanFunctionGetResponse scan = a.service().catalog_table_scan_function_get(
                    a.handle(), tableName.getSchemaName(), tableName.getTableName(), atUnit, atValue, null, null);
            byte[] bindArguments = ScanFunctionArguments.toBindArguments(scan.arguments());

            return new VgiTableHandle(info.schema_name(), info.name(),
                    scan.function_name(), bindArguments, info.columns(), info.cardinality_estimate(),
                    TupleDomain.all(), atUnit, atValue);
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
            columns.add(columnMetadataFor(field));
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
            // The map key is the SQL-facing name — VgiColumnNames.displayName()
            // renames VGI's is_row_id-tagged field to DuckDB's own "rowid"
            // pseudo-column name — while the handle keeps the WIRE name
            // (fields.get(i).getName()) for looking the column up in an
            // actual returned batch.
            out.put(VgiColumnNames.displayName(fields.get(i)), new VgiColumnHandle(fields.get(i).getName(), i));
        }
        return out;
    }

    private static ColumnMetadata columnMetadataFor(Field field) {
        return ColumnMetadata.builder()
                .setName(VgiColumnNames.displayName(field))
                .setType(VgiTypeMapping.toTrinoType(field))
                .setHidden(VgiColumnNames.isRowId(field))
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        VgiTableHandle handle = (VgiTableHandle) table;
        TableStatistics.Builder builder = TableStatistics.builder();
        if (handle.cardinalityEstimate() != null) {
            builder.setRowCount(Estimate.of(handle.cardinalityEstimate()));
        }
        // catalog_table_column_statistics_get is a DECLARATIVE-table-only RPC
        // (schema_name + table name, no bind_call) — a worker with nothing to
        // say returns empty bytes, which ColumnStatisticsDecoder reads as "no
        // statistics" rather than an error, so it's cheap and safe to call
        // unconditionally rather than gating it on row count being known.
        List<farm.query.vgi.catalog.ColumnStatistics> columnStats = client.withConnection(a ->
                ColumnStatisticsDecoder.decode(a.service().catalog_table_column_statistics_get(
                        a.handle(), handle.schemaName(), handle.tableName(), null, null)));
        if (handle.cardinalityEstimate() == null && columnStats.isEmpty()) {
            return TableStatistics.empty();
        }
        Schema schema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        List<Field> fields = schema == null ? List.of() : schema.getFields();
        Map<String, Integer> ordinalsByWireName = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            ordinalsByWireName.put(fields.get(i).getName(), i);
        }
        for (farm.query.vgi.catalog.ColumnStatistics stats : columnStats) {
            Integer ordinal = ordinalsByWireName.get(stats.columnName());
            if (ordinal == null) continue; // a stats row for a column this schema no longer has
            builder.setColumnStatistics(new VgiColumnHandle(stats.columnName(), ordinal),
                    toTrinoColumnStatistics(stats));
        }
        return builder.build();
    }

    private static io.trino.spi.statistics.ColumnStatistics toTrinoColumnStatistics(
            farm.query.vgi.catalog.ColumnStatistics stats) {
        io.trino.spi.statistics.ColumnStatistics.Builder builder =
                io.trino.spi.statistics.ColumnStatistics.builder();
        if (stats.distinctCount() != null) {
            builder.setDistinctValuesCount(Estimate.of(stats.distinctCount()));
        }
        if (!stats.hasNull()) {
            builder.setNullsFraction(Estimate.zero());
        } else if (!stats.hasNotNull()) {
            // Every value is NULL — the column carries no other information.
            builder.setNullsFraction(Estimate.of(1.0));
        }
        // A boolean has_null/has_not_null pair can't say WHAT FRACTION is null
        // when both are true, so nullsFraction stays unknown (the default) —
        // reporting a guessed fraction would mislead the cost model more than
        // reporting nothing does.
        toRange(stats).ifPresent(builder::setRange);
        return builder.build();
    }

    /**
     * VGI's min/max are boxed Java values matching the column's own Arrow
     * type — {@code Long} for an integer column, {@code Double} for a
     * floating-point one, {@code String} (or {@code byte[]}, for geometry
     * columns) for anything else. Trino's {@code DoubleRange} only makes
     * sense for the numeric case, so a non-{@link Number} min/max (a string
     * column's lexical bounds, say) correctly yields no range rather than a
     * meaningless numeric coercion — {@code dataSize}/{@code distinctValuesCount}
     * still carry whatever this connector knows about such a column.
     */
    private static Optional<DoubleRange> toRange(farm.query.vgi.catalog.ColumnStatistics stats) {
        OptionalDouble min = asDouble(stats.min());
        OptionalDouble max = asDouble(stats.max());
        if (min.isEmpty() || max.isEmpty()) return Optional.empty();
        return Optional.of(new DoubleRange(min.getAsDouble(), max.getAsDouble()));
    }

    private static OptionalDouble asDouble(Object value) {
        return value instanceof Number number ? OptionalDouble.of(number.doubleValue()) : OptionalDouble.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        VgiTableHandle handle = (VgiTableHandle) table;
        TupleDomain<VgiColumnHandle> newConstraint =
                constraint.getSummary().transformKeys(VgiColumnHandle.class::cast);
        TupleDomain<VgiColumnHandle> merged = handle.constraint().intersect(newConstraint);
        if (merged.equals(handle.constraint())) {
            // Nothing new to record — returning a result here anyway would
            // have Trino call applyFilter again with the same input forever.
            return Optional.empty();
        }
        VgiTableHandle newHandle = new VgiTableHandle(handle.schemaName(), handle.tableName(),
                handle.scanFunctionName(), handle.scanFunctionArguments(), handle.outputSchema(),
                handle.cardinalityEstimate(), merged, handle.atUnit(), handle.atValue());
        // remainingFilter is the SAME summary we were just given, unchanged:
        // this connector never declares a filter exactly applied (see
        // VgiFilterTranslator's javadoc for why), so Trino must still check
        // every row itself — recording the constraint on the handle is purely
        // informational, letting a worker with auto_apply_filters prune early
        // as an optimization Trino's own re-check stays correct regardless of.
        return Optional.of(new ConstraintApplicationResult<>(
                newHandle, constraint.getSummary(), constraint.getExpression(), false));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle table, ColumnHandle column) {
        VgiTableHandle handle = (VgiTableHandle) table;
        VgiColumnHandle columnHandle = (VgiColumnHandle) column;
        Schema schema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        Field field = schema.getFields().get(columnHandle.ordinal());
        return columnMetadataFor(field);
    }

    /** Real discovery — see {@link VgiScalarFunctions#discover}; may return more than one overload. */
    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name) {
        return scalarFunctions.functionsFor(name.schemaName(), name.functionName());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId) {
        FunctionMetadata metadata = scalarFunctions.metadataFor(functionId);
        if (metadata == null) {
            throw new IllegalArgumentException("unknown function id: " + functionId);
        }
        return metadata;
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(
            ConnectorSession session, FunctionId functionId, BoundSignature boundSignature) {
        // VGI scalars call no other Trino function/operator/cast — nothing to declare.
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }
}
