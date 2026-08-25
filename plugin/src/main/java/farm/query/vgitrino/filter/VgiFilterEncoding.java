// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.filter;

import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumn;
import farm.query.vgi.client.ProjectedColumns;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Encodes a stored {@code TupleDomain<VgiColumnHandle>} into
 * {@code InitRequest}/{@code TableFunctionPlanRequest}'s
 * {@code pushdown_filters} bytes for one specific invocation's projected
 * column list.
 *
 * <p>A pushdown filter's column index is relative to the PROJECTION that
 * invocation actually requested (see {@link ProjectedColumn}'s own javadoc for
 * why), which is why this takes the invocation's own {@code projectedColumns}
 * rather than encoding once and reusing the result across calls with
 * different projections.
 */
public final class VgiFilterEncoding {

    private VgiFilterEncoding() {}

    /**
     * @param constraint the stored constraint (from {@code applyFilter}), or {@code TupleDomain.all()}
     * @param fullSchema the table's full (bind-time) Arrow schema
     * @param projectedColumns this invocation's projected columns, in the
     *        exact order sent as {@code projection_ids}
     * @return the encoded {@code pushdown_filters} bytes, or {@code null} if
     *         there's nothing to push (no constraint, or every constrained
     *         column is outside this projection / untranslatable)
     */
    public static byte[] encode(TupleDomain<VgiColumnHandle> constraint, Schema fullSchema,
            List<VgiColumnHandle> projectedColumns) {
        if (constraint.isAll() || constraint.isNone()) return null;
        Optional<Map<VgiColumnHandle, Domain>> domains = constraint.getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) return null;

        List<String> projectedNames = projectedColumns.stream().map(VgiColumnHandle::name).toList();
        ProjectedColumns cols = ProjectedColumns.of(projectedNames);

        PushdownFiltersEncoder encoder = PushdownFiltersEncoder.builder();
        boolean any = false;
        for (Map.Entry<VgiColumnHandle, Domain> entry : domains.get().entrySet()) {
            VgiColumnHandle column = entry.getKey();
            if (!projectedNames.contains(column.name())) continue; // not in this invocation's projection
            Field field = fullSchema.getFields().get(column.ordinal());
            FilterPredicate predicate = VgiFilterTranslator.translate(entry.getValue(), field);
            if (predicate == null) continue;
            encoder.filter(cols.column(column.name()), predicate);
            any = true;
        }
        if (!any) return null;
        return encoder.encode().pushdownFilters();
    }
}
