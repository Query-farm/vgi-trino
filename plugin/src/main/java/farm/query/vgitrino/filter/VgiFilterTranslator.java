// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.filter;

import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ScalarValue;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates one column's Trino {@link Domain} into a VGI {@link FilterPredicate}.
 *
 * <p>Deliberately narrow: only the types this class explicitly recognizes are
 * translated ({@link #isSupported}), everything else — including a domain
 * shape this class doesn't have a case for — returns {@code null} and the
 * caller pushes nothing for that column. That is always safe for
 * correctness (Trino still evaluates the original predicate over whatever
 * rows come back — see {@link farm.query.vgitrino.metadata.VgiMetadata#applyFilter}),
 * but a wrong translation would not be: a worker with {@code auto_apply_filters}
 * on trusts the pushed predicate to prune rows itself, and Trino's own
 * re-check can never recover a row the worker never returned. Every case here
 * is covered by {@code VgiFilterPushdownQueryRunnerTest} against a real
 * {@code auto_apply_filters=true} fixture before being trusted.
 *
 * <p>128-bit decimals, REAL (float32), DATE, and TIMESTAMP are deliberately
 * NOT translated yet — not because they're hard in principle, but because
 * this class hasn't been tested against them and a wrong byte-level encoding
 * for one of those is exactly the silent-data-loss risk described above.
 */
public final class VgiFilterTranslator {

    private VgiFilterTranslator() {}

    /**
     * @param field the column's Arrow field (from the table's bind-time schema)
     * @return {@code true} if {@link #translate} can build a predicate for this type
     */
    public static boolean isSupported(Field field) {
        ArrowType type = field.getType();
        return switch (type.getTypeID()) {
            case Int, Utf8, LargeUtf8, Binary, LargeBinary, Bool -> true;
            case FloatingPoint -> ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE;
            default -> false;
        };
    }

    /**
     * Translate one column's domain into a predicate, or {@code null} if
     * there's nothing worth pushing (an unconstrained domain) or this
     * column's type/domain shape isn't one this class translates.
     *
     * @param domain the column's Trino domain
     * @param field the column's Arrow field
     * @return the predicate, or {@code null}
     */
    public static FilterPredicate translate(Domain domain, Field field) {
        if (!isSupported(field)) return null;
        if (domain.isAll()) return null;
        if (domain.isNone()) return null; // Trino rarely reaches applyFilter with this; let it handle it

        ArrowType type = field.getType();
        ValueSet values = domain.getValues();
        FilterPredicate valuePredicate;
        if (values.isDiscreteSet()) {
            List<Object> discrete = values.getDiscreteSet();
            if (discrete.isEmpty()) return null;
            List<FilterPredicate> eqs = new ArrayList<>(discrete.size());
            for (Object v : discrete) eqs.add(FilterPredicate.eq(toScalarValue(v, type)));
            valuePredicate = eqs.size() == 1 ? eqs.get(0) : FilterPredicate.or(eqs.toArray(new FilterPredicate[0]));
        } else {
            List<Range> ranges = values.getRanges().getOrderedRanges();
            List<FilterPredicate> rangePredicates = new ArrayList<>(ranges.size());
            for (Range range : ranges) {
                FilterPredicate p = rangeToPredicate(range, type);
                if (p == null) return null; // an unbounded-both-ways range inside a non-ALL domain: bail safely
                rangePredicates.add(p);
            }
            if (rangePredicates.isEmpty()) return null;
            valuePredicate = rangePredicates.size() == 1
                    ? rangePredicates.get(0) : FilterPredicate.or(rangePredicates.toArray(new FilterPredicate[0]));
        }
        return domain.isNullAllowed() ? FilterPredicate.or(valuePredicate, FilterPredicate.isNull()) : valuePredicate;
    }

    private static FilterPredicate rangeToPredicate(Range range, ArrowType type) {
        if (range.isAll()) return null;
        if (range.isSingleValue()) {
            return FilterPredicate.eq(toScalarValue(range.getSingleValue(), type));
        }
        List<FilterPredicate> bounds = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            ScalarValue lo = toScalarValue(range.getLowBoundedValue(), type);
            bounds.add(range.isLowInclusive() ? FilterPredicate.ge(lo) : FilterPredicate.gt(lo));
        }
        if (!range.isHighUnbounded()) {
            ScalarValue hi = toScalarValue(range.getHighBoundedValue(), type);
            bounds.add(range.isHighInclusive() ? FilterPredicate.le(hi) : FilterPredicate.lt(hi));
        }
        if (bounds.isEmpty()) return null; // both unbounded — isAll() should have caught this already
        return bounds.size() == 1 ? bounds.get(0) : FilterPredicate.and(bounds.toArray(new FilterPredicate[0]));
    }

    /**
     * Convert one Trino native-representation value into a {@link ScalarValue}
     * typed as the column's ACTUAL Arrow type — not inferred from the Java
     * value's class, which would silently pick a different width/precision
     * than the column's own (e.g. inferring {@code float64} for a value that
     * must compare against a {@code float32} column).
     */
    private static ScalarValue toScalarValue(Object trinoValue, ArrowType type) {
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return ScalarValue.of(type, ((Slice) trinoValue).toStringUtf8());
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
            return ScalarValue.of(type, ((Slice) trinoValue).getBytes());
        }
        if (type instanceof ArrowType.Bool) {
            return ScalarValue.of(type, trinoValue);
        }
        if (type instanceof ArrowType.Int) {
            return ScalarValue.of(type, ((Number) trinoValue).longValue());
        }
        if (type instanceof ArrowType.FloatingPoint) {
            return ScalarValue.of(type, ((Number) trinoValue).doubleValue());
        }
        // Unreachable: isSupported() gates every call site to this method.
        throw new IllegalStateException("no filter value conversion for Arrow type " + type);
    }
}
