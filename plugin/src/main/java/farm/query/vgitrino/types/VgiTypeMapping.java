// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Arrow ↔ Trino type/value mapping.
 *
 * <p>Covers what VGI's own {@code farm.query.vgi.types.Schemas} helper and the
 * common declarative-catalog column types actually produce: signed/unsigned
 * integers up to 32 bits widened to the next signed Trino width (VGI's own
 * {@code UInt64} has no exact Trino counterpart and maps to {@code BIGINT}
 * with a documented wraparound caveat — see {@link #toTrinoType}), both float
 * widths, UTF-8 strings, binary, booleans, dates, timestamps <em>without</em>
 * a time zone, and 128-bit decimals. Arrow types with no mapping here
 * (half-precision floats, timestamps WITH a time zone, duration, list/struct/
 * map nesting) throw {@link UnsupportedOperationException} rather than
 * silently truncating or mis-typing a column — extending this class is
 * tracked as follow-up work (the plan's Phase 8), not a silent gap.
 */
public final class VgiTypeMapping {

    private VgiTypeMapping() {}

    /**
     * Map one Arrow field to its Trino {@link Type}.
     *
     * @param field the Arrow field (its {@link ArrowType} plus, for decimals,
     *        precision/scale)
     * @return the corresponding Trino type
     * @throws UnsupportedOperationException if this field's Arrow type has no
     *         mapping (see the class javadoc for what's covered)
     */
    public static Type toTrinoType(Field field) {
        ArrowType type = field.getType();
        return switch (type.getTypeID()) {
            case Bool -> BooleanType.BOOLEAN;
            case Int -> {
                ArrowType.Int i = (ArrowType.Int) type;
                yield switch (i.getBitWidth()) {
                    case 8 -> i.getIsSigned() ? TinyintType.TINYINT : SmallintType.SMALLINT;
                    case 16 -> i.getIsSigned() ? SmallintType.SMALLINT : IntegerType.INTEGER;
                    case 32 -> i.getIsSigned() ? IntegerType.INTEGER : BigintType.BIGINT;
                    // Unsigned 64-bit has no exact Trino type; BIGINT is the closest
                    // fit and is exact for every value <= Long.MAX_VALUE (the vast
                    // majority of real row counts / ids). A value above that wraps
                    // negative on the Trino side — call it out rather than hide it.
                    default -> BigintType.BIGINT;
                };
            }
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) type).getPrecision()) {
                case SINGLE -> RealType.REAL;
                case DOUBLE -> DoubleType.DOUBLE;
                default -> throw unsupported(type, field.getName());
            };
            case Utf8, LargeUtf8 -> VarcharType.createUnboundedVarcharType();
            case Binary, LargeBinary, FixedSizeBinary -> VarbinaryType.VARBINARY;
            case Date -> DateType.DATE;
            case Timestamp -> {
                ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
                if (ts.getTimezone() != null) {
                    throw new UnsupportedOperationException("column '" + field.getName()
                            + "': TIMESTAMP WITH TIME ZONE is not yet supported");
                }
                yield switch (ts.getUnit()) {
                    case SECOND -> TimestampType.createTimestampType(0);
                    case MILLISECOND -> TimestampType.createTimestampType(3);
                    case MICROSECOND -> TimestampType.createTimestampType(6);
                    case NANOSECOND -> TimestampType.createTimestampType(9);
                };
            }
            case Decimal -> {
                ArrowType.Decimal d = (ArrowType.Decimal) type;
                if (d.getBitWidth() != 128) {
                    throw new UnsupportedOperationException("column '" + field.getName()
                            + "': only 128-bit decimals are supported, got " + d.getBitWidth() + "-bit");
                }
                yield DecimalType.createDecimalType(d.getPrecision(), d.getScale());
            }
            default -> throw unsupported(type, field.getName());
        };
    }

    private static UnsupportedOperationException unsupported(ArrowType type, String columnName) {
        return new UnsupportedOperationException(
                "column '" + columnName + "': no Trino mapping for Arrow type " + type);
    }

    /**
     * Append every value of one Arrow column into a Trino block builder.
     *
     * @param trinoType this column's Trino type, from {@link #toTrinoType}
     * @param vector the Arrow vector to read
     * @param builder the block builder to append into
     * @param rowCount the number of rows to read from {@code vector}
     */
    public static void appendColumn(Type trinoType, FieldVector vector, BlockBuilder builder, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            if (vector.isNull(row)) {
                builder.appendNull();
                continue;
            }
            appendValue(trinoType, vector, builder, row);
        }
    }

    private static void appendValue(Type type, FieldVector vector, BlockBuilder builder, int row) {
        switch (vector) {
            case BitVector v -> BooleanType.BOOLEAN.writeBoolean(builder, v.get(row) != 0);
            case TinyIntVector v -> type.writeLong(builder, v.get(row));
            case SmallIntVector v -> type.writeLong(builder, v.get(row));
            case IntVector v -> type.writeLong(builder, v.get(row));
            case BigIntVector v -> type.writeLong(builder, v.get(row));
            case UInt1Vector v -> type.writeLong(builder, v.get(row) & 0xFFL);
            case UInt2Vector v -> type.writeLong(builder, v.get(row) & 0xFFFFL);
            case UInt4Vector v -> type.writeLong(builder, v.get(row) & 0xFFFF_FFFFL);
            case Float4Vector v -> type.writeLong(builder, Float.floatToRawIntBits(v.get(row)));
            case Float8Vector v -> type.writeDouble(builder, v.get(row));
            case VarCharVector v -> type.writeSlice(builder, Slices.wrappedBuffer(v.get(row)));
            case LargeVarCharVector v -> type.writeSlice(builder, Slices.wrappedBuffer(v.get(row)));
            case VarBinaryVector v -> type.writeSlice(builder, Slices.wrappedBuffer(v.get(row)));
            case LargeVarBinaryVector v -> type.writeSlice(builder, Slices.wrappedBuffer(v.get(row)));
            case FixedSizeBinaryVector v -> type.writeSlice(builder, Slices.wrappedBuffer(v.get(row)));
            case DateDayVector v -> type.writeLong(builder, v.get(row));
            case TimeStampMicroVector v -> type.writeLong(builder, v.get(row));
            case DecimalVector v -> Decimals.writeBigDecimal((DecimalType) type, builder, v.getObject(row));
            default -> throw new UnsupportedOperationException(
                    "no value writer for Arrow vector type " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Build a Trino {@link Block} from one Arrow column.
     *
     * @param trinoType this column's Trino type, from {@link #toTrinoType}
     * @param vector the Arrow vector to read
     * @param rowCount the number of rows to read from {@code vector}
     * @return a block holding {@code rowCount} values (or nulls) from {@code vector}
     */
    public static Block toBlock(Type trinoType, FieldVector vector, int rowCount) {
        BlockBuilder builder = trinoType.createBlockBuilder(null, rowCount);
        appendColumn(trinoType, vector, builder, rowCount);
        return builder.build();
    }
}
