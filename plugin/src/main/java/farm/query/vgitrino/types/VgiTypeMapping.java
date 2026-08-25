// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Arrow ↔ Trino type/value mapping.
 *
 * <p>Covers what VGI's own {@code farm.query.vgi.types.Schemas} helper and the
 * common declarative-catalog column types actually produce: signed/unsigned
 * integers up to 32 bits widened to the next signed Trino width (VGI's own
 * {@code UInt64} has no exact Trino counterpart and maps to {@code BIGINT}
 * with a documented wraparound caveat — see {@link #toTrinoType}), both float
 * widths, UTF-8 strings, binary, booleans, dates, timestamps <em>without</em>
 * a time zone, 128-bit decimals, and arbitrarily nested {@code Struct}/
 * {@code List}/{@code FixedSizeList} (mapped to Trino {@link RowType}/
 * {@link ArrayType} — see {@link #toTrinoType}/{@link #toArrowField} for the
 * one asymmetry: writing a Trino {@link ArrayType} value always produces an
 * Arrow {@code List}, never a {@code FixedSizeList}, since Trino's type
 * system carries no fixed-length information to produce one from). Arrow
 * types with no mapping here (half-precision floats, timestamps WITH a time
 * zone, duration, {@code Map}) throw {@link UnsupportedOperationException}
 * rather than silently truncating or mis-typing a column — extending this
 * class further is tracked as follow-up work, not a silent gap. 128-bit
 * decimals are themselves not covered in the Trino -> Arrow (scalar-argument)
 * direction — see {@link #toArrowField}'s own note.
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
            case Struct -> {
                List<RowType.Field> rowFields = new ArrayList<>(field.getChildren().size());
                for (Field child : field.getChildren()) {
                    rowFields.add(RowType.field(child.getName(), toTrinoType(child)));
                }
                yield RowType.from(rowFields);
            }
            // A list's single child field carries the element type; its own name (conventionally
            // "item") carries no meaning Trino's ArrayType preserves.
            case List, LargeList, FixedSizeList -> new ArrayType(toTrinoType(field.getChildren().get(0)));
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
            case StructVector v -> type.writeObject(builder, readNested(type, v, row));
            case ListVector v -> type.writeObject(builder, readNested(type, v, row));
            case FixedSizeListVector v -> type.writeObject(builder, readNested(type, v, row));
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

    // ------------------------------------------------------------------
    // Trino -> Arrow: scalar-function arguments/return only (VgiScalarFunctions).
    // Same core-type coverage as toTrinoType's reverse direction.
    // ------------------------------------------------------------------

    /**
     * Map one Trino {@link Type} to the Arrow field VGI expects for it — the
     * write-side mirror of {@link #toTrinoType}, for a scalar function's
     * per-row {@code input_schema} or bind-time constant argument.
     *
     * <p>Equivalent to {@link #toArrowField(Type, String, Field)} with no hint
     * — an {@link ArrayType} always maps to an Arrow {@code List} without one
     * (see that method's own javadoc for why a hint can produce a
     * {@code FixedSizeList} instead).
     *
     * @param type the Trino type (one of {@link #toTrinoType}'s core-covered types)
     * @param name the field name
     * @return a nullable Arrow field of the corresponding type
     * @throws UnsupportedOperationException if {@code type} has no mapping
     *         here (see the class javadoc)
     */
    public static Field toArrowField(Type type, String name) {
        return toArrowField(type, name, null);
    }

    /**
     * Map one Trino {@link Type} to the Arrow field VGI expects for it, using
     * {@code hint} — the argument's ORIGINAL discovery-time Arrow field (from
     * {@code FunctionInfo.arguments}), when the caller has one — to decide an
     * {@link ArrayType}'s shape.
     *
     * <p>Trino's {@code ArrayType} carries no fixed-length information of its
     * own, so without a hint this always produces an Arrow {@code List}. A
     * worker that declared the argument as a {@code FixedSizeList} (e.g. the
     * reference fixture's {@code geo_distance_fixed}/{@code geo_centroid_fixed},
     * whose points are {@code list_(float64, 2)}) may validate the incoming
     * batch's column shape strictly enough to reject a plain {@code List} in
     * its place — passing the discovery-time field back here as {@code hint}
     * reproduces the exact {@code FixedSizeList} width instead. Recurses into
     * struct fields and list elements with their own corresponding child hint,
     * so a {@code FixedSizeList} nested inside a struct field (or another
     * list) still gets its width preserved.
     *
     * @param type the Trino type (one of {@link #toTrinoType}'s core-covered types)
     * @param name the field name
     * @param hint the original Arrow field this argument was discovered with,
     *        or {@code null} when there isn't one (a bind-time constant, or an
     *        {@code any}-typed argument, whose concrete type is only known at
     *        this specific call site)
     * @return a nullable Arrow field of the corresponding type
     * @throws UnsupportedOperationException if {@code type} has no mapping
     *         here (see the class javadoc)
     */
    public static Field toArrowField(Type type, String name, Field hint) {
        return switch (type) {
            case BooleanType t -> primitiveField(name, new ArrowType.Bool());
            case TinyintType t -> primitiveField(name, new ArrowType.Int(8, true));
            case SmallintType t -> primitiveField(name, new ArrowType.Int(16, true));
            case IntegerType t -> primitiveField(name, new ArrowType.Int(32, true));
            case BigintType t -> primitiveField(name, new ArrowType.Int(64, true));
            case RealType t -> primitiveField(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            case DoubleType t -> primitiveField(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case VarcharType t -> primitiveField(name, new ArrowType.Utf8());
            case VarbinaryType t -> primitiveField(name, new ArrowType.Binary());
            case DateType t -> primitiveField(name, new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY));
            case TimestampType t -> primitiveField(name, new ArrowType.Timestamp(microTimeUnit(t.getPrecision()), null));
            case RowType t -> {
                List<Field> hintChildren = hint != null ? hint.getChildren() : List.<Field>of();
                List<RowType.Field> fields = t.getFields();
                List<Field> children = new ArrayList<>(fields.size());
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field f = fields.get(i);
                    Field childHint = i < hintChildren.size() ? hintChildren.get(i) : null;
                    children.add(toArrowField(f.getType(), f.getName().orElse("field" + i), childHint));
                }
                yield new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
            }
            case ArrayType t -> {
                Field elementHint = hint != null && !hint.getChildren().isEmpty() ? hint.getChildren().get(0) : null;
                Field elementField = toArrowField(t.getElementType(), "item", elementHint);
                ArrowType shape = hint != null && hint.getType().getTypeID() == ArrowType.ArrowTypeID.FixedSizeList
                        ? new ArrowType.FixedSizeList(((ArrowType.FixedSizeList) hint.getType()).getListSize())
                        : new ArrowType.List();
                yield new Field(name, FieldType.nullable(shape), List.of(elementField));
            }
            // DECIMAL is deliberately NOT covered in this (Trino -> Arrow, scalar-
            // function) direction: Trino's BOXED_NULLABLE representation for a
            // decimal argument is a raw Long (short) or Int128 (long) unscaled
            // value, not a BigDecimal, and bridging that correctly needs real,
            // separately-verified Int128<->Arrow-decimal-vector handling — see
            // the README's deferred-items list. toTrinoType's read direction
            // (declarative table columns) is unaffected.
            default -> throw new UnsupportedOperationException(
                    "column '" + name + "': no Arrow mapping for Trino type " + type);
        };
    }

    private static Field primitiveField(String name, ArrowType arrowType) {
        return new Field(name, new FieldType(true, arrowType, null), null);
    }

    private static TimeUnit microTimeUnit(int precision) {
        // Matches toTrinoType's own precision->unit mapping in reverse; VGI's
        // own fixtures use microsecond timestamps almost exclusively, but every
        // precision toTrinoType can produce must round-trip back out.
        return switch (precision) {
            case 0 -> TimeUnit.SECOND;
            case 3 -> TimeUnit.MILLISECOND;
            case 6 -> TimeUnit.MICROSECOND;
            case 9 -> TimeUnit.NANOSECOND;
            default -> TimeUnit.MICROSECOND;
        };
    }

    /**
     * Write one Trino native argument value (the {@code BOXED_NULLABLE}
     * representation — boxed {@link Long}/{@link Double}/{@link Boolean}/
     * {@link Slice}, or {@code null}) into row {@code row} of {@code vector}.
     *
     * @param type the argument's Trino type
     * @param vector the destination Arrow vector, built from {@link #toArrowField}
     * @param row the row index to write
     * @param value the boxed value, or {@code null}
     */
    public static void writeValue(Type type, FieldVector vector, int row, Object value) {
        if (value == null) {
            vector.setNull(row);
            return;
        }
        switch (vector) {
            case BitVector v -> v.setSafe(row, ((Boolean) value) ? 1 : 0);
            case TinyIntVector v -> v.setSafe(row, ((Long) value).byteValue());
            case SmallIntVector v -> v.setSafe(row, ((Long) value).shortValue());
            case IntVector v -> v.setSafe(row, ((Long) value).intValue());
            case BigIntVector v -> v.setSafe(row, (Long) value);
            case Float4Vector v -> v.setSafe(row, Float.intBitsToFloat(((Long) value).intValue()));
            case Float8Vector v -> v.setSafe(row, (Double) value);
            case VarCharVector v -> {
                Slice s = (Slice) value;
                v.setSafe(row, s.getBytes());
            }
            case VarBinaryVector v -> {
                Slice s = (Slice) value;
                v.setSafe(row, s.getBytes());
            }
            case DateDayVector v -> v.setSafe(row, ((Long) value).intValue());
            case TimeStampMicroVector v -> v.setSafe(row, (Long) value);
            case StructVector v -> writeNested(type, v, row, value);
            case ListVector v -> writeNested(type, v, row, value);
            case FixedSizeListVector v -> writeNested(type, v, row, value);
            default -> throw new UnsupportedOperationException(
                    "no value writer for Arrow vector type " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Read one value back out of row {@code row} of {@code vector}, as the
     * {@code NULLABLE_RETURN}/{@code BOXED_NULLABLE} boxed representation
     * ({@link Long}/{@link Double}/{@link Boolean}/{@link Slice}, or
     * {@code null}) — the read-side counterpart of {@link #writeValue}, for a
     * scalar function's single output column.
     *
     * @param type the return/argument's Trino type
     * @param vector the Arrow vector to read
     * @param row the row index to read
     * @return the boxed value, or {@code null}
     */
    public static Object readValue(Type type, FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        return switch (vector) {
            case BitVector v -> v.get(row) != 0;
            case TinyIntVector v -> (long) v.get(row);
            case SmallIntVector v -> (long) v.get(row);
            case IntVector v -> (long) v.get(row);
            case BigIntVector v -> v.get(row);
            case UInt1Vector v -> v.get(row) & 0xFFL;
            case UInt2Vector v -> v.get(row) & 0xFFFFL;
            case UInt4Vector v -> v.get(row) & 0xFFFF_FFFFL;
            case Float4Vector v -> (long) Float.floatToRawIntBits(v.get(row));
            case Float8Vector v -> v.get(row);
            case VarCharVector v -> Slices.wrappedBuffer(v.get(row));
            case LargeVarCharVector v -> Slices.wrappedBuffer(v.get(row));
            case VarBinaryVector v -> Slices.wrappedBuffer(v.get(row));
            case LargeVarBinaryVector v -> Slices.wrappedBuffer(v.get(row));
            case FixedSizeBinaryVector v -> Slices.wrappedBuffer(v.get(row));
            case DateDayVector v -> (long) v.get(row);
            case TimeStampMicroVector v -> v.get(row);
            case StructVector v -> readNested(type, v, row);
            case ListVector v -> readNested(type, v, row);
            case FixedSizeListVector v -> readNested(type, v, row);
            default -> throw new UnsupportedOperationException(
                    "no value reader for Arrow vector type " + vector.getClass().getSimpleName());
        };
    }

    // ------------------------------------------------------------------
    // Struct/List/FixedSizeList: recursive bridging shared by appendValue
    // (table-column reading), readValue, and writeValue.
    // ------------------------------------------------------------------

    /**
     * Read one Arrow struct/list/fixed-size-list cell as the Trino "object" representation
     * ({@link SqlRow} for a struct, {@link Block} for a list) — what {@link RowType}/
     * {@link ArrayType} each override {@code Type.writeObject} to accept. Recurses through
     * {@link #readValue} per child/element, so arbitrarily nested struct-of-list-of-struct shapes
     * round-trip without special-casing depth.
     */
    private static Object readNested(Type type, FieldVector vector, int row) {
        return switch (vector) {
            case StructVector sv -> {
                RowType rowType = (RowType) type;
                List<RowType.Field> fields = rowType.getFields();
                yield RowValueBuilder.buildRowValue(rowType, fieldBuilders -> {
                    for (int i = 0; i < fields.size(); i++) {
                        Type fieldType = fields.get(i).getType();
                        Field childArrowField = sv.getField().getChildren().get(i);
                        FieldVector childVector = sv.getChild(childArrowField.getName());
                        writeBoxedValue(fieldType, fieldBuilders.get(i), readValue(fieldType, childVector, row));
                    }
                });
            }
            case ListVector lv -> {
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();
                int start = lv.getElementStartIndex(row);
                int end = lv.getElementEndIndex(row);
                FieldVector inner = lv.getDataVector();
                yield ArrayValueBuilder.buildArrayValue(arrayType, end - start, elementBuilder -> {
                    for (int i = start; i < end; i++) {
                        writeBoxedValue(elementType, elementBuilder, readValue(elementType, inner, i));
                    }
                });
            }
            case FixedSizeListVector fl -> {
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();
                int width = fl.getListSize();
                int start = row * width;
                FieldVector inner = fl.getDataVector();
                yield ArrayValueBuilder.buildArrayValue(arrayType, width, elementBuilder -> {
                    for (int i = 0; i < width; i++) {
                        writeBoxedValue(elementType, elementBuilder, readValue(elementType, inner, start + i));
                    }
                });
            }
            default -> throw new UnsupportedOperationException(
                    "no nested value reader for Arrow vector type " + vector.getClass().getSimpleName());
        };
    }

    /**
     * Write a Trino {@link SqlRow}/{@link Block} (a struct/list argument's {@code BOXED_NULLABLE}
     * representation) into an Arrow struct/list vector — the write-side mirror of
     * {@link #readNested}, recursing through {@link #writeValue} per child/element.
     */
    private static void writeNested(Type type, FieldVector vector, int row, Object value) {
        switch (vector) {
            case StructVector sv -> {
                RowType rowType = (RowType) type;
                SqlRow sqlRow = (SqlRow) value;
                List<RowType.Field> fields = rowType.getFields();
                int rawIndex = sqlRow.getRawIndex();
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i).getType();
                    Block fieldBlock = sqlRow.getRawFieldBlock(i);
                    Field childArrowField = sv.getField().getChildren().get(i);
                    FieldVector childVector = sv.getChild(childArrowField.getName());
                    writeValue(fieldType, childVector, row, readBoxedValue(fieldType, fieldBlock, rawIndex));
                }
                sv.setIndexDefined(row);
            }
            case ListVector lv -> {
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();
                Block arrayBlock = (Block) value;
                int startOffset = lv.startNewValue(row);
                FieldVector dataVector = lv.getDataVector();
                int count = arrayBlock.getPositionCount();
                int needed = startOffset + count;
                while (dataVector.getValueCapacity() < needed) dataVector.reAlloc();
                for (int i = 0; i < count; i++) {
                    writeValue(elementType, dataVector, startOffset + i, readBoxedValue(elementType, arrayBlock, i));
                }
                lv.endValue(row, count);
                if (needed > dataVector.getValueCount()) dataVector.setValueCount(needed);
            }
            case FixedSizeListVector fl -> {
                ArrayType arrayType = (ArrayType) type;
                Type elementType = arrayType.getElementType();
                Block arrayBlock = (Block) value;
                int width = fl.getListSize();
                if (arrayBlock.getPositionCount() != width) {
                    throw new IllegalArgumentException("expected " + width
                            + " elements for a fixed-size list argument, got " + arrayBlock.getPositionCount());
                }
                int startOffset = fl.startNewValue(row);
                FieldVector dataVector = fl.getDataVector();
                int needed = startOffset + width;
                while (dataVector.getValueCapacity() < needed) dataVector.reAlloc();
                for (int i = 0; i < width; i++) {
                    writeValue(elementType, dataVector, startOffset + i, readBoxedValue(elementType, arrayBlock, i));
                }
                fl.setNotNull(row);
                if (needed > dataVector.getValueCount()) dataVector.setValueCount(needed);
            }
            default -> throw new UnsupportedOperationException(
                    "no nested value writer for Arrow vector type " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Read one field/element out of a Trino {@link Block} in the same boxed representation
     * ({@link Long}/{@link Double}/{@link Boolean}/{@link Slice}/{@link SqlRow}/{@link Block}, or
     * {@code null}) {@link #readValue}/{@link #writeValue} use elsewhere — dispatching on
     * {@link Type#getJavaType()} since {@code Type.getObject}/{@code writeObject} are only valid
     * for types whose native representation genuinely IS an object (Slice, SqlRow, Block, Int128,
     * …); a primitive-native type (long/double/boolean) requires its own typed getter/writer.
     */
    private static Object readBoxedValue(Type type, Block block, int position) {
        if (block.isNull(position)) return null;
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) return type.getLong(block, position);
        if (javaType == double.class) return type.getDouble(block, position);
        if (javaType == boolean.class) return type.getBoolean(block, position);
        return type.getObject(block, position);
    }

    /** The write-side mirror of {@link #readBoxedValue}. */
    private static void writeBoxedValue(Type type, BlockBuilder builder, Object value) {
        if (value == null) {
            builder.appendNull();
            return;
        }
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) type.writeLong(builder, (Long) value);
        else if (javaType == double.class) type.writeDouble(builder, (Double) value);
        else if (javaType == boolean.class) type.writeBoolean(builder, (Boolean) value);
        else type.writeObject(builder, value);
    }

    /**
     * Convert a value in this connector's boxed representation (as {@link #readValue} returns, or
     * {@link #writeValue} expects) into the plain, recursively-nested Java shape VGI's
     * {@code ArgumentsEncoder}/{@code ScalarValue} client-side encoder expects for a bind-time
     * constant argument — used only for {@code vgi_const} scalar-function arguments, never for
     * the per-row exchange path (which uses {@link #writeValue}/{@link #readValue} directly).
     *
     * <p>{@link Slice} becomes {@link String} (UTF-8) or {@code byte[]} (binary); a struct becomes
     * a {@link LinkedHashMap} (preserving field order); a list becomes an {@link ArrayList} —
     * recursively, so an arbitrarily nested struct/list constant converts in one pass.
     *
     * <p><strong>One honest caveat</strong>: nested values are converted to whatever Java class
     * {@code ScalarValue.of(Object)} naturally infers a matching Arrow type from — a {@code REAL}
     * (32-bit float) struct/list field specifically widens to {@code FLOAT64} on the wire this
     * way, since {@code ScalarValue}'s own type inference only distinguishes {@code Float} vs.
     * {@code Double} it never receives (this method always hands it a {@code Double}). The
     * top-level value's own declared Arrow type (built via {@link #toArrowField} by the caller) is
     * NOT affected — only values nested inside a struct/list lose this width.
     *
     * @param type the value's Trino type
     * @param boxedValue the boxed value, or {@code null}
     * @return the plain, recursively-converted value, or {@code null}
     */
    public static Object toPlainValue(Type type, Object boxedValue) {
        if (boxedValue == null) return null;
        return switch (type) {
            case VarcharType t -> ((Slice) boxedValue).toStringUtf8();
            case VarbinaryType t -> ((Slice) boxedValue).getBytes();
            case RealType t -> Float.intBitsToFloat(((Long) boxedValue).intValue());
            case DateType t -> ((Long) boxedValue).intValue();
            case RowType rowType -> {
                SqlRow sqlRow = (SqlRow) boxedValue;
                List<RowType.Field> fields = rowType.getFields();
                int rawIndex = sqlRow.getRawIndex();
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i).getType();
                    String fieldName = fields.get(i).getName().orElse("field" + i);
                    Object fieldValue = readBoxedValue(fieldType, sqlRow.getRawFieldBlock(i), rawIndex);
                    map.put(fieldName, toPlainValue(fieldType, fieldValue));
                }
                yield map;
            }
            case ArrayType arrayType -> {
                Block arrayBlock = (Block) boxedValue;
                Type elementType = arrayType.getElementType();
                List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
                for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                    list.add(toPlainValue(elementType, readBoxedValue(elementType, arrayBlock, i)));
                }
                yield list;
            }
            default -> boxedValue; // Boolean/Long/Double already match ScalarValue's expectations
        };
    }
}
