// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgitrino.types.VgiTypeMapping;
import io.airlift.slice.Slices;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * One VGI table-function argument, decoded from a {@code FunctionInfo.arguments}
 * schema field into what a Trino {@code ScalarArgumentSpecification} needs.
 *
 * <p>The wire encoding is {@code farm.query.vgi.internal.ArgumentSpecSerializer}
 * — one Arrow field per argument, metadata carrying {@code vgi_arg=named}
 * (absent = positional), {@code vgi_varargs=true}, {@code vgi_type=any|table},
 * and {@code vgi_default} (a Gson JSON encoding of the default literal). This
 * class mirrors that decode side (there's no existing one to reuse — see
 * {@link farm.query.vgitrino.types.ArrowSchemaCodec}'s own javadoc for why this
 * connector doesn't depend on {@code farm.query.vgi.internal} directly).
 *
 * @param name the argument name
 * @param type the Trino type
 * @param hasDefault whether {@link #defaultValue} applies
 * @param defaultValue the default value (already coerced to {@code type}'s
 *        carrier representation), or {@code null} when {@link #hasDefault} is false
 * @param positional whether this argument must be sent as a positional slot
 *        on the wire ({@code ArgumentsEncoder.positional(...)}) rather than a
 *        named one, regardless of how the Trino caller wrote it — VGI's own
 *        {@code ArgumentsParser} dispatches by wire shape, not by whatever
 *        name a caller's {@code arg => value} syntax happened to use
 * @param constArg whether this is a {@code vgi_const} argument — a scalar
 *        function's bind-time constant, sent via {@code BindRequest.arguments}
 *        rather than as a per-row {@code input_schema} column. Unused by table
 *        functions (every {@code ScalarArgumentSpecification} is already
 *        bind-time-only there); read by {@code VgiScalarFunctions} to split an
 *        invocation's arguments into the bind-cache key vs. the per-row batch.
 */
public record VgiArgSpec(String name, Type type, boolean hasDefault, Object defaultValue, boolean positional,
        boolean constArg) {

    /**
     * Decode one argument field, or return {@code null} if this argument's
     * shape isn't supported yet (varargs, {@code any}-typed, or a TABLE input
     * — none of which a plain {@code ScalarArgumentSpecification} can express).
     * The caller should skip registering the whole function when any argument
     * comes back {@code null}, rather than silently drop one argument from its
     * signature.
     *
     * @param field the argument's Arrow field, from the decoded
     *        {@code FunctionInfo.arguments} schema
     * @return the decoded spec, or {@code null} if unsupported
     */
    public static VgiArgSpec decode(Field field) {
        var metadata = field.getMetadata();
        if (metadata != null) {
            if ("true".equals(metadata.get("vgi_varargs"))) return null;
            String vgiType = metadata.get("vgi_type");
            if ("any".equals(vgiType) || "table".equals(vgiType)) return null;
        }
        Type type;
        try {
            type = VgiTypeMapping.toTrinoType(field);
        } catch (UnsupportedOperationException e) {
            return null;
        }
        boolean positional = metadata == null || !"named".equals(metadata.get("vgi_arg"));
        boolean constArg = metadata != null && "true".equals(metadata.get("vgi_const"));
        String defaultJson = metadata == null ? null : metadata.get("vgi_default");
        if (defaultJson == null) {
            return new VgiArgSpec(field.getName(), type, false, null, positional, constArg);
        }
        return new VgiArgSpec(field.getName(), type, true, coerceDefault(defaultJson, type), positional, constArg);
    }

    /**
     * Coerce a {@code vgi_default} JSON-literal string into {@code type}'s
     * carrier representation. Handwritten rather than pulling in a JSON
     * library for this one call site: {@code vgi_default} is always a bare
     * scalar literal (number / quoted string / {@code true}/{@code false} /
     * {@code null}), never an object or array.
     */
    private static Object coerceDefault(String json, Type type) {
        String trimmed = json.strip();
        if (trimmed.equals("null")) return null;
        // ScalarArgumentSpecification validates the default against the
        // TYPE'S OWN native/internal Java representation (Type.getJavaType()),
        // not a convenient wrapper — VARCHAR wants a Slice, not a String; REAL
        // wants the raw int bits as a long, not a double (same reasoning as
        // VgiTypeMapping's Float4Vector case).
        if (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType) {
            return Long.parseLong(trimmed);
        }
        if (type instanceof RealType) {
            return (long) Float.floatToRawIntBits(Float.parseFloat(trimmed));
        }
        if (type instanceof DoubleType) {
            return Double.parseDouble(trimmed);
        }
        if (trimmed.equals("true") || trimmed.equals("false")) {
            return Boolean.parseBoolean(trimmed);
        }
        // A quoted JSON string: strip the surrounding quotes. vgi_default
        // never encodes escape sequences a caller would pass into these
        // simple scalar arguments, so no unescaping is needed.
        String unquoted = trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")
                ? trimmed.substring(1, trimmed.length() - 1) : trimmed;
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(unquoted);
        }
        return unquoted;
    }
}
