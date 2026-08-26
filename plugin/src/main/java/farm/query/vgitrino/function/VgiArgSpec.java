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
 * @param tableArg whether this is a {@code vgi_type=table} argument (VGI's
 *        {@code TableInput} — the classic, non-blended table-in-out kind's
 *        streaming-table argument). {@link #type} is {@code null} whenever
 *        this is {@code true} — a table argument has no single Trino scalar
 *        type. A plain {@link VgiTableFunction}/blended {@code
 *        VgiTableInOutFunction} can't express this and must skip registering
 *        the whole function when any decoded argument comes back with this
 *        set; only {@code VgiTableInOutTableFunctions} uses it.
 */
public record VgiArgSpec(String name, Type type, boolean hasDefault, Object defaultValue, boolean positional,
        boolean constArg, boolean tableArg) {

    /**
     * Decode one argument field, or return {@code null} if this argument's
     * shape isn't supported at all (varargs or {@code any}-typed — neither
     * has a Trino representation anywhere in this connector yet). A {@code
     * vgi_type=table} argument DOES decode successfully now (with {@link
     * #tableArg} set and {@link #type} {@code null}) — unlike varargs/{@code
     * any}, it has a real Trino representation ({@code
     * TableArgumentSpecification}), just not one every caller of this method
     * can use; callers that can't (a plain table function, blended) must
     * check {@link #tableArg} themselves and skip registering the whole
     * function, rather than silently drop one argument from its signature.
     *
     * <h2>{@code vgi_varargs} is a confirmed Trino table-function SPI ceiling, not
     * a scope choice — traced end to end, not assumed</h2>
     *
     * <p>Unlike a scalar function ({@link VgiScalarFunctions}'s {@code
     * Signature.variableArity()}, a real {@code MethodHandle}-collector mechanism),
     * Trino's table-function argument model has no variadic concept whatsoever.
     * {@code io.trino.spi.function.table.ArgumentSpecification} (trino-spi 483)
     * carries only a name, a {@code required} flag, and an optional default —
     * {@code ScalarArgumentSpecification} adds exactly one concrete {@code Type},
     * never a repeatable one. {@code AbstractConnectorTableFunction} registers one
     * fixed {@code List<ArgumentSpecification>} for the function's entire lifetime
     * — there is no per-call-site re-declaration the way a scalar function's
     * {@code getScalarFunctionImplementation} gets one. And the engine code that
     * actually binds a {@code TABLE(...)} call's arguments against that declared
     * list — {@code StatementAnalyzer.analyzeArguments}, trino-main
     * {@code io/trino/sql/analyzer/StatementAnalyzer.java:1925-1987} — throws
     * {@code INVALID_ARGUMENTS} ("Too many arguments...") the instant
     * {@code arguments.size() > argumentSpecifications.size()} (line 1927), and a
     * positional call binds strictly by index (line 1974: {@code
     * argumentSpecifications.get(i)}), filling only the DECLARED, PRE-EXISTING
     * trailing slots that were left unspecified via {@code analyzeDefault} (lines
     * 1980-1983) — never a repeating group. There is no mechanism, at any layer,
     * for one declared argument to stand for "zero or more actual arguments."
     *
     * <p>The one real (if partial) workaround this SPI structurally allows — since
     * {@code ScalarArgumentSpecification.defaultValue()} makes a trailing slot
     * optional (line 1980's positional-default fill) — would be registering N
     * FIXED optional trailing slots, all sharing one declared {@code Type}, up to
     * some hardcoded cap; a caller supplying fewer than N gets the rest defaulted
     * to {@code null}, which {@link VgiTableFunction#analyze} already omits from
     * the wire encoding (a null-valued {@code ScalarArgument} is skipped before
     * {@code ArgumentsEncoder.positional}), so the bytes actually sent would match
     * what a true repeating argument would have produced for that arity — a
     * genuinely honest partial mechanism, in principle. It was not built, because
     * checking it against the real corpus first (per this connector's own
     * discipline — see the README's "Scope" section) showed it would fix nothing
     * real:
     *
     * <ul>
     *   <li>{@code constant_columns} — the corpus's actual varargs-not-registered
     *       failure (135 occurrences) — declares its trailing argument {@code
     *       Any}-typed (vgi-python {@code vgi/_test_fixtures/table/pairs.py}'s
     *       {@code ConstantColumnsFunctionArguments.values}: {@code
     *       Annotated<tuple<Any, ...>, Arg(1, varargs=True, ...)>}), and real call
     *       sites mix types WITHIN one call (e.g. {@code constant_columns(2, 100,
     *       'test', 3.14, 999)} — int64, string, double, int64 in one invocation).
     *       A fixed-cap workaround needs ONE declared {@code Type} per slot;
     *       Trino's table-function SPI has no analogue of {@code
     *       Signature.typeVariable()} at all (no type-variable concept anywhere in
     *       {@code ArgumentSpecification}), so there is no {@code Type} that could
     *       even be declared for this argument, cap or no cap.</li>
     *   <li>The corpus's only other two producer-mode varargs table functions are
     *       ALREADY unregisterable for reasons that have nothing to do with
     *       varargs: {@code repeat_value}'s int/string overloads ({@code
     *       RepeatValueIntArgs}/{@code RepeatValueStrArgs}, same {@code pairs.py})
     *       register under the identical name, tripping {@link
     *       VgiTableFunctions#discover}'s pre-existing overload-collision skip;
     *       {@code union_varargs}'s argument ({@code UnionVarargsArgs}, same file)
     *       is Arrow-Union-typed, a type {@link
     *       farm.query.vgitrino.types.VgiTypeMapping} does not map at all,
     *       independent of its varargs shape.</li>
     * </ul>
     *
     * <p>So a fixed-cap workaround would add real code and a real (if honest)
     * semantic compromise to fix zero real fixtures — every real vararg
     * table function in the corpus is blocked by a second, independent
     * ceiling even where this one is set aside. This is why {@code
     * vgi_varargs} is documented here as a confirmed SPI ceiling rather than
     * implemented around: not because no partial mechanism exists, but
     * because building one against no real, testable fixture would be
     * exactly the kind of "registered wrong" this class's own discipline
     * exists to avoid.
     *
     * @param field the argument's Arrow field, from the decoded
     *        {@code FunctionInfo.arguments} schema
     * @return the decoded spec, or {@code null} if unsupported
     */
    public static VgiArgSpec decode(Field field) {
        var metadata = field.getMetadata();
        boolean tableArg = false;
        if (metadata != null) {
            if ("true".equals(metadata.get("vgi_varargs"))) return null;
            String vgiType = metadata.get("vgi_type");
            if ("any".equals(vgiType)) return null;
            tableArg = "table".equals(vgiType);
        }
        boolean positional = metadata == null || !"named".equals(metadata.get("vgi_arg"));
        boolean constArg = metadata != null && "true".equals(metadata.get("vgi_const"));
        if (tableArg) {
            // No Trino Type, no default — TableArgumentSpecification is always required=true,
            // defaultValue=null (verified against the real SPI: its constructor hard-codes this).
            return new VgiArgSpec(field.getName(), null, false, null, positional, constArg, true);
        }
        Type type;
        try {
            type = VgiTypeMapping.toTrinoType(field);
        } catch (UnsupportedOperationException e) {
            return null;
        }
        String defaultJson = metadata == null ? null : metadata.get("vgi_default");
        if (defaultJson == null) {
            return new VgiArgSpec(field.getName(), type, false, null, positional, constArg, false);
        }
        return new VgiArgSpec(field.getName(), type, true, coerceDefault(defaultJson, type), positional, constArg, false);
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
