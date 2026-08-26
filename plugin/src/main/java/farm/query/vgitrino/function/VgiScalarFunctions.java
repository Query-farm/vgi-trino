// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.type.TypeTemplates.typeVariable;

/**
 * Real (not hardcoded) VGI scalar-function discovery and dispatch — supersedes the
 * proof-of-concept {@code VgiScalarFunctionSpike}.
 *
 * <h2>Connection lifecycle</h2>
 *
 * <p>The spike held one connection open for a {@code MethodHandle} instance's
 * entire lifetime via Trino's {@code instanceFactory} hook — a genuine bug, not
 * a shortcut: {@code instanceFactory} runs once per {@code Driver} (not once per
 * query), or bounding connections is impossible, AND Trino never calls any
 * lifecycle/close hook on the instance it produces, so every borrowed
 * connection leaked until GC eventually collected the whole operator graph.
 *
 * <p>This class never holds a connection longer than one invocation. Every
 * {@link #invoke} call borrows a connection via {@link VgiWorkerClient#withConnection},
 * does {@code init()} + one {@code exchange()} turn + {@code session.close()},
 * and returns it — exactly mirroring the existing, already-tested table-scan
 * pattern ({@code VgiSplitManager} binds once, {@code VgiPageSource} redeems on
 * a separately-borrowed connection per split). The one thing never assumed:
 * whether a fresh {@code init()} needs a fresh {@code bind()} every time.
 * {@link BindCache} answers that — see its own javadoc.
 *
 * <p>{@code instanceFactory} still produces one object per {@code Driver} (an
 * {@link Invoker}), but it holds no connection and no open stream — just
 * immutable per-call-site config plus references to the shared {@link
 * VgiWorkerClient}/{@link BindCache} — so Trino never calling a cleanup hook on
 * it is harmless.
 *
 * <h2>Deferred, named explicitly (see the README's "Scalar functions" section)</h2>
 *
 * <ul>
 *   <li>{@code Struct}/{@code List}/{@code FixedSizeList} arguments and return values ARE
 *       supported ({@code geo_*}, {@code binary_packet}), recursively, in both directions —
 *       {@link VgiTypeMapping#toArrowField(Type, String, Field)}'s {@code hint} parameter is what
 *       lets a {@code FixedSizeList} argument's exact width survive round-tripping through
 *       Trino's width-erasing {@code ArrayType}. Varargs ARE supported too ({@code sum_values},
 *       {@code concat_values} are excluded only because their return type is separately dynamic —
 *       see below — not because of their varargs shape; {@code geo_centroid_struct} combines
 *       varargs with struct arguments and a struct return).</li>
 *   <li>Settings ({@code required_settings}) map onto Trino session properties (declared via
 *       {@code VgiConnector.getSessionProperties()}), and secrets ({@code required_secrets}) onto
 *       {@code ConnectorIdentity.getExtraCredentials()} (a {@code vgi_secret.<key>.<field>=value}
 *       convention — see {@link BindCache}'s own javadoc for the one honest limitation this
 *       leaves) — both delivered via a {@code ConnectorSession} threaded through {@code
 *       supportsSession} (see {@link #methodHandle}/{@link #methodHandleNoSession}). Auth-context
 *       arguments need no support at all — confirmed invisible on the wire entirely (verified via
 *       {@code whoami}, callable exactly like any other one-argument function). One real, narrow
 *       gap remains: a settings/secrets-using function combined with a genuine per-row COLUMN
 *       argument hits a real Trino 483 columnar-bytecode-generation bug — see {@link
 *       #methodHandleNoSession}'s own javadoc for what was actually observed, and why every OTHER
 *       function is deliberately kept off the {@code supportsSession} path entirely.</li>
 *   <li>A dynamic (bind-time-computed) return type — VGI's {@code on_bind}
 *       can compute a NEW output type from the argument's actual type (e.g.
 *       {@code double}'s int8 -> int64 promotion); Trino resolves a function's
 *       return type from its static {@link Signature} alone, before any RPC
 *       happens, so this has no Trino representation and is skipped at
 *       discovery (detected via the {@code vgi:any} output-field metadata
 *       {@code ScalarFunction.catalog_output_schema} emits for it — note the
 *       colon, a different key than the argument-side {@code vgi_type=any}).
 *       This is a real ceiling of Trino's function-resolution model, not a
 *       scope choice.
 *       An {@code any}-typed ARGUMENT with a static, concrete return type
 *       (e.g. {@code any_mixed}) is fully supported via {@code
 *       Signature.typeVariable} — each such argument gets its own independent
 *       type variable; this class does not attempt to unify one with the
 *       return type, since the one fixture needing that ({@code double}) is
 *       exactly the dynamic-return case above and is already excluded.</li>
 *   <li>A colliding overload set (two VGI overloads whose Arrow argument types both map onto the
 *       identical Trino {@link Signature} — Trino has no unsigned integer type, so e.g. {@code
 *       int64} and {@code uint64} overloads of the same name collide on {@code BIGINT}) is
 *       pruned to one registration, preferring a lossless Arrow -> Trino mapping over a lossy one
 *       — see {@link #discover}'s own comment. Also a ceiling, not a scope choice.</li>
 * </ul>
 *
 * <p>Every discovery-time skip and overload-collision decision logs a {@code WARN} — nothing here
 * silently drops a function or silently prefers one overload over another with no visible trace.</p>
 */
public final class VgiScalarFunctions {

    private static final Logger LOG = Logger.get(VgiScalarFunctions.class);

    private VgiScalarFunctions() {}

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    /**
     * One (possibly overloaded) VGI scalar function argument, resolved at discovery time.
     *
     * @param varargs whether this is the trailing {@code vgi_varargs} argument — always the LAST
     *        entry in a function's argument list (a non-trailing vararg means the whole function
     *        is skipped at discovery, see {@link #tryBuildEntry}); consumes every resolved
     *        argument position from here to the end of a specific call site's bound arity (see
     *        {@link #effectiveArgs})
     * @param arrowHint this argument's original discovery-time Arrow field, or {@code null} for
     *        an {@code any}-typed argument (whose concrete type is only known at a specific call
     *        site, via {@code BoundSignature}, with no discovery-time Arrow shape to hint from).
     *        Threaded through to {@link VgiTypeMapping#toArrowField(Type, String, Field)} so a
     *        {@code FixedSizeList} argument's exact width survives round-tripping through
     *        Trino's width-erasing {@code ArrayType} — see that method's own javadoc.
     */
    record ScalarArg(String name, boolean positional, boolean constArg, boolean anyType,
            Type concreteType, String typeVarName, boolean varargs, Field arrowHint) {}

    /**
     * One registered overload: static discovery-time info plus the Trino {@link FunctionMetadata}.
     *
     * @param losslessMapping whether every argument's and the return's Arrow -> Trino mapping is
     *        exact (no representable value is lost) — used only to break a same-Trino-{@code
     *        Signature} overload collision in favor of the more faithful registration; see
     *        {@link #discover}
     * @param requiredSettings session-settings names this function needs — {@code
     *        FunctionInfo.required_settings} verbatim. Unlike a const/row argument, a setting is
     *        never part of the Trino {@link Signature}: it's a compute()-only out-of-band
     *        parameter with no corresponding {@code arguments} schema field at all (confirmed
     *        against the real fixture's {@code multiply_by_setting(value)} — one Signature
     *        argument, {@code value}, with {@code multiplier} supplied entirely via {@code
     *        BindRequest.settings}). Mapped onto Trino session properties — see
     *        {@link VgiConnector#getSessionProperties()}.
     * @param requiredSecrets secrets this function needs — {@code FunctionInfo.required_secrets}
     *        verbatim, same out-of-band treatment as settings. Mapped onto Trino's {@code
     *        ConnectorIdentity.getExtraCredentials()} — see {@link BindCache}'s own note on the
     *        credential-key convention and its one real limitation.
     */
    record Entry(FunctionId functionId, String schemaName, String functionName,
            FunctionMetadata metadata, List<ScalarArg> args, Type declaredReturnType, boolean losslessMapping,
            List<String> requiredSettings, List<FunctionRequiredSecret> requiredSecrets) {}

    /**
     * Every VGI scalar function this connector can support, discovered once at
     * catalog-attach time (mirroring {@link VgiTableFunctions#discover}) and
     * held for the catalog's lifetime — {@link FunctionId}s it hands out stay
     * valid across every later {@code getFunctionMetadata}/
     * {@code getScalarFunctionImplementation} call, however many separate
     * {@code ConnectorMetadata}/{@code FunctionProvider} instances Trino
     * constructs per query.
     */
    public static final class Registry {
        private final Map<FunctionId, Entry> byId;
        private final Map<String, List<Entry>> byName;
        private final Set<String> requiredSettingNames;

        private Registry(Map<FunctionId, Entry> byId, Map<String, List<Entry>> byName) {
            this.byId = byId;
            this.byName = byName;
            Set<String> names = new HashSet<>();
            for (Entry entry : byId.values()) names.addAll(entry.requiredSettings());
            this.requiredSettingNames = Set.copyOf(names);
        }

        /** @return every overload registered under {@code (schemaName, functionName)}, or empty */
        public Collection<FunctionMetadata> functionsFor(String schemaName, String functionName) {
            List<Entry> entries = byName.get(nameKey(schemaName, functionName));
            if (entries == null) return List.of();
            return entries.stream().map(Entry::metadata).toList();
        }

        /**
         * @return the union of every registered function's {@code required_settings} names — what
         *         {@link VgiConnector#getSessionProperties()} declares as this catalog's Trino
         *         session properties
         */
        public Set<String> requiredSettingNames() {
            return requiredSettingNames;
        }

        /** @return the {@link FunctionMetadata} for a previously-handed-out {@link FunctionId}, or {@code null} */
        public FunctionMetadata metadataFor(FunctionId functionId) {
            Entry entry = byId.get(functionId);
            return entry == null ? null : entry.metadata();
        }

        /** @return the full discovery-time {@link Entry} for a {@link FunctionId}, or {@code null} */
        Entry entryFor(FunctionId functionId) {
            return byId.get(functionId);
        }
    }

    private static String nameKey(String schemaName, String functionName) {
        return schemaName.toLowerCase(Locale.ROOT) + "." + functionName.toLowerCase(Locale.ROOT);
    }

    /**
     * Discover every {@code SCALAR_FUNCTION} across every schema and build an
     * {@link Entry} (and Trino {@link FunctionMetadata}) for each overload this
     * connector can support — skipping, rather than registering wrong or
     * crashing catalog creation, any function whose shape is out of scope (see
     * the class javadoc's deferred-items list).
     *
     * @param client the pooled connection to attach and query
     * @return the discovered registry
     */
    public static Registry discover(VgiWorkerClient client) {
        return client.withConnection(a -> {
            List<String> schemas = new ArrayList<>();
            for (byte[] item : a.service().catalog_schemas(a.handle(), null).items()) {
                schemas.add(RecordCodec.deserializeFromBytes(item, SchemaInfo.class).name());
            }
            Map<FunctionId, Entry> byId = new LinkedHashMap<>();
            Map<String, List<Entry>> byName = new LinkedHashMap<>();
            Map<String, Integer> overloadCounters = new HashMap<>();
            for (String schemaName : schemas) {
                ItemsResponse functions = a.service().catalog_schema_contents_functions(
                        a.handle(), schemaName, "SCALAR_FUNCTION", null, null);
                for (byte[] item : functions.items()) {
                    FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
                    Entry entry = tryBuildEntry(schemaName, info, overloadCounters);
                    if (entry == null) continue; // unsupported shape — see class javadoc, and the WARN already logged
                    List<Entry> overloads = byName.computeIfAbsent(nameKey(schemaName, info.name()), k -> new ArrayList<>());
                    // VGI's own Arrow type system can distinguish two overloads (e.g. int64 vs.
                    // uint32/uint64, all of which VgiTypeMapping widens to BIGINT — VarcharType has
                    // no unsigned counterpart) that collapse onto the IDENTICAL Trino Signature.
                    // Registering both would make every call ambiguous ("Could not choose a best
                    // candidate operator") — confirmed against the real fixture's 5-way `type_info`
                    // overload set, three of whose signatures collide this way. Only one can ever be
                    // registered; prefer whichever mapping is LOSSLESS (an exact Arrow<->Trino value
                    // round trip) over one that could silently misrepresent a value (e.g. an unsigned
                    // 64-bit argument above Long.MAX_VALUE, widened onto signed BIGINT) — this is
                    // "Trino intelligence" about the collision, not an arbitrary discovery-order
                    // pick. Between two equally (non-)lossless candidates, the first discovered wins,
                    // for a stable, deterministic result.
                    Entry colliding = overloads.stream()
                            .filter(existing -> existing.metadata().getSignature().equals(entry.metadata().getSignature()))
                            .findFirst().orElse(null);
                    if (colliding != null) {
                        if (entry.losslessMapping() && !colliding.losslessMapping()) {
                            overloads.remove(colliding);
                            byId.remove(colliding.functionId());
                            overloads.add(entry);
                            byId.put(entry.functionId(), entry);
                            LOG.warn("VGI scalar function %s.%s: overload %s and %s collapse to the identical "
                                            + "Trino signature %s (Trino cannot distinguish them) — keeping %s, "
                                            + "the LOSSLESS Arrow -> Trino mapping, over %s, which can silently "
                                            + "misrepresent a value",
                                    schemaName, info.name(), colliding.functionId(), entry.functionId(),
                                    entry.metadata().getSignature(), entry.functionId(), colliding.functionId());
                        } else {
                            LOG.warn("VGI scalar function %s.%s: overload %s collapses to the identical Trino "
                                            + "signature %s as already-registered %s (Trino cannot distinguish "
                                            + "them) — skipping it; %s",
                                    schemaName, info.name(), entry.functionId(), entry.metadata().getSignature(),
                                    colliding.functionId(), colliding.losslessMapping()
                                            ? "the registered one is already a lossless mapping"
                                            : "both are lossy Arrow -> Trino mappings; keeping the one discovered first");
                        }
                        continue;
                    }
                    overloads.add(entry);
                    byId.put(entry.functionId(), entry);
                }
            }
            return new Registry(byId, byName);
        });
    }

    private static Entry tryBuildEntry(String schemaName, FunctionInfo info, Map<String, Integer> overloadCounters) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<ScalarArg> args = new ArrayList<>();
        int anyIndex = 0;
        for (Field field : argsSchema == null ? List.<Field>of() : argsSchema.getFields()) {
            ScalarArg arg = decodeScalarArg(context, field, anyIndex);
            if (arg == null) return null; // reason already logged by decodeScalarArg
            if (arg.anyType()) anyIndex++;
            args.add(arg);
        }
        // A vararg argument must be the LAST one — Signature.variableArity() means "the last
        // declared argument type repeats," which has no representation for a vararg group
        // followed by more fixed arguments.
        for (int i = 0; i < args.size() - 1; i++) {
            if (args.get(i).varargs()) {
                LOG.warn("VGI scalar function %s: skipping registration — argument '%s' is vgi_varargs but "
                                + "isn't the LAST argument; Trino's variable-arity signature only supports a "
                                + "repeating trailing argument", context, args.get(i).name());
                return null;
            }
        }

        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) {
            LOG.warn("VGI scalar function %s: skipping registration — expected exactly one output column, got %s",
                    context, outSchema == null ? "none" : outSchema.getFields().size());
            return null;
        }
        Field outField = outSchema.getFields().get(0);
        Map<String, String> outMeta = outField.getMetadata();
        boolean dynamicReturn = (outMeta != null && "true".equals(outMeta.get("vgi:any")))
                || outField.getType().getTypeID() == ArrowType.ArrowTypeID.Null;
        if (dynamicReturn) {
            LOG.warn("VGI scalar function %s: skipping registration — its return type is computed dynamically "
                            + "at bind time (on_bind), which has no Trino representation (a function's return "
                            + "type is resolved from its static Signature before any RPC happens)", context);
            return null;
        }
        Type returnType;
        try {
            returnType = VgiTypeMapping.toTrinoType(outField);
        } catch (UnsupportedOperationException e) {
            LOG.warn(e, "VGI scalar function %s: skipping registration — unsupported return type", context);
            return null;
        }

        Signature.Builder sig = Signature.builder();
        for (ScalarArg arg : args) {
            if (arg.anyType()) sig.typeVariable(arg.typeVarName());
        }
        for (ScalarArg arg : args) {
            if (arg.anyType()) sig.argumentType(typeVariable(arg.typeVarName()));
            else sig.argumentType(arg.concreteType());
        }
        if (!args.isEmpty() && args.get(args.size() - 1).varargs()) {
            sig.variableArity();
        }
        sig.returnType(returnType);
        Signature signature = sig.build();

        int overloadIndex = overloadCounters.merge(schemaName + ":" + info.name(), 1, Integer::sum) - 1;
        FunctionId functionId = new FunctionId("vgi:" + schemaName + ":" + info.name() + ":" + overloadIndex);
        List<String> requiredSettings = info.required_settings() == null ? List.of() : info.required_settings();
        List<FunctionRequiredSecret> requiredSecrets = info.required_secrets() == null ? List.of() : info.required_secrets();

        FunctionMetadata.Builder metadataBuilder = FunctionMetadata.scalarBuilder(info.name())
                .signature(signature)
                .functionId(functionId)
                .argumentNullability(Collections.nCopies(args.size(), true))
                .nullable()
                .description(info.description() == null ? "" : info.description());
        // CONSISTENT is VGI's default; anything else (VOLATILE, CONSISTENT_WITHIN_QUERY) means
        // Trino must not constant-fold or otherwise assume repeated calls agree. A function
        // reading settings/secrets ALSO must not be constant-folded, for a different but equally
        // real reason: its result depends on session/identity state entirely outside its
        // Signature arguments, and Trino's own IR constant-folding (EvaluateCall, gated on
        // exactly this deterministic flag) evaluates a call with all-literal arguments using a
        // session that isn't yet bound to this catalog — confirmed the hard way against the real
        // fixture's multiply_by_setting: without this, ConnectorSession.getProperty throws
        // "Session property 'null.multiplier' does not exist" at PLAN time, not execution time.
        if ((info.stability() != null && !"CONSISTENT".equals(info.stability()))
                || !requiredSettings.isEmpty() || !requiredSecrets.isEmpty()) {
            metadataBuilder.nondeterministic();
        }
        FunctionMetadata metadata = metadataBuilder.build();

        boolean losslessMapping = args.stream().allMatch(a -> isLosslessMapping(a.arrowHint()))
                && isLosslessMapping(outField);
        return new Entry(functionId, schemaName, info.name(), metadata, args, returnType, losslessMapping,
                requiredSettings, requiredSecrets);
    }

    /**
     * Whether {@code field}'s Arrow type maps to its resolved Trino type with no possible loss
     * of a representable value. The one known lossy case in {@link VgiTypeMapping#toTrinoType}:
     * an unsigned 64-bit integer widens to (signed) {@code BIGINT}, which cannot represent a
     * value above {@code Long.MAX_VALUE} without wrapping negative. Used only to prefer one
     * overload over another when both collapse to the same Trino {@link Signature} — see
     * {@link #discover}.
     *
     * @param field the argument/return's discovery-time Arrow field, or {@code null} for an
     *        {@code any}-typed argument (no concrete Arrow type to lose fidelity from)
     */
    private static boolean isLosslessMapping(Field field) {
        if (field == null) return true;
        if (field.getType() instanceof ArrowType.Int i) {
            return i.getBitWidth() != 64 || i.getIsSigned();
        }
        return true;
    }

    private static ScalarArg decodeScalarArg(String context, Field field, int anyIndex) {
        Map<String, String> metadata = field.getMetadata();
        String vgiType = metadata == null ? null : metadata.get("vgi_type");
        if ("table".equals(vgiType)) {
            LOG.warn("VGI scalar function %s: skipping registration — argument '%s' is TABLE-typed, "
                    + "not a scalar argument", context, field.getName());
            return null;
        }
        boolean positional = metadata == null || !"named".equals(metadata.get("vgi_arg"));
        boolean constArg = metadata != null && "true".equals(metadata.get("vgi_const"));
        boolean varargs = metadata != null && "true".equals(metadata.get("vgi_varargs"));
        // A constant vararg has no real fixture and no clear wire meaning (ArgumentsEncoder's
        // bind-time constants and a varargs row-column group are two different channels) —
        // skip rather than guess.
        if (varargs && constArg) {
            LOG.warn("VGI scalar function %s: skipping registration — argument '%s' is both vgi_const and "
                    + "vgi_varargs, a combination with no clear wire meaning", context, field.getName());
            return null;
        }
        if ("any".equals(vgiType)) {
            return new ScalarArg(field.getName(), positional, constArg, true, null, "T" + anyIndex, varargs, null);
        }
        Type type;
        try {
            type = VgiTypeMapping.toTrinoType(field);
        } catch (UnsupportedOperationException e) {
            LOG.warn(e, "VGI scalar function %s: skipping registration — unsupported type for argument '%s'",
                    context, field.getName());
            return null;
        }
        return new ScalarArg(field.getName(), positional, constArg, false, type, null, varargs, field);
    }

    // ------------------------------------------------------------------
    // Per-call-site config, resolved at getScalarFunctionImplementation time
    // (BoundSignature carries the concrete types for any type-variable args)
    // ------------------------------------------------------------------

    /**
     * Everything one call site's {@code invoke}/bind-cache lookups need,
     * resolved once per {@code getScalarFunctionImplementation} call (i.e. once
     * per {@code Driver}, matching Trino's own {@code instanceFactory} cadence)
     * from the static {@link Entry} plus the call site's resolved {@code
     * BoundSignature} types.
     *
     * @param entry the discovery-time entry (schema/function name, per-argument
     *        const/positional/name info)
     * @param argumentTypes the resolved concrete Trino type for every argument,
     *        in signature order (from {@code BoundSignature.getArgumentTypes()})
     * @param returnType the resolved concrete return type
     * @param rowInputSchema the Arrow schema for the per-row (non-const)
     *        arguments only, in the order they appear in {@link #rowArgIndices}
     * @param rowArgIndices signature-order indices of the non-const arguments
     * @param constArgIndices signature-order indices of the {@code vgi_const} arguments
     * @param effectiveArgs {@code entry.args()}, expanded to this call site's actual bound
     *        arity — identical to {@code entry.args()} for a non-variadic function; for a
     *        variable-arity one, the trailing vararg spec is repeated (each repeat given a
     *        distinct synthetic name, since Arrow field names must be unique — VGI's own
     *        dispatch reads a vararg group by BATCH COLUMN POSITION, not name, confirmed
     *        against {@code vgi/scalar_function.py}'s {@code _resolution_index} handling, so the
     *        exact repeated name is never significant beyond uniqueness)
     */
    record CallConfig(Entry entry, List<Type> argumentTypes, Type returnType, Schema rowInputSchema,
            int[] rowArgIndices, int[] constArgIndices, List<ScalarArg> effectiveArgs) {}

    /**
     * Resolve one call site's {@link CallConfig} from the static {@link Entry}
     * and this call's bound argument/return types.
     */
    public static CallConfig buildCallConfig(Entry entry, List<Type> argumentTypes, Type returnType) {
        List<ScalarArg> specs = effectiveArgs(entry, argumentTypes.size());
        List<Integer> rowIdx = new ArrayList<>();
        List<Integer> constIdx = new ArrayList<>();
        List<Field> rowFields = new ArrayList<>();
        for (int i = 0; i < specs.size(); i++) {
            ScalarArg spec = specs.get(i);
            if (spec.constArg()) {
                constIdx.add(i);
            } else {
                rowIdx.add(i);
                rowFields.add(VgiTypeMapping.toArrowField(argumentTypes.get(i), spec.name(), spec.arrowHint()));
            }
        }
        return new CallConfig(entry, argumentTypes, returnType, new Schema(rowFields),
                rowIdx.stream().mapToInt(Integer::intValue).toArray(),
                constIdx.stream().mapToInt(Integer::intValue).toArray(),
                specs);
    }

    /**
     * Expand {@code entry.args()} to exactly {@code callArity} entries — see {@link CallConfig}'s
     * {@code effectiveArgs} javadoc.
     */
    private static List<ScalarArg> effectiveArgs(Entry entry, int callArity) {
        List<ScalarArg> declared = entry.args();
        if (callArity == declared.size()) return declared;
        ScalarArg varargSpec = declared.get(declared.size() - 1);
        List<ScalarArg> effective = new ArrayList<>(callArity);
        effective.addAll(declared.subList(0, declared.size() - 1));
        int repeatCount = callArity - (declared.size() - 1);
        for (int i = 0; i < repeatCount; i++) {
            effective.add(new ScalarArg(varargSpec.name() + "_" + i, varargSpec.positional(),
                    varargSpec.constArg(), varargSpec.anyType(), varargSpec.concreteType(),
                    varargSpec.typeVarName(), true, varargSpec.arrowHint()));
        }
        return effective;
    }

    // ------------------------------------------------------------------
    // Bind cache — the ONE thing worth caching across invocations
    // ------------------------------------------------------------------

    /** One cached {@code bind()} result: replayable at {@code init()} time on any connection. */
    record BindEntry(byte[] bindCallBytes, byte[] outputSchemaBytes, byte[] opaqueData) {}

    private record CacheKey(FunctionId functionId, List<Object> constArgValues,
            Map<String, String> resolvedSettings, Map<String, String> resolvedSecretFields) {}

    /**
     * Caches {@code bind()} results keyed by {@code (function, observed const-argument values,
     * resolved settings, resolved secret fields)}, so a query whose "constant" argument really is
     * constant across every row pays exactly one {@code bind()} RPC no matter how many rows are
     * processed — every {@code init()}+{@code exchange()} after the first replays the same cached
     * {@code bind_call} bytes on whatever connection happens to be free.
     *
     * <p>Trino's {@code FunctionProvider} has no channel to receive a constant argument's actual
     * VALUE ahead of invocation ({@code BoundSignature} carries only types) — a {@code vgi_const}
     * argument arrives as just another value on the same row-at-a-time {@code MethodHandle} call,
     * indistinguishable at that point from a genuinely per-row-varying one. This cache is the
     * pragmatic equivalent of VGI's own bind-time-constant model: bind once per distinct value
     * actually observed, rebind the moment it changes. A query where the "constant" truly varies
     * per row degrades to "no caching benefit" (a fresh {@code bind()} every call) — never wrong
     * results, just no speedup; see the README's honest accounting of this.
     *
     * <p>Settings and secrets are session-scoped, not query-argument-scoped, but this cache is
     * shared across every session/query on the catalog — so their resolved values must ALSO be
     * part of the key, or two sessions with different {@code SET SESSION}/{@code
     * --extra-credential} values would incorrectly share one query's bind.
     *
     * <h2>The secrets credential-key convention, and its one real limitation</h2>
     *
     * <p>A required secret (VGI's {@code FunctionRequiredSecret(secret_type, scope, secret_name)})
     * is looked up in {@code ConnectorIdentity.getExtraCredentials()} — a flat, client-supplied
     * {@code Map<String, String>} (populated e.g. via the JDBC/CLI {@code --extra-credential}
     * flag) — by scanning for keys of the form {@code vgi_secret.<secretKey>.<fieldName>}, where
     * {@code secretKey} is {@code secret_name} if present, else {@code secret_type}. Every
     * matching key becomes one field of that secret's struct (confirmed against the real
     * fixture's {@code secret_field()}, whose {@code vgi_example} secret needs TWO fields, {@code
     * port} and {@code secret_string} — a flat single credential value could not represent this).
     *
     * <p>This sends whatever fields the CALLER happens to supply via {@code --extra-credential},
     * proactively, on the first {@code bind()} ({@code resolved_secrets_provided=true} whenever
     * any secret data was found) — it does NOT replicate VGI's own two-phase secret-resolution
     * dance ({@code BindResponse.lookup_secret_types}/{@code lookup_scopes}/{@code lookup_names}
     * triggering a second bind pass), since Trino has no per-query "ask the client for a
     * credential" round trip once a query is already running. A missing field is simply absent
     * from the sent struct — whatever the worker does with an incomplete secret (VGI's own
     * {@code dict.get}-style field access, in the reference fixture, silently substitutes an
     * empty value rather than failing) is what a caller sees, not a clean "missing credential"
     * error from this connector.
     *
     * <p>Bounded (simple LRU) so a workload with many distinct const-argument/settings/secret
     * combinations can't grow this without limit; a concurrent miss on the same key may bind
     * twice — both computed binds are equivalent, so this loses nothing but an occasional
     * redundant RPC.
     */
    public static final class BindCache {
        private static final int MAX_ENTRIES = 256;
        private static final String SECRET_CREDENTIAL_PREFIX = "vgi_secret.";

        private final Map<CacheKey, BindEntry> map = Collections.synchronizedMap(
                new LinkedHashMap<>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<CacheKey, BindEntry> eldest) {
                        return size() > MAX_ENTRIES;
                    }
                });

        BindEntry getOrBind(VgiWorkerClient client, CallConfig cfg, List<Object> constValues,
                ConnectorSession session) {
            Map<String, String> resolvedSettings = resolveSettings(cfg.entry().requiredSettings(), session);
            Map<String, String> resolvedSecretFields = resolveSecretFields(cfg.entry().requiredSecrets(), session);
            CacheKey key = new CacheKey(cfg.entry().functionId(), constValues, resolvedSettings, resolvedSecretFields);
            BindEntry cached = map.get(key);
            if (cached != null) return cached;
            BindEntry fresh = client.withConnection(
                    a -> doBind(a, cfg, constValues, resolvedSettings, resolvedSecretFields));
            map.putIfAbsent(key, fresh);
            return map.get(key);
        }

        private static BindEntry doBind(VgiWorkerClient.Attached a, CallConfig cfg, List<Object> constValues,
                Map<String, String> resolvedSettings, Map<String, String> resolvedSecretFields) {
            byte[] argumentsBytes = encodeConstArgs(cfg, constValues);
            byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(cfg.rowInputSchema());
            byte[] settingsBytes = resolvedSettings.isEmpty() ? null : SettingsEncoder.of(resolvedSettings);
            byte[] secretsBytes = encodeSecrets(resolvedSecretFields);
            BindRequest bindRequest = new BindRequest(
                    cfg.entry().functionName(),
                    argumentsBytes,
                    "SCALAR",
                    inputSchemaBytes,
                    settingsBytes,
                    secretsBytes,
                    a.handle(),           // attach_opaque_data
                    null,                 // transaction_opaque_data
                    secretsBytes != null, // resolved_secrets_provided — see class javadoc's caveat
                    null, null,           // at_unit / at_value — not applicable to scalars
                    null, null,           // copy_from / copy_to
                    cfg.entry().schemaName());
            BindResponse bound = a.service().bind(bindRequest, null);
            byte[] bindCallBytes = RecordCodec.serializeToBytes(bindRequest);
            return new BindEntry(bindCallBytes, bound.output_schema(), bound.opaque_data());
        }

        /**
         * {@code required_settings} names with a non-null current session-property value.
         *
         * <p>Package-private, not private: {@code VgiTableInOutTableFunction} reuses this verbatim
         * for a classic table-in-out function's own {@code required_settings} — the wire shape and
         * resolution rule are identical to a scalar function's (confirmed against the real
         * fixture's {@code filter_by_setting}), and {@code analyze()} already receives a real {@code
         * ConnectorSession} directly (unlike a scalar function's {@code FunctionProvider}, which
         * needed this whole {@code BindCache} to get one at all), so no bind-cache equivalent is
         * needed on that side — just this same resolution logic, called once per {@code analyze()}.
         */
        static Map<String, String> resolveSettings(List<String> requiredSettings, ConnectorSession session) {
            if (requiredSettings.isEmpty()) return Map.of();
            Map<String, String> resolved = new LinkedHashMap<>();
            for (String name : requiredSettings) {
                String value = session.getProperty(name, String.class);
                if (value != null) resolved.put(name, value);
            }
            return resolved;
        }

        /**
         * {@code required_secrets} fields found in {@code ConnectorIdentity.getExtraCredentials()}
         * — see the class javadoc for the {@code vgi_secret.<secretKey>.<fieldName>} convention.
         * Flattened as {@code "<secretKey>.<fieldName>" -> value} (rather than a nested map) so it
         * can sit directly in {@link CacheKey}, itself a plain record. Package-private for the same
         * reason as {@link #resolveSettings} — reused verbatim by {@code VgiTableInOutTableFunction}.
         *
         * <p>Gated on {@code requiredSecrets} being non-empty, same as {@link #resolveSettings} —
         * a function whose {@code on_bind} resolves a secret dynamically with no static {@code
         * Secret()}/{@code Meta.required_secrets} declaration (the real fixture's {@code
         * secret_in_out}) never has anything forwarded here, even if the caller's {@code
         * --extra-credential} supplied it — a deliberate, security-relevant gate (this connector
         * never guesses which credentials a function needs), not an oversight; see the README for
         * the honest accounting of which real fixtures this leaves out of reach.
         */
        static Map<String, String> resolveSecretFields(List<FunctionRequiredSecret> requiredSecrets,
                ConnectorSession session) {
            if (requiredSecrets.isEmpty()) return Map.of();
            Map<String, String> credentials = session.getIdentity().getExtraCredentials();
            Map<String, String> resolved = new LinkedHashMap<>();
            for (FunctionRequiredSecret required : requiredSecrets) {
                String secretKey = required.secret_name() != null ? required.secret_name() : required.secret_type();
                String prefix = SECRET_CREDENTIAL_PREFIX + secretKey + ".";
                for (Map.Entry<String, String> credential : credentials.entrySet()) {
                    if (credential.getKey().startsWith(prefix)) {
                        resolved.put(secretKey + "." + credential.getKey().substring(prefix.length()),
                                credential.getValue());
                    }
                }
            }
            return resolved;
        }

        /**
         * Un-flattens {@link #resolveSecretFields}'s map back into one struct-valued setting per
         * secret. Package-private for the same reason as {@link #resolveSettings}.
         */
        static byte[] encodeSecrets(Map<String, String> resolvedSecretFields) {
            if (resolvedSecretFields.isEmpty()) return null;
            Map<String, Map<String, Object>> bySecret = new LinkedHashMap<>();
            for (Map.Entry<String, String> field : resolvedSecretFields.entrySet()) {
                int dot = field.getKey().indexOf('.');
                String secretKey = field.getKey().substring(0, dot);
                String fieldName = field.getKey().substring(dot + 1);
                bySecret.computeIfAbsent(secretKey, k -> new LinkedHashMap<>()).put(fieldName, field.getValue());
            }
            // Same wire shape as BindRequest.settings — a one-row batch, one Struct-typed column
            // per secret name — which is exactly what SettingsEncoder already builds given a
            // Map-valued setting; no separate SecretsEncoder exists (or is needed).
            return SettingsEncoder.of(bySecret);
        }

        private static byte[] encodeConstArgs(CallConfig cfg, List<Object> constValues) {
            // Always encode a (possibly zero-child) args batch, never a bare `null` — a
            // multi-overload function's worker-side dispatch inspects this batch to help
            // resolve which overload was meant, even when none of ITS OWN differentiating
            // arguments happen to be `vgi_const` (confirmed against the real fixture worker:
            // a bare `null` here breaks bind() for `type_info`/`any_mixed` with an
            // AttributeError, while a single-overload function like `passthru`/`multiply`
            // never touches this path and tolerated `null` — but there is no reason to send
            // a shape the reference implementation doesn't expect, even where it happens not
            // to matter yet).
            ArgumentsEncoder encoder = ArgumentsEncoder.builder();
            int[] constIdx = cfg.constArgIndices();
            for (int i = 0; i < constIdx.length; i++) {
                int sigIndex = constIdx[i];
                ScalarArg spec = cfg.effectiveArgs().get(sigIndex);
                Type type = cfg.argumentTypes().get(sigIndex);
                ScalarValue value = toScalarValue(type, constValues.get(i));
                if (spec.positional()) encoder.positional(value);
                else encoder.named(spec.name(), value);
            }
            return encoder.encode();
        }

        /**
         * Build the {@code ArgumentsEncoder}-ready constant, delegating the value's actual shape
         * to {@link VgiTypeMapping#toPlainValue} — see its own javadoc for the one honest caveat
         * (a nested struct/list field's exact width isn't always preserved, since {@code
         * ScalarValue}'s own type inference only distinguishes what Java's boxing naturally does).
         */
        private static ScalarValue toScalarValue(Type trinoType, Object boxedValue) {
            ArrowType arrowType = VgiTypeMapping.toArrowField(trinoType, "value").getType();
            if (boxedValue == null) return ScalarValue.ofNull(arrowType);
            return ScalarValue.of(arrowType, VgiTypeMapping.toPlainValue(trinoType, boxedValue));
        }
    }

    // ------------------------------------------------------------------
    // Dispatch: instanceFactory produces a cheap, resource-free Invoker;
    // the generic methodHandle does one borrow-init-exchange-close per call.
    // ------------------------------------------------------------------

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    record Invoker(VgiWorkerClient client, CallConfig config, BindCache bindCache) {}

    private static final MethodHandle NEW_INVOKER;
    private static final MethodHandle INVOKE_GENERIC;
    private static final MethodHandle INVOKE_NO_SESSION;

    static {
        try {
            NEW_INVOKER = LOOKUP.findStatic(VgiScalarFunctions.class, "newInvoker",
                    MethodType.methodType(Invoker.class, VgiWorkerClient.class, CallConfig.class, BindCache.class));
            INVOKE_GENERIC = LOOKUP.findStatic(VgiScalarFunctions.class, "invoke",
                    MethodType.methodType(Object.class, Invoker.class, ConnectorSession.class, Object[].class));
            INVOKE_NO_SESSION = LOOKUP.findStatic(VgiScalarFunctions.class, "invokeNoSession",
                    MethodType.methodType(Object.class, Invoker.class, Object[].class));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Invoker newInvoker(VgiWorkerClient client, CallConfig config, BindCache bindCache) {
        return new Invoker(client, config, bindCache);
    }

    /** {@code () -> Invoker}, bound to this call site's client/config/bind-cache via {@code insertArguments}. */
    public static MethodHandle instanceFactory(VgiWorkerClient client, CallConfig config, BindCache bindCache) {
        return MethodHandles.insertArguments(NEW_INVOKER, 0, client, config, bindCache);
    }

    /**
     * {@code (Invoker, ConnectorSession, Object, Object, ..., Object) -> Object}, {@code arity}
     * trailing boxed arguments. The {@code ConnectorSession} parameter is what lets {@link #invoke}
     * read session properties ({@code required_settings}) and extra credentials ({@code
     * required_secrets}) — Trino supplies the query's real session here itself, once {@link
     * VgiFunctionProvider} declares {@code supportsSession} in the invocation convention it hands
     * {@code ScalarFunctionAdapter.adapt}; this class never constructs one.
     *
     * <p>Use only for a function that actually declares {@code required_settings}/{@code
     * required_secrets} — see {@link #methodHandleNoSession} for why every other function uses a
     * plain, session-free handle instead of this one unconditionally.
     */
    public static MethodHandle methodHandle(int arity) {
        return INVOKE_GENERIC.asCollector(Object[].class, arity);
    }

    /**
     * {@code (Invoker, Object, Object, ..., Object) -> Object} — the session-free counterpart of
     * {@link #methodHandle}, for the vast majority of functions that read no settings/secrets.
     *
     * <p>Not just a minor optimization: Trino 483's columnar {@code PageProjectionWork} bytecode
     * generator has a real bug in the combination of {@code supportsSession=true} with an adapted
     * argument convention (confirmed by reading the actual generated bytecode against the real
     * fixture's {@code multiply_by_setting}/{@code scale_by_setting} — a missing unbox before an
     * {@code LSTORE}, and separately a raw {@code Block}/{@code int} descriptor mismatch,
     * depending on the requested convention) — it does NOT reproduce for a session-declaring
     * function with zero regular arguments ({@code secret_field()}, verified working end to end).
     * Declaring {@code supportsSession} only for the functions that truly need it keeps every
     * other function on the already-proven, session-free path, unaffected by this gap — see the
     * README's own note on the one real limitation this leaves.
     */
    public static MethodHandle methodHandleNoSession(int arity) {
        return INVOKE_NO_SESSION.asCollector(Object[].class, arity);
    }

    /** {@link #invoke} for a function with no {@code required_settings}/{@code required_secrets} at all. */
    private static Object invokeNoSession(Invoker invoker, Object[] args) {
        return invoke(invoker, null, args);
    }

    /** One invocation: split const vs. per-row args, resolve/reuse the bind, one exchange turn. */
    private static Object invoke(Invoker invoker, ConnectorSession session, Object[] args) {
        CallConfig cfg = invoker.config();
        int[] constIdx = cfg.constArgIndices();
        List<Object> constValues = new ArrayList<>(constIdx.length);
        for (int i : constIdx) constValues.add(args[i]);
        BindEntry bind = invoker.bindCache().getOrBind(invoker.client(), cfg, constValues, session);

        return invoker.client().withConnection(a -> {
            try (VectorSchemaRoot input = VectorSchemaRoot.create(cfg.rowInputSchema(), Allocators.root())) {
                input.allocateNew();
                int[] rowIdx = cfg.rowArgIndices();
                for (int rowPos = 0; rowPos < rowIdx.length; rowPos++) {
                    int sigIndex = rowIdx[rowPos];
                    FieldVector vector = input.getVector(rowPos);
                    VgiTypeMapping.writeValue(cfg.argumentTypes().get(sigIndex), vector, 0, args[sigIndex]);
                }
                for (FieldVector vector : input.getFieldVectors()) vector.setValueCount(1);
                input.setRowCount(1);

                InitRequest initRequest = new InitRequest(
                        bind.bindCallBytes(), bind.outputSchemaBytes(), bind.opaqueData(),
                        null, null, null, null, null, null,
                        null, null, null, null,
                        null, null,
                        null, null, null, null);
                RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
                ClientStreamSession<?> exchangeSession = (ClientStreamSession<?>) stream;
                AnnotatedBatch out = exchangeSession.exchange(new AnnotatedBatch(input, null));
                // Every VGI scalar function's output schema has exactly one column, named "result".
                FieldVector resultVector = out.root().getVector("result");
                Object result = VgiTypeMapping.readValue(cfg.returnType(), resultVector, 0);
                exchangeSession.close();
                return result;
            }
        });
    }
}
