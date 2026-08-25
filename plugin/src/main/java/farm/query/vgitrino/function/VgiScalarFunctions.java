// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
 *   <li>{@code Struct}/{@code List}/{@code FixedSizeList} arguments/return
 *       ({@code geo_*}, {@code binary_packet}) — {@link VgiTypeMapping} covers
 *       core types only in the Trino -> Arrow direction.</li>
 *   <li>Varargs ({@code sum_values}, {@code concat_values}) — a real, separate
 *       marshaling design, skipped at discovery.</li>
 *   <li>Settings/secrets/auth-context arguments — skipped at discovery.</li>
 *   <li>A dynamic (bind-time-computed) return type — VGI's {@code on_bind}
 *       can compute a NEW output type from the argument's actual type (e.g.
 *       {@code double}'s int8 -> int64 promotion); Trino resolves a function's
 *       return type from its static {@link Signature} alone, before any RPC
 *       happens, so this has no Trino representation and is skipped at
 *       discovery (detected via the {@code vgi:any} output-field metadata
 *       {@code ScalarFunction.catalog_output_schema} emits for it — note the
 *       colon, a different key than the argument-side {@code vgi_type=any}).
 *       An {@code any}-typed ARGUMENT with a static, concrete return type
 *       (e.g. {@code any_mixed}) is fully supported via {@code
 *       Signature.typeVariable} — each such argument gets its own independent
 *       type variable; this class does not attempt to unify one with the
 *       return type, since the one fixture needing that ({@code double}) is
 *       exactly the dynamic-return case above and is already excluded.</li>
 * </ul>
 */
public final class VgiScalarFunctions {

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
     */
    record ScalarArg(String name, boolean positional, boolean constArg, boolean anyType,
            Type concreteType, String typeVarName, boolean varargs) {}

    /** One registered overload: static discovery-time info plus the Trino {@link FunctionMetadata}. */
    record Entry(FunctionId functionId, String schemaName, String functionName,
            FunctionMetadata metadata, List<ScalarArg> args, Type declaredReturnType) {}

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

        private Registry(Map<FunctionId, Entry> byId, Map<String, List<Entry>> byName) {
            this.byId = byId;
            this.byName = byName;
        }

        /** @return every overload registered under {@code (schemaName, functionName)}, or empty */
        public Collection<FunctionMetadata> functionsFor(String schemaName, String functionName) {
            List<Entry> entries = byName.get(nameKey(schemaName, functionName));
            if (entries == null) return List.of();
            return entries.stream().map(Entry::metadata).toList();
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
                    if (entry == null) continue; // unsupported shape — see class javadoc
                    List<Entry> overloads = byName.computeIfAbsent(nameKey(schemaName, info.name()), k -> new ArrayList<>());
                    // VGI's own Arrow type system can distinguish two overloads (e.g. int64 vs.
                    // uint32/uint64, all of which VgiTypeMapping widens to BIGINT — VarcharType has
                    // no unsigned counterpart) that collapse onto the IDENTICAL Trino Signature.
                    // Registering both would make every call ambiguous ("Could not choose a best
                    // candidate operator") — confirmed against the real fixture's 5-way `type_info`
                    // overload set, three of whose signatures collide this way. Keep the
                    // first-discovered overload per distinct Signature; a colliding later one is
                    // simply unreachable from Trino, not silently wrong.
                    boolean collides = overloads.stream()
                            .anyMatch(existing -> existing.metadata().getSignature().equals(entry.metadata().getSignature()));
                    if (collides) continue;
                    overloads.add(entry);
                    byId.put(entry.functionId(), entry);
                }
            }
            return new Registry(byId, byName);
        });
    }

    private static Entry tryBuildEntry(String schemaName, FunctionInfo info, Map<String, Integer> overloadCounters) {
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<ScalarArg> args = new ArrayList<>();
        int anyIndex = 0;
        for (Field field : argsSchema == null ? List.<Field>of() : argsSchema.getFields()) {
            ScalarArg arg = decodeScalarArg(field, anyIndex);
            if (arg == null) return null; // table-typed / const-vararg / unsupported concrete type
            if (arg.anyType()) anyIndex++;
            args.add(arg);
        }
        // A vararg argument must be the LAST one — Signature.variableArity() means "the last
        // declared argument type repeats," which has no representation for a vararg group
        // followed by more fixed arguments.
        for (int i = 0; i < args.size() - 1; i++) {
            if (args.get(i).varargs()) return null;
        }

        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) return null;
        Field outField = outSchema.getFields().get(0);
        Map<String, String> outMeta = outField.getMetadata();
        boolean dynamicReturn = (outMeta != null && "true".equals(outMeta.get("vgi:any")))
                || outField.getType().getTypeID() == ArrowType.ArrowTypeID.Null;
        if (dynamicReturn) return null; // bind-time-computed return type — see class javadoc
        Type returnType;
        try {
            returnType = VgiTypeMapping.toTrinoType(outField);
        } catch (UnsupportedOperationException e) {
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

        FunctionMetadata.Builder metadataBuilder = FunctionMetadata.scalarBuilder(info.name())
                .signature(signature)
                .functionId(functionId)
                .argumentNullability(Collections.nCopies(args.size(), true))
                .nullable()
                .description(info.description() == null ? "" : info.description());
        // CONSISTENT is VGI's default; anything else (VOLATILE, CONSISTENT_WITHIN_QUERY) means
        // Trino must not constant-fold or otherwise assume repeated calls agree.
        if (info.stability() != null && !"CONSISTENT".equals(info.stability())) {
            metadataBuilder.nondeterministic();
        }
        FunctionMetadata metadata = metadataBuilder.build();

        return new Entry(functionId, schemaName, info.name(), metadata, args, returnType);
    }

    private static ScalarArg decodeScalarArg(Field field, int anyIndex) {
        Map<String, String> metadata = field.getMetadata();
        String vgiType = metadata == null ? null : metadata.get("vgi_type");
        if ("table".equals(vgiType)) return null; // not a scalar argument at all
        boolean positional = metadata == null || !"named".equals(metadata.get("vgi_arg"));
        boolean constArg = metadata != null && "true".equals(metadata.get("vgi_const"));
        boolean varargs = metadata != null && "true".equals(metadata.get("vgi_varargs"));
        // A constant vararg has no real fixture and no clear wire meaning (ArgumentsEncoder's
        // bind-time constants and a varargs row-column group are two different channels) —
        // skip rather than guess.
        if (varargs && constArg) return null;
        if ("any".equals(vgiType)) {
            return new ScalarArg(field.getName(), positional, constArg, true, null, "T" + anyIndex, varargs);
        }
        Type type;
        try {
            type = VgiTypeMapping.toTrinoType(field);
        } catch (UnsupportedOperationException e) {
            return null; // unsupported concrete type — see class javadoc
        }
        return new ScalarArg(field.getName(), positional, constArg, false, type, null, varargs);
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
                rowFields.add(VgiTypeMapping.toArrowField(argumentTypes.get(i), spec.name()));
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
                    varargSpec.typeVarName(), true));
        }
        return effective;
    }

    // ------------------------------------------------------------------
    // Bind cache — the ONE thing worth caching across invocations
    // ------------------------------------------------------------------

    /** One cached {@code bind()} result: replayable at {@code init()} time on any connection. */
    record BindEntry(byte[] bindCallBytes, byte[] outputSchemaBytes, byte[] opaqueData) {}

    private record CacheKey(FunctionId functionId, List<Object> constArgValues) {}

    /**
     * Caches {@code bind()} results keyed by {@code (function, observed const-argument values)},
     * so a query whose "constant" argument really is constant across every row pays exactly one
     * {@code bind()} RPC no matter how many rows are processed — every {@code init()}+{@code
     * exchange()} after the first replays the same cached {@code bind_call} bytes on whatever
     * connection happens to be free.
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
     * <p>Bounded (simple LRU) so a workload with many distinct const-argument values can't grow
     * this without limit; a concurrent miss on the same key may bind twice — both computed binds
     * are equivalent, so this loses nothing but an occasional redundant RPC.
     */
    public static final class BindCache {
        private static final int MAX_ENTRIES = 256;

        private final Map<CacheKey, BindEntry> map = Collections.synchronizedMap(
                new LinkedHashMap<>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<CacheKey, BindEntry> eldest) {
                        return size() > MAX_ENTRIES;
                    }
                });

        BindEntry getOrBind(VgiWorkerClient client, CallConfig cfg, List<Object> constValues) {
            CacheKey key = new CacheKey(cfg.entry().functionId(), constValues);
            BindEntry cached = map.get(key);
            if (cached != null) return cached;
            BindEntry fresh = client.withConnection(a -> doBind(a, cfg, constValues));
            map.putIfAbsent(key, fresh);
            return map.get(key);
        }

        private static BindEntry doBind(VgiWorkerClient.Attached a, CallConfig cfg, List<Object> constValues) {
            byte[] argumentsBytes = encodeConstArgs(cfg, constValues);
            byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(cfg.rowInputSchema());
            BindRequest bindRequest = new BindRequest(
                    cfg.entry().functionName(),
                    argumentsBytes,
                    "SCALAR",
                    inputSchemaBytes,
                    null,           // settings — deferred, see class javadoc
                    null,           // secrets — deferred, see class javadoc
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    false,          // resolved_secrets_provided
                    null, null,     // at_unit / at_value — not applicable to scalars
                    null, null,     // copy_from / copy_to
                    cfg.entry().schemaName());
            BindResponse bound = a.service().bind(bindRequest, null);
            byte[] bindCallBytes = RecordCodec.serializeToBytes(bindRequest);
            return new BindEntry(bindCallBytes, bound.output_schema(), bound.opaque_data());
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

    static {
        try {
            NEW_INVOKER = LOOKUP.findStatic(VgiScalarFunctions.class, "newInvoker",
                    MethodType.methodType(Invoker.class, VgiWorkerClient.class, CallConfig.class, BindCache.class));
            INVOKE_GENERIC = LOOKUP.findStatic(VgiScalarFunctions.class, "invoke",
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

    /** {@code (Invoker, Object, Object, ..., Object) -> Object}, {@code arity} trailing boxed arguments. */
    public static MethodHandle methodHandle(int arity) {
        return INVOKE_GENERIC.asCollector(Object[].class, arity);
    }

    /** One invocation: split const vs. per-row args, resolve/reuse the bind, one exchange turn. */
    private static Object invoke(Invoker invoker, Object[] args) {
        CallConfig cfg = invoker.config();
        int[] constIdx = cfg.constArgIndices();
        List<Object> constValues = new ArrayList<>(constIdx.length);
        for (int i : constIdx) constValues.add(args[i]);
        BindEntry bind = invoker.bindCache().getOrBind(invoker.client(), cfg, constValues);

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
                ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
                AnnotatedBatch out = session.exchange(new AnnotatedBatch(input, null));
                // Every VGI scalar function's output schema has exactly one column, named "result".
                FieldVector resultVector = out.root().getVector("result");
                Object result = VgiTypeMapping.readValue(cfg.returnType(), resultVector, 0);
                session.close();
                return result;
            }
        });
    }
}
