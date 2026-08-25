// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowBatchCodec;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import farm.query.vgitrino.types.VgiTypeMapping;
import io.airlift.log.Logger;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
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
 * Real VGI aggregate-function discovery and dispatch.
 *
 * <h2>Non-decomposable, single-execution design — a protocol fact, not a shortcut</h2>
 *
 * <p>VGI's aggregate wire protocol ({@code aggregate_bind}/{@code update}/{@code combine}/{@code
 * finalize}/{@code destructor}, all plain unary RPCs — a genuinely different design from scalar
 * functions' exchange-mode streaming) stores per-group state SERVER-SIDE, addressed only by
 * {@code (execution_id, group_id)} — the client never sees the state's bytes at all. {@code
 * aggregate_combine}'s wire shape confirms this precisely: its {@code merge_batch} carries only
 * {@code source_group_id}/{@code target_group_id} COLUMNS, no state payload — it merges two
 * groups' state WITHIN one already-bound {@code execution_id}, on one worker. There is no RPC that
 * produces a portable, execution-independent "intermediate state" blob a DIFFERENT accumulator (on
 * a different node, with a different {@code execution_id}) could deserialize and merge — which is
 * exactly what Trino's distributed partial-aggregation model needs for a decomposable aggregate
 * (ship each node's partial state, {@link AccumulatorStateSerializer#serialize} it, merge on
 * another node via {@code combine}).
 *
 * <p>So this connector declares every VGI aggregate NON-decomposable: no {@code combineFunction},
 * no intermediate type ({@link AggregationFunctionMetadata#isDecomposable} is defined as "has an
 * intermediate type" — declaring none disables it outright). Trino then runs the whole aggregation
 * as a single stage feeding one accumulator, which maps directly onto one {@code aggregate_bind}
 * call minting one {@code execution_id} for that accumulator's entire lifetime — sidestepping the
 * cross-node-combine question entirely, correctly, rather than guessing at a wire mechanism that
 * doesn't exist. The trade is real (no partial-aggregation parallelism across nodes) and is
 * documented in the README, not hidden.
 *
 * <h2>Row-at-a-time input(), batched RPCs — not a contradiction</h2>
 *
 * <p>Trino's {@code AccumulatorCompiler} calls the {@code input} {@link MethodHandle} once PER ROW
 * — there is no vectorized/batched input convention at the implementation level. Naively RPC-ing
 * on every call would be one round trip per input row, which is fine for a scalar function (called
 * once per OUTPUT row) but would be a genuine regression for an aggregate (called once per INPUT
 * row feeding a far smaller number of groups). {@link AggregateState} instead buffers each row
 * in-process (into a local {@link VectorSchemaRoot} tagged with a {@code __vgi_group_id} column —
 * VGI's own {@code aggregate_update} already expects exactly this batched, group-tagged shape) and
 * only calls {@code aggregate_update} when the buffer crosses a size threshold, or when {@code
 * output} needs to flush whatever's pending before finalizing. This is a real fit for VGI's actual
 * wire design, not a workaround bolted on afterward.
 *
 * <h2>Const arguments: bound lazily, from the first observed row — not at state-creation time</h2>
 *
 * <p>{@code vgi_const} arguments (bind-time constants, e.g. {@code vgi_percentile}'s percentile)
 * ARE supported, but need a different trick than scalar functions use. VGI's own {@code
 * AggregateBindRequest.arguments} field is exactly the const-value channel — the same shape as
 * scalar functions' {@code BindRequest.arguments} — but {@code aggregate_bind} has to happen
 * before any row can be processed, and {@link AccumulatorStateFactory#createSingleState()}/{@code
 * createGroupedState()} take NO arguments at all, so there's no way to know the actual constant
 * VALUE at state-creation time. Trino doesn't distinguish a "const" argument at the SPI level
 * either — it arrives as an ordinary per-row {@code (ValueBlock, position)} pair on every {@code
 * input()} call, indistinguishable at that point from a genuinely per-row-varying one (the same
 * fact {@link VgiScalarFunctions.BindCache}'s own javadoc explains for scalars). {@link
 * AggregateState} instead defers {@code aggregate_bind} until the FIRST row actually arrives,
 * reads the const argument's value off that row, and binds once for the accumulator's entire
 * lifetime — matching VGI's own bind-once-per-execution semantics exactly, since a "constant" is
 * genuinely constant across a whole accumulation by definition. A function with no const arguments
 * still binds eagerly at construction (unaffected, no lazy-binding overhead). One honest edge case:
 * if an accumulator NEVER receives a single row for any of its groups (e.g. aggregating an empty
 * table), the constant's value is never observed and {@code aggregate_bind} never happens at all —
 * {@link #output} returns {@code NULL} directly in that case rather than binding with a fabricated
 * value, which is the correct empty-aggregate answer for every real fixture this connector has
 * seen, though not a general proof it's right for every conceivable aggregate.
 *
 * <h2>Varargs</h2>
 *
 * <p>Supported via {@link Signature#variableArity()} — {@link #effectiveArgs} expands the
 * declared, discovery-time argument list to a specific call site's actual bound arity, exactly
 * mirroring {@link VgiScalarFunctions}'s own {@code effectiveArgs} for the identical reason
 * (Arrow field names must stay unique; VGI dispatches a vararg group by column position, not
 * name). A non-trailing vararg argument is skipped at discovery, same validity rule as scalars.
 *
 * <h2>Deferred, named explicitly (v1 scope)</h2>
 *
 * <ul>
 *   <li>{@code any}-typed arguments ARE supported, via {@link Signature#typeVariable} exactly like
 *       {@link VgiScalarFunctions} — needed for real fixtures like {@code vgi_sum_all}, whose
 *       vararg argument is itself any-typed. A dynamic (bind-time-computed) return type is skipped
 *       for the identical reason it's skipped for scalars: Trino resolves a function's return type
 *       from its static {@link Signature} before any RPC happens.</li>
 *   <li>Windowed usage ({@code supports_window}/{@code streaming_partitioned}) — {@code OVER} still
 *       works via Trino's own automatic window-accumulator fallback (full recompute per frame, not
 *       the optimized incremental {@code WindowAccumulator} path — see {@code
 *       AggregationImplementation.Builder#windowAccumulator}), so this is a performance ceiling, not
 *       a correctness gap.</li>
 *   <li>An argument count above {@link #MAX_ARITY} — {@link #inputHandle} only has hand-written
 *       {@code MethodHandle}s for 0..{@value #MAX_ARITY} arguments (Trino's {@code
 *       AccumulatorCompiler} expects an exact, statically-typed {@code (State, ValueBlock, int,
 *       ValueBlock, int, ...)} parameter list per argument — not a varargs-collectible shape — so a
 *       higher arity needs another hand-written overload, not a generic one).</li>
 *   <li>{@code aggregate_destructor} is never called — a known, deliberate gap for v1: this
 *       connector's single-execution design makes it always safe to call once a group's finalize
 *       result is read, but the state simply isn't reclaimed proactively yet. See the README.</li>
 * </ul>
 */
public final class VgiAggregateFunctions {

    private static final Logger LOG = Logger.get(VgiAggregateFunctions.class);
    private static final int MAX_ARITY = 4;
    private static final int FLUSH_THRESHOLD = 4096;
    private static final String GROUP_ID_FIELD = "__vgi_group_id";

    private VgiAggregateFunctions() {}

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    /**
     * One aggregate argument, resolved at discovery time.
     *
     * @param anyType whether this is a {@code vgi_type=any} argument — {@code type} is {@code
     *        null} and {@code typeVarName} identifies its {@link Signature#typeVariable}, mirroring
     *        {@code VgiScalarFunctions.ScalarArg} exactly
     * @param varargs whether this is the trailing {@code vgi_varargs} argument — see {@link
     *        #effectiveArgs}
     */
    record AggregateArg(String name, boolean positional, boolean constArg, boolean anyType, boolean varargs,
            Type type, String typeVarName, Field arrowHint) {}

    /** One registered aggregate: static discovery-time info plus the Trino {@link FunctionMetadata}. */
    record Entry(FunctionId functionId, String schemaName, String functionName,
            FunctionMetadata metadata, List<AggregateArg> args, Type returnType) {}

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

        /**
         * @return this function's {@link AggregationFunctionMetadata} — always non-decomposable,
         *         see the class javadoc — or {@code null} for an unknown {@link FunctionId}. Public
         *         (unlike {@link #entryFor}) since {@code VgiMetadata}, in a different package,
         *         needs this without needing the rest of {@link Entry}'s discovery-time detail.
         */
        public AggregationFunctionMetadata aggregationFunctionMetadataFor(FunctionId functionId) {
            Entry entry = byId.get(functionId);
            return entry == null ? null : AggregationFunctionMetadata.builder().build();
        }
    }

    private static String nameKey(String schemaName, String functionName) {
        return schemaName.toLowerCase(Locale.ROOT) + "." + functionName.toLowerCase(Locale.ROOT);
    }

    /**
     * Discover every {@code AGGREGATE_FUNCTION} across every schema — mirrors {@link
     * VgiScalarFunctions#discover} exactly, minus the lossless-overload-collision handling (v1
     * aggregates have no unsigned-width-collapsing case to worry about yet, since every
     * unsupported argument shape is simply skipped rather than mapped lossily).
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
                        a.handle(), schemaName, "AGGREGATE_FUNCTION", null, null);
                for (byte[] item : functions.items()) {
                    FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
                    Entry entry = tryBuildEntry(schemaName, info, overloadCounters);
                    if (entry == null) continue; // unsupported shape — reason already logged
                    byName.computeIfAbsent(nameKey(schemaName, info.name()), k -> new ArrayList<>()).add(entry);
                    byId.put(entry.functionId(), entry);
                }
            }
            return new Registry(byId, byName);
        });
    }

    private static Entry tryBuildEntry(String schemaName, FunctionInfo info, Map<String, Integer> overloadCounters) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<AggregateArg> args = new ArrayList<>();
        int anyIndex = 0;
        for (Field field : argsSchema == null ? List.<Field>of() : argsSchema.getFields()) {
            AggregateArg arg = decodeAggregateArg(context, field, anyIndex);
            if (arg == null) return null; // reason already logged
            if (arg.anyType()) anyIndex++;
            args.add(arg);
        }
        // A vararg argument must be the LAST one — Signature.variableArity() means "the last
        // declared argument type repeats," which has no representation for a vararg group followed
        // by more fixed arguments. Mirrors VgiScalarFunctions' identical check.
        for (int i = 0; i < args.size() - 1; i++) {
            if (args.get(i).varargs()) {
                LOG.warn("VGI aggregate function %s: skipping registration — argument '%s' is vgi_varargs but "
                        + "isn't the LAST argument; Trino's variable-arity signature only supports a repeating "
                        + "trailing argument", context, args.get(i).name());
                return null;
            }
        }
        if (args.size() > MAX_ARITY) {
            LOG.warn("VGI aggregate function %s: skipping registration — %d arguments exceeds the %d this "
                    + "connector has hand-written input MethodHandles for", context, args.size(), MAX_ARITY);
            return null;
        }

        Schema outSchema = ArrowSchemaCodec.deserializeSchema(info.output_schema());
        if (outSchema == null || outSchema.getFields().size() != 1) {
            LOG.warn("VGI aggregate function %s: skipping registration — expected exactly one output column, got %s",
                    context, outSchema == null ? "none" : outSchema.getFields().size());
            return null;
        }
        Field outField = outSchema.getFields().get(0);
        Map<String, String> outMeta = outField.getMetadata();
        boolean dynamicReturn = (outMeta != null && "true".equals(outMeta.get("vgi:any")))
                || outField.getType().getTypeID() == ArrowType.ArrowTypeID.Null;
        if (dynamicReturn) {
            LOG.warn("VGI aggregate function %s: skipping registration — its return type is computed dynamically "
                            + "at bind time, which has no Trino representation", context);
            return null;
        }
        Type returnType;
        try {
            returnType = VgiTypeMapping.toTrinoType(outField);
        } catch (UnsupportedOperationException e) {
            LOG.warn(e, "VGI aggregate function %s: skipping registration — unsupported return type", context);
            return null;
        }

        Signature.Builder sig = Signature.builder();
        for (AggregateArg arg : args) {
            if (arg.anyType()) sig.typeVariable(arg.typeVarName());
        }
        for (AggregateArg arg : args) {
            if (arg.anyType()) sig.argumentType(typeVariable(arg.typeVarName()));
            else sig.argumentType(arg.type());
        }
        if (!args.isEmpty() && args.get(args.size() - 1).varargs()) {
            sig.variableArity();
        }
        sig.returnType(returnType);

        int overloadIndex = overloadCounters.merge(schemaName + ":" + info.name(), 1, Integer::sum) - 1;
        FunctionId functionId = new FunctionId("vgi_agg:" + schemaName + ":" + info.name() + ":" + overloadIndex);

        FunctionMetadata metadata = FunctionMetadata.aggregateBuilder(info.name())
                .signature(sig.build())
                .functionId(functionId)
                // VGI's default null handling (confirmed against the real vgi_sum fixture: with
                // NullHandling.DEFAULT, a NULL-valued row's update() is never called at all) matches
                // Trino's own non-nullable-argument convention exactly — the engine pre-filters null
                // rows before ever calling our input() handle, so this connector never needs to see
                // one. A VGI aggregate whose null handling genuinely differs is out of v1 scope.
                .argumentNullability(Collections.nCopies(args.size(), false))
                .nullable()
                .description(info.description() == null ? "" : info.description())
                .build();

        return new Entry(functionId, schemaName, info.name(), metadata, args, returnType);
    }

    private static AggregateArg decodeAggregateArg(String context, Field field, int anyIndex) {
        Map<String, String> metadata = field.getMetadata();
        String vgiType = metadata == null ? null : metadata.get("vgi_type");
        if ("table".equals(vgiType)) {
            LOG.warn("VGI aggregate function %s: skipping registration — argument '%s' is TABLE-typed",
                    context, field.getName());
            return null;
        }
        boolean positional = metadata == null || !"named".equals(metadata.get("vgi_arg"));
        boolean constArg = metadata != null && "true".equals(metadata.get("vgi_const"));
        boolean varargs = metadata != null && "true".equals(metadata.get("vgi_varargs"));
        // Same combination scalars reject: a constant vararg has no real fixture and no clear wire
        // meaning (bind-time constants and a per-row varargs group are two different channels).
        if (varargs && constArg) {
            LOG.warn("VGI aggregate function %s: skipping registration — argument '%s' is both vgi_const and "
                    + "vgi_varargs, a combination with no clear wire meaning", context, field.getName());
            return null;
        }
        if ("any".equals(vgiType)) {
            return new AggregateArg(field.getName(), positional, constArg, true, varargs, null,
                    "T" + anyIndex, null);
        }
        Type type;
        try {
            type = VgiTypeMapping.toTrinoType(field);
        } catch (UnsupportedOperationException e) {
            LOG.warn(e, "VGI aggregate function %s: skipping registration — unsupported type for argument '%s'",
                    context, field.getName());
            return null;
        }
        return new AggregateArg(field.getName(), positional, constArg, false, varargs, type, null, field);
    }

    // ------------------------------------------------------------------
    // Per-call-site config
    // ------------------------------------------------------------------

    /**
     * @param effectiveArgs {@code entry.args()}, expanded to this call site's actual bound arity —
     *        identical to {@code entry.args()} for a non-variadic function; for a variable-arity
     *        one, the trailing vararg spec is repeated (each repeat given a distinct synthetic
     *        name, since Arrow field names must be unique — VGI's own dispatch reads a vararg
     *        group by BATCH COLUMN POSITION, not name, mirroring {@code
     *        VgiScalarFunctions.CallConfig}'s identical field)
     * @param rowArgIndices signature-order indices of the non-const arguments — {@code
     *        updateSchema}'s columns are these, in this order (excluding the trailing group-id one)
     * @param constArgIndices signature-order indices of the {@code vgi_const} arguments — read off
     *        the first observed row and used ONLY to build the lazy {@code aggregate_bind} call
     *        (see {@link AggregateState})
     * @param updateSchema the wire schema for {@code aggregate_update}'s {@code input_batch}: the
     *        ROW (non-const) argument columns, in order, plus the trailing {@link #GROUP_ID_FIELD}
     *        column every row is tagged with
     * @param bindInputSchema the wire schema {@code aggregate_bind} declares as {@code
     *        input_schema} — the SAME row-argument columns, WITHOUT the group-id column (bind
     *        describes the shape of one group's logical input, not the batched-update wire framing,
     *        and never includes const arguments — those travel via {@code arguments} instead)
     */
    record CallConfig(Entry entry, List<Type> argumentTypes, Type returnType, List<AggregateArg> effectiveArgs,
            int[] rowArgIndices, int[] constArgIndices, Schema updateSchema, Schema bindInputSchema) {}

    public static CallConfig buildCallConfig(Entry entry, List<Type> argumentTypes, Type returnType) {
        List<AggregateArg> specs = effectiveArgs(entry, argumentTypes.size());
        List<Integer> rowIdx = new ArrayList<>();
        List<Integer> constIdx = new ArrayList<>();
        List<Field> rowFields = new ArrayList<>();
        for (int i = 0; i < specs.size(); i++) {
            AggregateArg spec = specs.get(i);
            if (spec.constArg()) {
                constIdx.add(i);
            } else {
                rowIdx.add(i);
                rowFields.add(VgiTypeMapping.toArrowField(argumentTypes.get(i), spec.name(), spec.arrowHint()));
            }
        }
        Schema bindInputSchema = new Schema(rowFields);
        List<Field> updateFields = new ArrayList<>(rowFields);
        updateFields.add(new Field(GROUP_ID_FIELD, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        return new CallConfig(entry, argumentTypes, returnType, specs,
                rowIdx.stream().mapToInt(Integer::intValue).toArray(),
                constIdx.stream().mapToInt(Integer::intValue).toArray(),
                new Schema(updateFields), bindInputSchema);
    }

    /**
     * Expand {@code entry.args()} to exactly {@code callArity} entries — see {@link CallConfig}'s
     * {@code effectiveArgs} javadoc.
     */
    private static List<AggregateArg> effectiveArgs(Entry entry, int callArity) {
        List<AggregateArg> declared = entry.args();
        if (callArity == declared.size()) return declared;
        AggregateArg varargSpec = declared.get(declared.size() - 1);
        List<AggregateArg> effective = new ArrayList<>(callArity);
        effective.addAll(declared.subList(0, declared.size() - 1));
        int repeatCount = callArity - (declared.size() - 1);
        for (int i = 0; i < repeatCount; i++) {
            effective.add(new AggregateArg(varargSpec.name() + "_" + i, varargSpec.positional(),
                    varargSpec.constArg(), varargSpec.anyType(), true, varargSpec.type(),
                    varargSpec.typeVarName(), varargSpec.arrowHint()));
        }
        return effective;
    }

    // ------------------------------------------------------------------
    // Accumulator state: one instance per accumulator, shared across every
    // group it holds (Trino's own GroupedAccumulatorState convention — see
    // AccumulatorStateFactory's javadoc: createSingleState/createGroupedState
    // both return the same T, so one class legitimately serves both).
    // ------------------------------------------------------------------

    static final class AggregateState implements GroupedAccumulatorState {
        private final VgiWorkerClient client;
        private final CallConfig config;
        private final VectorSchemaRoot pending;
        private byte[] executionId;
        private byte[] outputSchemaBytes;
        private boolean bound;
        private int groupId;
        private int pendingRows;

        AggregateState(VgiWorkerClient client, CallConfig config) {
            this.client = client;
            this.config = config;
            this.pending = VectorSchemaRoot.create(config.updateSchema(), Allocators.root());
            this.pending.allocateNew();
            // A function with no const arguments needs no observed row to bind correctly — bind
            // immediately, exactly like v1's original design, rather than deferring for no reason.
            if (config.constArgIndices().length == 0) {
                bind(List.of());
            }
        }

        private void bind(List<Object> constValues) {
            byte[] argumentsBytes = encodeConstArgs(config, constValues);
            byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(config.bindInputSchema());
            AggregateBindResponse response = client.withConnection(a -> a.service().aggregate_bind(
                    new AggregateBindRequest(config.entry().functionName(), argumentsBytes, inputSchemaBytes,
                            null, null, a.handle(), config.entry().schemaName())));
            this.executionId = response.execution_id();
            this.outputSchemaBytes = response.output_schema();
            this.bound = true;
        }

        @Override
        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size) {
            // No group-indexed structure to pre-size — every row is tagged with its own group id
            // in the shared pending buffer regardless of how many distinct groups exist.
        }

        @Override
        public long getEstimatedSize() {
            return 4096L + (long) pendingRows * 64L;
        }

        /**
         * Buffer one row's already-resolved argument values (in SIGNATURE order — every declared
         * argument, const ones included, since Trino has no concept of "const" at the SPI level),
         * tagged with the CURRENT group id. A function with const arguments binds lazily here, on
         * the FIRST call only — see the class javadoc's "Const arguments" section.
         */
        void addRow(Object[] boxedArgs) {
            if (!bound) {
                List<Object> constValues = new ArrayList<>();
                for (int sigIdx : config.constArgIndices()) constValues.add(boxedArgs[sigIdx]);
                bind(constValues);
            }
            int[] rowIdx = config.rowArgIndices();
            for (int pos = 0; pos < rowIdx.length; pos++) {
                int sigIdx = rowIdx[pos];
                VgiTypeMapping.writeValue(config.argumentTypes().get(sigIdx), pending.getVector(pos), pendingRows,
                        boxedArgs[sigIdx]);
            }
            ((BigIntVector) pending.getVector(rowIdx.length)).setSafe(pendingRows, groupId);
            pendingRows++;
            if (pendingRows >= FLUSH_THRESHOLD) flush();
        }

        /** Send whatever rows are buffered via one {@code aggregate_update} call, then reset the buffer. */
        void flush() {
            if (pendingRows == 0) return;
            for (FieldVector v : pending.getFieldVectors()) v.setValueCount(pendingRows);
            pending.setRowCount(pendingRows);
            byte[] batchBytes = ArrowBatchCodec.serialize(pending);
            client.withConnection(a -> a.service().aggregate_update(new AggregateUpdateRequest(
                    config.entry().functionName(), executionId, batchBytes, a.handle(), config.entry().schemaName())));
            pendingRows = 0;
            for (FieldVector v : pending.getFieldVectors()) v.reset();
        }

        /**
         * Flush any pending rows, then finalize the CURRENT group id and return its boxed result
         * value. Terminal for that group — see the class javadoc's note on {@code
         * aggregate_destructor} not being called yet.
         */
        Object finalizeCurrentGroup() {
            // Never bound at all: this accumulator's entire lifetime saw zero rows across every
            // group (e.g. aggregating an empty table) — a const argument's value was never
            // observed, so there is nothing correct to bind with. NULL is the right empty-aggregate
            // answer for every real fixture this connector has seen; see the class javadoc.
            if (!bound) return null;
            flush();
            Schema groupIdSchema = new Schema(List.of(new Field("group_id",
                    FieldType.notNullable(new ArrowType.Int(64, true)), null)));
            try (VectorSchemaRoot gidRoot = VectorSchemaRoot.create(groupIdSchema, Allocators.root())) {
                gidRoot.allocateNew();
                ((BigIntVector) gidRoot.getVector(0)).setSafe(0, groupId);
                gidRoot.getVector(0).setValueCount(1);
                gidRoot.setRowCount(1);
                byte[] gidBytes = ArrowBatchCodec.serialize(gidRoot);
                AggregateFinalizeResponse resp = client.withConnection(a -> a.service().aggregate_finalize(
                        new AggregateFinalizeRequest(config.entry().functionName(), executionId, gidBytes,
                                outputSchemaBytes, a.handle(), config.entry().schemaName())));
                return ArrowBatchCodec.withReadBatch(resp.result_batch(), Allocators.root(), root -> {
                    if (root == null || root.getRowCount() == 0) return null;
                    return VgiTypeMapping.readValue(config.returnType(), root.getVector(0), 0);
                });
            }
        }
    }

    /**
     * Build {@code aggregate_bind}'s {@code arguments} bytes from the observed const values —
     * mirrors {@code VgiScalarFunctions.BindCache#encodeConstArgs} exactly, including always
     * encoding a (possibly zero-child) args batch rather than a bare {@code null} (confirmed
     * necessary for a multi-overload function's worker-side dispatch — see that method's own note).
     */
    private static byte[] encodeConstArgs(CallConfig cfg, List<Object> constValues) {
        ArgumentsEncoder encoder = ArgumentsEncoder.builder();
        int[] constIdx = cfg.constArgIndices();
        for (int i = 0; i < constIdx.length; i++) {
            int sigIndex = constIdx[i];
            AggregateArg spec = cfg.effectiveArgs().get(sigIndex);
            Type type = cfg.argumentTypes().get(sigIndex);
            ScalarValue value = toScalarValue(type, constValues.get(i));
            if (spec.positional()) encoder.positional(value);
            else encoder.named(spec.name(), value);
        }
        return encoder.encode();
    }

    /** Build the {@code ArgumentsEncoder}-ready constant — see {@code VgiTypeMapping#toPlainValue}'s
     *  own javadoc for the one honest caveat (a nested struct/list field's exact width isn't always
     *  preserved). */
    private static ScalarValue toScalarValue(Type trinoType, Object boxedValue) {
        ArrowType arrowType = VgiTypeMapping.toArrowField(trinoType, "value").getType();
        if (boxedValue == null) return ScalarValue.ofNull(arrowType);
        return ScalarValue.of(arrowType, VgiTypeMapping.toPlainValue(trinoType, boxedValue));
    }

    static final class StateFactory implements AccumulatorStateFactory<AggregateState> {
        private final VgiWorkerClient client;
        private final CallConfig config;

        StateFactory(VgiWorkerClient client, CallConfig config) {
            this.client = client;
            this.config = config;
        }

        @Override
        public AggregateState createSingleState() {
            return new AggregateState(client, config);
        }

        @Override
        public AggregateState createGroupedState() {
            return new AggregateState(client, config);
        }
    }

    /**
     * Never actually invoked: {@link #aggregationFunctionMetadataFor} declares every VGI aggregate
     * non-decomposable (no intermediate type), so Trino never ships/merges a serialized
     * intermediate state across a stage boundary. Still required structurally by {@link
     * io.trino.spi.function.AggregationImplementation.Builder#accumulatorStateDescriptor} — throws
     * rather than silently producing a meaningless value if this assumption ever turns out wrong.
     */
    static final class StateSerializer implements AccumulatorStateSerializer<AggregateState> {
        @Override
        public Type getSerializedType() {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public void serialize(AggregateState state, BlockBuilder out) {
            throw new UnsupportedOperationException(
                    "VGI aggregates are declared non-decomposable; serialize should never be invoked");
        }

        @Override
        public void deserialize(Block block, int index, AggregateState state) {
            throw new UnsupportedOperationException(
                    "VGI aggregates are declared non-decomposable; deserialize should never be invoked");
        }
    }

    // ------------------------------------------------------------------
    // Dispatch: hand-written input MethodHandles for arity 0..MAX_ARITY (see
    // the class javadoc for why this can't be a generic collector), one
    // shared output MethodHandle regardless of arity.
    // ------------------------------------------------------------------

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final MethodHandle[] INPUT_HANDLES = new MethodHandle[MAX_ARITY + 1];
    private static final MethodHandle OUTPUT_HANDLE;

    static {
        try {
            INPUT_HANDLES[0] = LOOKUP.findStatic(VgiAggregateFunctions.class, "input0",
                    java.lang.invoke.MethodType.methodType(void.class, AggregateState.class));
            INPUT_HANDLES[1] = LOOKUP.findStatic(VgiAggregateFunctions.class, "input1",
                    java.lang.invoke.MethodType.methodType(void.class,
                            AggregateState.class, ValueBlock.class, int.class));
            INPUT_HANDLES[2] = LOOKUP.findStatic(VgiAggregateFunctions.class, "input2",
                    java.lang.invoke.MethodType.methodType(void.class,
                            AggregateState.class, ValueBlock.class, int.class, ValueBlock.class, int.class));
            INPUT_HANDLES[3] = LOOKUP.findStatic(VgiAggregateFunctions.class, "input3",
                    java.lang.invoke.MethodType.methodType(void.class, AggregateState.class,
                            ValueBlock.class, int.class, ValueBlock.class, int.class, ValueBlock.class, int.class));
            INPUT_HANDLES[4] = LOOKUP.findStatic(VgiAggregateFunctions.class, "input4",
                    java.lang.invoke.MethodType.methodType(void.class, AggregateState.class,
                            ValueBlock.class, int.class, ValueBlock.class, int.class,
                            ValueBlock.class, int.class, ValueBlock.class, int.class));
            OUTPUT_HANDLE = LOOKUP.findStatic(VgiAggregateFunctions.class, "output",
                    java.lang.invoke.MethodType.methodType(void.class, AggregateState.class, BlockBuilder.class));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** @return the hand-written {@code input} {@link MethodHandle} for exactly {@code arity} arguments */
    public static MethodHandle inputHandle(int arity) {
        if (arity < 0 || arity >= INPUT_HANDLES.length || INPUT_HANDLES[arity] == null) {
            throw new IllegalArgumentException("no input MethodHandle for arity " + arity);
        }
        return INPUT_HANDLES[arity];
    }

    public static MethodHandle outputHandle() {
        return OUTPUT_HANDLE;
    }

    private static void input0(AggregateState state) {
        state.addRow(new Object[0]);
    }

    private static void input1(AggregateState state, ValueBlock b0, int p0) {
        state.addRow(new Object[] {readArg(state, 0, b0, p0)});
    }

    private static void input2(AggregateState state, ValueBlock b0, int p0, ValueBlock b1, int p1) {
        state.addRow(new Object[] {readArg(state, 0, b0, p0), readArg(state, 1, b1, p1)});
    }

    private static void input3(AggregateState state, ValueBlock b0, int p0, ValueBlock b1, int p1,
            ValueBlock b2, int p2) {
        state.addRow(new Object[] {
                readArg(state, 0, b0, p0), readArg(state, 1, b1, p1), readArg(state, 2, b2, p2)});
    }

    private static void input4(AggregateState state, ValueBlock b0, int p0, ValueBlock b1, int p1,
            ValueBlock b2, int p2, ValueBlock b3, int p3) {
        state.addRow(new Object[] {
                readArg(state, 0, b0, p0), readArg(state, 1, b1, p1),
                readArg(state, 2, b2, p2), readArg(state, 3, b3, p3)});
    }

    private static Object readArg(AggregateState state, int argIndex, ValueBlock block, int position) {
        return VgiTypeMapping.readBoxedValue(state.config.argumentTypes().get(argIndex), block, position);
    }

    private static void output(AggregateState state, BlockBuilder out) {
        Object result = state.finalizeCurrentGroup();
        VgiTypeMapping.writeBoxedValue(state.config.returnType(), out, result);
    }
}
