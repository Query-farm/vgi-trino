// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.function;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SPIKE — proves out Trino's connector-defined scalar-function SPI
 * ({@code ConnectorMetadata.getFunctions}/{@code FunctionProvider.getScalarFunctionImplementation})
 * end to end against a REAL VGI worker's exchange-mode scalar call, not a mock.
 *
 * <p>Deliberately hardcoded to exactly one function — the reference fixture's
 * {@code main.passthru} ({@code value: Utf8 -> Utf8}, no const/bind-time
 * arguments, no settings) — the simplest possible signature, chosen so this
 * spike isolates the one question actually in doubt (does Trino's dispatch
 * mechanism reach a real VGI worker at all) from a separate, larger piece of
 * work this does NOT attempt: dynamically decoding {@code
 * FunctionInfo.arguments}/{@code output_schema} from {@code
 * catalog_schema_contents_functions} to discover arbitrary catalog scalars.
 * That's real, scoped follow-on work, not a blocker this spike needed to
 * solve to answer its actual question.
 *
 * <h2>The performance question this spike deliberately does NOT solve</h2>
 *
 * <p>Trino's scalar {@link java.lang.invoke.MethodHandle} calling convention is
 * row-at-a-time from the connector's perspective — there is no batch-level
 * hook. VGI's scalar protocol is exchange-mode (client sends a batch, worker
 * answers a batch), so a naive per-row implementation means one full RPC
 * round trip per row. This spike mitigates the worst of that (opening the
 * bind/init exchange stream ONCE per query via Trino's {@code
 * instanceFactory} mechanism, then reusing that one open stream for one
 * exchange turn per row) but does not eliminate the fundamental one-round-trip-
 * per-row cost. A production version would need real inter-row batching
 * (accumulate several {@link #invoke} calls, flush one exchange turn for
 * many rows at once) — a genuinely separate design problem from "does the
 * dispatch mechanism work," which is all this spike answers.
 */
public final class VgiScalarFunctionSpike {

    private VgiScalarFunctionSpike() {}

    private static final String SCHEMA_NAME = "main";
    private static final String FUNCTION_NAME = "passthru";
    private static final Schema INPUT_SCHEMA = new Schema(List.of(
            new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)));

    /** True iff {@code (schemaName, functionName)} is the one function this spike hardcodes. */
    public static boolean matches(String schemaName, String functionName) {
        return SCHEMA_NAME.equalsIgnoreCase(schemaName) && FUNCTION_NAME.equalsIgnoreCase(functionName);
    }

    /** The {@link FunctionId} this spike's one function is always registered under. */
    private static final FunctionId FUNCTION_ID = new FunctionId("vgi_spike:" + SCHEMA_NAME + ":" + FUNCTION_NAME);

    public static FunctionId functionId() {
        return FUNCTION_ID;
    }

    /** @return the hardcoded {@link FunctionMetadata} for {@code main.passthru}, if this is that function's id */
    public static Optional<FunctionMetadata> metadataFor(FunctionId functionId) {
        if (!functionId.equals(FUNCTION_ID)) return Optional.empty();
        Signature signature = Signature.builder()
                .returnType(VarcharType.VARCHAR)
                .argumentType(VarcharType.VARCHAR)
                .build();
        return Optional.of(FunctionMetadata.scalarBuilder(FUNCTION_NAME)
                .signature(signature)
                .functionId(FUNCTION_ID)
                .description("SPIKE: real VGI exchange-mode scalar, dispatched live via bind/init/exchange")
                .build());
    }

    // ------------------------------------------------------------------
    // Dispatch: instanceFactory opens ONE bind/init exchange stream per query;
    // the main methodHandle reuses it for one exchange() turn per row.
    // ------------------------------------------------------------------

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    /** {@code () -> ScalarStream}, bound to a specific {@link VgiWorkerClient} via {@code insertArguments}. */
    public static MethodHandle instanceFactory(VgiWorkerClient client) {
        try {
            MethodHandle raw = LOOKUP.findStatic(VgiScalarFunctionSpike.class, "openStream",
                    java.lang.invoke.MethodType.methodType(ScalarStream.class, VgiWorkerClient.class));
            return MethodHandles.insertArguments(raw, 0, client);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** {@code (ScalarStream, Slice) -> Slice} — Trino's instance parameter first, then the real argument. */
    public static MethodHandle methodHandle() {
        try {
            return LOOKUP.findStatic(VgiScalarFunctionSpike.class, "invoke",
                    java.lang.invoke.MethodType.methodType(Slice.class, ScalarStream.class, Slice.class));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Holds one open bind/init exchange stream, kept alive for the query's duration via Trino's per-call-site
     *  instance mechanism, and released back to the pool when Trino discards the instance. */
    static final class ScalarStream implements AutoCloseable {
        private final VgiWorkerClient client;
        private final VgiWorkerClient.Attached connection;
        private final ClientStreamSession<?> session;
        private volatile boolean healthy = true;

        private ScalarStream(VgiWorkerClient client, VgiWorkerClient.Attached connection, ClientStreamSession<?> session) {
            this.client = client;
            this.connection = connection;
            this.session = session;
        }

        @Override
        public void close() {
            try {
                session.close();
            } catch (Exception e) {
                healthy = false;
            } finally {
                client.release(connection, healthy);
            }
        }
    }

    /** Opens the bind/init exchange stream once — invoked by Trino's {@code instanceFactory} hook. */
    static ScalarStream openStream(VgiWorkerClient client) {
        VgiWorkerClient.Attached a = client.borrow();
        boolean ok = false;
        try {
            byte[] inputSchemaBytes = ArrowSchemaCodec.serializeSchema(INPUT_SCHEMA);
            BindRequest bindRequest = new BindRequest(
                    FUNCTION_NAME,
                    null,           // arguments — no const bind-time args
                    "SCALAR",
                    inputSchemaBytes,
                    null,           // settings
                    null,           // secrets
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    false,          // resolved_secrets_provided
                    null, null,     // at_unit / at_value
                    null, null,     // copy_from / copy_to
                    SCHEMA_NAME);
            BindResponse bound = a.service().bind(bindRequest, null);
            InitRequest initRequest = new InitRequest(
                    RecordCodec.serializeToBytes(bindRequest),
                    bound.output_schema(),
                    bound.opaque_data(),
                    null, null, null, null, null, null,
                    null, null, null, null,
                    null, null,
                    null, null, null, null);
            RpcStream<? extends StreamState> stream = a.service().init(initRequest, null);
            ScalarStream result = new ScalarStream(client, a, (ClientStreamSession<?>) stream);
            ok = true;
            return result;
        } finally {
            if (!ok) client.release(a, false);
        }
    }

    /** One exchange turn: a single-row input batch in, the single-row {@code result} column out. */
    static Slice invoke(ScalarStream stream, Slice value) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(INPUT_SCHEMA, Allocators.root())) {
            input.allocateNew();
            VarCharVector valueVector = (VarCharVector) input.getVector("value");
            if (value == null) {
                valueVector.setNull(0);
            } else {
                valueVector.setSafe(0, value.getBytes());
            }
            valueVector.setValueCount(1);
            input.setRowCount(1);

            AnnotatedBatch out = stream.session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = out.root();
            VarCharVector result = (VarCharVector) root.getVector("result");
            if (result.isNull(0)) return null;
            return Slices.wrappedBuffer(result.get(0));
        } catch (RuntimeException e) {
            stream.healthy = false;
            throw e;
        }
    }
}
