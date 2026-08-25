// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.upstream;

import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.Signature;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.BigintType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Collections.nCopies;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Minimal, VGI-independent reproducer for an upstream Trino 483 bug — NOT part of this
 * connector's own test coverage (nothing here depends on any {@code farm.query.vgitrino} or
 * {@code farm.query.vgi} class). Written to accompany a bug report against
 * <a href="https://github.com/trinodb/trino">trinodb/trino</a>: a connector-defined scalar
 * function that declares {@code supportsSession=true} (the same mechanism Trino's own built-in
 * {@code current_user()}/{@code current_path()} use internally) and is adapted via {@link
 * ScalarFunctionAdapter#adapt} — the standard pattern real connectors (Iceberg, this one) use to
 * bridge a simple implementation convention to whatever Trino actually requests at a call site —
 * hits two distinct failures once a genuine per-row column argument is involved:
 *
 * <ol>
 *   <li>{@link #columnArgumentProducesInvalidColumnarBytecode()} — querying the function against a
 *       real table column triggers Trino's columnar {@code PageProjectionWork} code generator,
 *       which produces bytecode that fails ASM class verification (a missing unboxing conversion
 *       before an {@code LSTORE}, confirmed by decompiling the generated class in the exception
 *       message).</li>
 *   <li>{@link #noFromLiteralArgumentHitsUnboundSession()} — querying the function with a bare
 *       literal argument and no real table source (so Trino evaluates the whole projection via
 *       {@code IrExpressionEvaluator} while building a {@code ValuesNode}-only local plan) supplies
 *       a {@code ConnectorSession} that was never bound to any catalog ({@code
 *       FullConnectorSession(session, identity)}, whose 2-arg constructor leaves {@code
 *       properties}/{@code catalogHandle}/{@code catalogName} all {@code null}) — so {@code
 *       ConnectorSession.getProperty} unconditionally throws {@code "Session property
 *       'null.<name>' does not exist"}, even though the property genuinely is declared and a real
 *       {@code SET SESSION} against the SAME session succeeds.</li>
 * </ol>
 *
 * <p>Neither failure reproduces for a session-declaring function with zero regular arguments —
 * confirmed separately (not reproduced here) against a real connector (Query Farm's
 * <a href="https://github.com/Query-farm/vgi-trino">vgi-trino</a>), whose {@code secret_field()}
 * equivalent works end to end. Both come from the one connector-defined-function feature genuinely
 * untested by Trino's own first-party code: no built-in {@code supportsSession} function (all of
 * which only ever read session/system-level state, never a catalog session property) ever
 * exercises this exact combination the way a real connector reading its own declared settings
 * needs to.
 */
public final class TrinoSupportsSessionColumnarBugReproTest {

    private static final String SETTING_NAME = "multiplier";

    @Test
    @Timeout(60)
    void columnArgumentProducesInvalidColumnarBytecode() throws Exception {
        try (DistributedQueryRunner runner = newRunner()) {
            Session withSetting = Session.builder(runner.getDefaultSession())
                    .setCatalogSessionProperty("repro", SETTING_NAME, "3")
                    .build();
            // Expected: 5 rows of x * 3. Actual (Trino 483): "Error processing class definition" —
            // ASM bytecode-verification failure in a generated PageProjectionWork class (a missing
            // unboxing conversion before an LSTORE; run with the assertThrows'd exception printed
            // to see the full decompiled class).
            Exception e = assertThrows(Exception.class,
                    () -> runner.execute(withSetting, "SELECT repro.default.repro_setting(x) FROM repro.default.t"));
            assertTrue(e.getMessage().contains("Error processing class definition"), e.getMessage());
        }
    }

    @Test
    @Timeout(60)
    void noFromLiteralArgumentHitsUnboundSession() throws Exception {
        try (DistributedQueryRunner runner = newRunner()) {
            Session withSetting = Session.builder(runner.getDefaultSession())
                    .setCatalogSessionProperty("repro", SETTING_NAME, "3")
                    .build();
            // Expected: a single row, 15 (5 * 3). Actual (Trino 483): TrinoException
            // "Session property 'null.multiplier' does not exist" from FullConnectorSession —
            // even though the SAME session's setCatalogSessionProperty call above is genuinely
            // valid (a real `SET SESSION repro.multiplier = '3'` against this session succeeds).
            Exception e = assertThrows(Exception.class,
                    () -> runner.execute(withSetting, "SELECT repro.default.repro_setting(5)"));
            assertTrue(e.getMessage().contains("Session property 'null.multiplier' does not exist"), e.getMessage());
        }
    }

    private static DistributedQueryRunner newRunner() throws Exception {
        Session session = TestingSession.testSessionBuilder().setCatalog("repro").setSchema("default").build();
        DistributedQueryRunner runner = DistributedQueryRunner.builder(session).setWorkerCount(1).build();
        runner.installPlugin(new ReproPlugin());
        runner.createCatalog("repro", "repro", Map.of());
        return runner;
    }

    // ------------------------------------------------------------------
    // Minimal connector: one schema, one table `t` with one BIGINT column `x`
    // (values 0..4), and one scalar function `repro_setting(x) -> x * <session
    // property 'multiplier'>` declared with supportsSession=true.
    // ------------------------------------------------------------------

    public static final class ReproPlugin implements Plugin {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories() {
            return List.of(new ReproConnectorFactory());
        }
    }

    private static final class ReproConnectorFactory implements ConnectorFactory {
        @Override
        public String getName() {
            return "repro";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
            return new ReproConnector();
        }
    }

    public record ReproTransactionHandle() implements ConnectorTransactionHandle {}

    public record ReproTableHandle() implements ConnectorTableHandle {}

    public record ReproColumnHandle(String name) implements ColumnHandle {}

    public record ReproSplit() implements ConnectorSplit {}

    private static final class ReproConnector implements Connector {
        @Override
        public ConnectorTransactionHandle beginTransaction(
                IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
            return new ReproTransactionHandle();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
            return new ReproMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return new ConnectorSplitManager() {
                @Override
                public ConnectorSplitSource getSplits(
                        ConnectorTransactionHandle transaction, ConnectorSession session,
                        ConnectorTableHandle table, java.util.Set<ColumnHandle> dynamicFilterColumns,
                        Constraint constraint) {
                    return new FixedSplitSource(new ReproSplit());
                }
            };
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return new ConnectorPageSourceProvider() {
                @Override
                public io.trino.spi.connector.ConnectorPageSource createPageSource(
                        ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split,
                        ConnectorTableHandle table, Optional<ConnectorTableCredentials> tableCredentials,
                        List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
                    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 5);
                    for (long v = 0; v < 5; v++) BigintType.BIGINT.writeLong(builder, v);
                    return new FixedPageSource(List.of(new Page(builder.build())));
                }
            };
        }

        @Override
        public Optional<FunctionProvider> getFunctionProvider() {
            return Optional.of(new ReproFunctionProvider());
        }

        @Override
        public List<PropertyMetadata<?>> getSessionProperties() {
            return List.of(PropertyMetadata.stringProperty(SETTING_NAME, "multiplier for repro_setting", null, false));
        }

        @Override
        public void shutdown() {}
    }

    private static final class ReproMetadata implements ConnectorMetadata {
        private static final SchemaTableName TABLE_NAME = new SchemaTableName("default", "t");
        static final FunctionId FUNCTION_ID = new FunctionId("repro:repro_setting");
        private static final FunctionMetadata FUNCTION_METADATA = FunctionMetadata.scalarBuilder("repro_setting")
                .signature(Signature.builder()
                        .argumentType(BigintType.BIGINT)
                        .returnType(BigintType.BIGINT)
                        .build())
                .functionId(FUNCTION_ID)
                .argumentNullability(true)
                .nullable()
                .nondeterministic() // depends on session state outside its Signature arguments
                .description("x * session property 'multiplier' — upstream repro")
                .build();

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session, SchemaTableName tableName,
                Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
            return tableName.equals(TABLE_NAME) ? new ReproTableHandle() : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(TABLE_NAME, List.of(new ColumnMetadata("x", BigintType.BIGINT)));
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of("x", new ReproColumnHandle("x"));
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column) {
            return new ColumnMetadata(((ReproColumnHandle) column).name(), BigintType.BIGINT);
        }

        @Override
        public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name) {
            return "repro_setting".equals(name.functionName()) ? List.of(FUNCTION_METADATA) : List.of();
        }

        @Override
        public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId) {
            return FUNCTION_METADATA;
        }

        @Override
        public FunctionDependencyDeclaration getFunctionDependencies(
                ConnectorSession session, FunctionId functionId, BoundSignature boundSignature) {
            return FunctionDependencyDeclaration.NO_DEPENDENCIES;
        }
    }

    /** A trivial per-call-site instance, standing in for the real connector's own Invoker — present
     *  because the originally-observed bytecode dump showed one (a {@code __cachedInstance0} field),
     *  so this repro includes the same shape rather than omitting it as an untested variable. */
    public record ReproInstance() {}

    private static final class ReproFunctionProvider implements FunctionProvider {
        private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
        private static final MethodHandle INVOKE;
        private static final MethodHandle NEW_INSTANCE;

        static {
            try {
                INVOKE = LOOKUP.findStatic(ReproFunctionProvider.class, "invoke",
                        MethodType.methodType(Object.class, ReproInstance.class, ConnectorSession.class, Object.class));
                NEW_INSTANCE = LOOKUP.findStatic(ReproFunctionProvider.class, "newInstance",
                        MethodType.methodType(ReproInstance.class));
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }

        private static ReproInstance newInstance() {
            return new ReproInstance();
        }

        // (ReproInstance, ConnectorSession, Object x) -> Object — reads the "multiplier" session
        // property and multiplies. Boxed-nullable in, nullable-return out: the same convention a
        // real connector's honest null handling uses (see farm.query.vgitrino.function.VgiFunctionProvider).
        private static Object invoke(ReproInstance instance, ConnectorSession session, Object x) {
            if (x == null) return null;
            String multiplier = session.getProperty(SETTING_NAME, String.class);
            if (multiplier == null) throw new IllegalStateException("multiplier session property not set");
            return ((Long) x) * Long.parseLong(multiplier);
        }

        @Override
        public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle handle) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScalarFunctionImplementation getScalarFunctionImplementation(
                FunctionId functionId, BoundSignature boundSignature,
                FunctionDependencies functionDependencies, InvocationConvention invocationConvention) {
            InvocationConvention actualConvention = new InvocationConvention(
                    nCopies(1, BOXED_NULLABLE), NULLABLE_RETURN, true, true);
            MethodHandle adapted = ScalarFunctionAdapter.adapt(
                    INVOKE, BigintType.BIGINT, List.of(BigintType.BIGINT), actualConvention, invocationConvention);
            return ScalarFunctionImplementation.builder().methodHandle(adapted).instanceFactory(NEW_INSTANCE).build();
        }
    }
}
