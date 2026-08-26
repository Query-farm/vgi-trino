// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgitrino.VgiConfig;
import farm.query.vgitrino.client.VgiWorkerClient;
import farm.query.vgitrino.function.VgiScalarFunctions;
import farm.query.vgitrino.function.VgiTableFunctionHandle;
import farm.query.vgitrino.function.VgiTableInOutFunctionHandle;
import farm.query.vgitrino.metadata.VgiColumnHandle;
import farm.query.vgitrino.metadata.VgiTableHandle;
import farm.query.vgitrino.types.ArrowSchemaCodec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Binds a table's scan function once, then hands off to a {@link VgiSplitSource}
 * that paginates {@code table_function_plan}.
 *
 * <p>The bind happens on whichever pooled connection is available; the
 * resulting {@code bind_call}/{@code bind_opaque_data} are then reused to plan
 * and redeem splits on OTHER pooled connections too. This is intentional, not
 * an oversight — VGI's split tokens are designed to be redeemable "by any
 * worker instance" precisely so a distributed engine's plan phase and its many
 * parallel readers never need to share a connection.
 *
 * <p>v1 does not yet use {@code desiredColumns}/{@code constraint} here — see
 * the plan's Phase 4 for projection/filter pushdown into the plan and bind
 * calls.
 */
public final class VgiSplitManager implements ConnectorSplitManager {

    private final VgiWorkerClient client;
    private final VgiConfig config;

    /**
     * @param client the pooled connection to this catalog's VGI worker
     * @param config this catalog's configuration
     */
    public VgiSplitManager(VgiWorkerClient client, VgiConfig config) {
        this.client = client;
        this.config = config;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> desiredColumns,
            Constraint constraint) {
        VgiTableHandle handle = (VgiTableHandle) table;
        // Trino already tells us exactly which columns this query needs here —
        // no applyProjection plumbing required to push that through as
        // table_function_plan/init's projection_ids. An EMPTY set arrives for
        // e.g. SELECT count(*), where Trino needs no column values at all —
        // sending that through as an explicit projection_ids=[] (zero columns)
        // was tried and is wrong: against a real projection_pushdown=true
        // fixture (example.data.filter_echo_table) it made the worker return
        // ZERO ROWS instead of the correct count, not merely zero-width ones.
        // Treat empty as "no restriction" (null) instead — correct, if not
        // maximally I/O-efficient, for the rare all-columns-elided case.
        List<VgiColumnHandle> projectedColumns = desiredColumns.stream()
                .map(VgiColumnHandle.class::cast)
                .sorted(java.util.Comparator.comparingInt(VgiColumnHandle::ordinal))
                .toList();
        List<Integer> projectionIds = projectedColumns.isEmpty()
                ? null : projectedColumns.stream().map(VgiColumnHandle::ordinal).toList();
        Schema fullSchema = ArrowSchemaCodec.deserializeSchema(handle.outputSchema());
        return client.withConnection(a -> {
            // required_settings/required_secrets resolve here exactly like a scalar function's or
            // classic table-in-out's — reusing VgiScalarFunctions.BindCache#resolveSettings/
            // #resolveSecretFields/#encodeSecrets verbatim; handle.requiredSettings()/
            // requiredSecrets() were resolved once at getTableHandle time (see VgiTableScanFunctions).
            Map<String, String> resolvedSettings =
                    VgiScalarFunctions.BindCache.resolveSettings(handle.requiredSettings(), session);
            Map<String, String> resolvedSecretFields =
                    VgiScalarFunctions.BindCache.resolveSecretFields(handle.requiredSecrets(), session);
            byte[] settingsBytes = resolvedSettings.isEmpty() ? null : SettingsEncoder.of(resolvedSettings);
            byte[] secretsBytes = VgiScalarFunctions.BindCache.encodeSecrets(resolvedSecretFields);

            BindRequest bindRequest = new BindRequest(
                    handle.scanFunctionName(),
                    handle.scanFunctionArguments(),
                    "TABLE",
                    null,           // input_schema — producer-mode table function
                    settingsBytes,
                    secretsBytes,
                    a.handle(),     // attach_opaque_data
                    null,           // transaction_opaque_data
                    secretsBytes != null, // resolved_secrets_provided — see BindCache's own caveat
                    handle.atUnit(), handle.atValue(),     // AS OF, resolved at getTableHandle time
                    null, null,     // copy_from / copy_to
                    // NOT handle.schemaName(): catalog_table_scan_function_get
                    // resolves a table's backing scan function, but doesn't say
                    // which schema that function itself is registered in — it
                    // can differ from the table's own schema (e.g. the
                    // reference fixture's data.rowid_first scans via
                    // main.rowid_sequence). null lets the worker's dispatcher
                    // search every schema by name, which is exactly the
                    // fallback vgi-python's own schema-less Client uses.
                    null);
            BindResponse bound = a.service().bind(bindRequest, null);

            // Two-phase secret negotiation: a scan function that resolves a secret DYNAMICALLY
            // (SecretsAccessor.get() called from inside on_bind's body — not a Secret()/
            // Meta.required_secrets STATIC declaration) reports nothing in FunctionInfo at all —
            // confirmed live against the real fixture (secret_demo backing secret_demo_table):
            // resolve_metadata(SecretDemoFunction).required_secrets == [] — so the proactive
            // resolveSecretFields() call above has nothing to key off and sends nothing for it.
            // VGI's own bind wire protocol covers exactly this case instead: a first bind whose
            // on_bind couldn't resolve a needed secret returns a "secret scope request" —
            // BindResponse.lookup_secret_types/lookup_scopes/lookup_names non-empty, output_schema
            // empty — and the caller must resolve exactly those (never a static declaration) and
            // retry with resolved_secrets_provided=true (mirrors the real C++ extension's own
            // TryParseBindSecretScopeResponse/retry loop in vgi_bind_protocol.cpp).
            if (!bound.lookup_secret_types().isEmpty()) {
                List<FunctionRequiredSecret> requestedSecrets = new ArrayList<>(bound.lookup_secret_types().size());
                for (int i = 0; i < bound.lookup_secret_types().size(); i++) {
                    String scope = bound.lookup_scopes().get(i);
                    String name = bound.lookup_names().get(i);
                    requestedSecrets.add(new FunctionRequiredSecret(
                            bound.lookup_secret_types().get(i),
                            scope == null || scope.isEmpty() ? null : scope,
                            name == null || name.isEmpty() ? null : name));
                }
                Map<String, String> retrySecretFields = new LinkedHashMap<>(
                        VgiScalarFunctions.BindCache.resolveSecretFields(requestedSecrets, session));
                // A dynamically-requested secret (SecretsAccessor.get()/of_type(), the ONLY access
                // pattern that can ever reach this two-phase branch) is read back at process()
                // time through vgi-python's ResolvedSecrets.of_type(), which filters on a
                // synthetic "type" field INSIDE each resolved secret's own field dict (confirmed
                // against the real source: ResolvedSecrets.of_type in vgi/table_function.py checks
                // fields.get("type"), not the dict's own key) — a field the real DuckDB C++
                // extension always attaches when it resolves a CREATE SECRET, but which
                // #resolveSecretFields (built for a STATIC Secret()-annotated argument, read by
                // plain dict key, never by of_type()) never adds on its own. Synthesize it here,
                // only for a secret this connector actually found real fields for — never mark an
                // entirely-unsupplied secret "resolved" just to carry a bare type tag.
                for (FunctionRequiredSecret requested : requestedSecrets) {
                    String secretKey = requested.secret_name() != null ? requested.secret_name() : requested.secret_type();
                    String fieldPrefix = secretKey + ".";
                    boolean anyFieldResolved = retrySecretFields.keySet().stream().anyMatch(k -> k.startsWith(fieldPrefix));
                    if (anyFieldResolved) {
                        retrySecretFields.put(secretKey + ".type", requested.secret_type());
                    }
                }
                byte[] retrySecretsBytes = VgiScalarFunctions.BindCache.encodeSecrets(retrySecretFields);
                BindRequest retryRequest = new BindRequest(
                        bindRequest.function_name(), bindRequest.arguments(), bindRequest.function_type(),
                        bindRequest.input_schema(), bindRequest.settings(), retrySecretsBytes,
                        bindRequest.attach_opaque_data(), bindRequest.transaction_opaque_data(),
                        true, // resolved_secrets_provided — this IS the second, resolved pass
                        bindRequest.at_unit(), bindRequest.at_value(),
                        bindRequest.copy_from(), bindRequest.copy_to(), bindRequest.schema_name());
                bound = a.service().bind(retryRequest, null);
                bindRequest = retryRequest;
            }

            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);
            return new VgiSplitSource(client, config, serializedBindCall, bound.opaque_data(),
                    projectionIds, fullSchema, projectedColumns, handle.constraint());
        });
    }

    /**
     * Splits for a {@code TABLE(catalog.schema.fn(...))} call: the bind
     * already happened in {@link farm.query.vgitrino.function.VgiTableFunction#analyze}
     * (or, for a table-in-out literal call, {@link
     * farm.query.vgitrino.function.VgiTableInOutFunction#analyze}), so this
     * just hands its result straight to a {@link VgiSplitSource} — no extra
     * RPC round trip here.
     *
     * <p>A table-in-out literal call has no {@code table_function_plan}
     * pagination concept at all — one bind, one exchange turn, no splits to
     * enumerate — so it gets a trivial single-split {@link FixedSplitSource}
     * instead of a {@link VgiSplitSource}; see {@link
     * farm.query.vgitrino.split.VgiTableInOutSplit}'s own javadoc.
     */
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle handle) {
        if (handle instanceof VgiTableInOutFunctionHandle h) {
            return new FixedSplitSource(new VgiTableInOutSplit(h));
        }
        VgiTableFunctionHandle functionHandle = (VgiTableFunctionHandle) handle;
        // No predicate or dynamic-filter pushdown for table functions in v1:
        // Trino's ConnectorTableFunction SPI (483) has no Constraint/DynamicFilter
        // hook anywhere in the TABLE(...) call path (ConnectorTableFunctionHandle
        // is a bare marker interface, and TableFunctionProcessorProvider's split
        // processor takes no filter of any kind) — there is nothing to thread
        // through even in principle, not merely something left undone.
        Schema fullSchema = ArrowSchemaCodec.deserializeSchema(functionHandle.outputSchema());
        return new VgiSplitSource(client, config, functionHandle.bindCall(), functionHandle.bindOpaqueData(),
                null, fullSchema, List.of(), TupleDomain.all());
    }
}
