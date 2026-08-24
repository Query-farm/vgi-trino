// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.UUID;

/**
 * v1 marker transaction handle: VGI transactional attach/commit/rollback
 * ({@code catalog_transaction_begin/commit/rollback}) isn't wired up yet (see
 * the plan's non-goals), so this carries only an id for Trino's own logging —
 * every query gets its own instance, none of them do anything on the VGI side.
 *
 * @param id a per-transaction identifier, for log correlation only
 */
public record VgiTransactionHandle(UUID id) implements ConnectorTransactionHandle {

    /** @return a fresh handle */
    public static VgiTransactionHandle create() {
        return new VgiTransactionHandle(UUID.randomUUID());
    }
}
