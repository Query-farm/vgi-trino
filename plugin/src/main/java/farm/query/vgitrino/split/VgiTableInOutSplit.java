// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.split;

import farm.query.vgitrino.function.VgiTableInOutFunctionHandle;
import io.trino.spi.connector.ConnectorSplit;

/**
 * The trivial, always-exactly-one split for a table-in-out literal call.
 *
 * <p>Unlike a regular {@link VgiSplit}, there is no {@code table_function_plan}
 * pagination here at all — a literal call is a single bind-then-exchange
 * interaction with no split enumeration concept (see {@code
 * VgiTableInOutFunction}'s javadoc) — so this exists purely to satisfy Trino's
 * {@code ConnectorSplitManager}/{@code TableFunctionSplitProcessor} SPI, which
 * requires at least one {@link ConnectorSplit} to hand a processor. Every
 * default from {@link ConnectorSplit} (remotely-accessible, no address
 * affinity, standard weight) is exactly right here — nothing about a single
 * in-process exchange has a meaningful "where should this run" answer beyond
 * "wherever Trino schedules the query's split processor."
 *
 * @param handle the already-bound call this split redeems — everything a
 *        {@code VgiTableInOutSplitProcessor} needs to run {@code init()} +
 *        one exchange turn
 */
public record VgiTableInOutSplit(VgiTableInOutFunctionHandle handle) implements ConnectorSplit {
}
