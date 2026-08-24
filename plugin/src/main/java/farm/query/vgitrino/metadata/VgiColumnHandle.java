// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.metadata;

import io.trino.spi.connector.ColumnHandle;

/**
 * One column of a {@link VgiTableHandle}: its name and its position in the
 * table's Arrow schema — the index VGI's {@code projection_ids} pushdown
 * identifies it by.
 *
 * <p>Deliberately does NOT carry a Trino {@code Type}: {@code Type} isn't
 * Jackson-serialisable on its own (it needs a connector-wide
 * {@code TypeManager} to resolve a signature back), and there's no need to
 * carry one — anywhere this column's type matters, its table handle's
 * {@link VgiTableHandle#outputSchema()} plus this {@code ordinal} re-derive it
 * via {@link farm.query.vgitrino.types.VgiTypeMapping#toTrinoType}.
 *
 * @param name the column name
 * @param ordinal this column's index in the table's Arrow schema
 */
public record VgiColumnHandle(String name, int ordinal) implements ColumnHandle {
}
