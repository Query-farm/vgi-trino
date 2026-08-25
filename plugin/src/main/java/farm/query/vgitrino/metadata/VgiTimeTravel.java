// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.metadata;

import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;

/**
 * Converts Trino's {@code FOR VERSION/TIMESTAMP AS OF} clause into VGI's own
 * {@code at_unit}/{@code at_value} string pair — the wire fields {@code BindRequest}
 * and the {@code catalog_table_get}/{@code catalog_table_scan_function_get} RPCs
 * already carry (see {@code VgiMetadata}/{@code VgiSplitManager}). The type-dispatch
 * shape (which {@code versionType()}s to accept per {@link
 * io.trino.spi.connector.PointerType}, and how to decode each one's packed/unpacked
 * representation) mirrors Trino's own Iceberg connector — {@code
 * IcebergMetadata.getSnapshotIdFromVersion}/{@code getTemporalSnapshotIdFromVersion}
 * — fetched from Trino's real source rather than guessed, since this repo carries
 * no local Iceberg/Delta connector to crib from directly.
 *
 * <p>The resulting {@code at_unit} strings ({@code "VERSION"}/{@code "TIMESTAMP"})
 * and {@code at_value} formats were checked against the actual reference fixture
 * every existing vgi-trino test already runs against —
 * {@code vgi-python/vgi/_test_fixtures/table/versioned.py}'s {@code
 * resolve_version} reads {@code at_unit} case-insensitively and, for {@code
 * "TIMESTAMP"}, parses only the leading 4-digit year out of {@code at_value} —
 * so the ISO-8601 string this class produces is compatible without any
 * fixture-specific formatting.
 */
final class VgiTimeTravel {

    private VgiTimeTravel() {}

    /** The resolved AT clause: a {@code (unit, value)} pair, both present or this optional empty —
     *  VGI's own worker-side validation (both-or-neither) is satisfied by construction. */
    record AtClause(String atUnit, String atValue) {}

    /**
     * @param startVersion Trino's {@code FOR ... AS OF} range start — VGI has no range concept, so a
     *        present value is rejected outright rather than silently ignored
     * @param endVersion Trino's {@code FOR ... AS OF} point — the only one VGI's single-point AT model
     *        can express
     * @return the resolved AT clause, or empty when neither version is present (a plain, non-time-travel read)
     * @throws TrinoException if {@code startVersion} is present, or either version's type isn't one this
     *         connector knows how to convert
     */
    static Optional<AtClause> resolve(
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                    "VGI time travel supports a single AS OF point (FOR VERSION/TIMESTAMP AS OF), "
                            + "not a version range");
        }
        if (endVersion.isEmpty()) {
            return Optional.empty();
        }
        ConnectorTableVersion version = endVersion.get();
        return Optional.of(switch (version.getPointerType()) {
            case TARGET_ID -> new AtClause("VERSION", targetIdValue(version));
            case TEMPORAL -> new AtClause("TIMESTAMP", temporalValue(version));
        });
    }

    /**
     * {@code FOR VERSION AS OF <expr>}: Trino accepts an integer-family or {@code VARCHAR}-typed
     * expression here — a snapshot id/version number or a named ref/version tag — matching Iceberg's
     * own conversion, generalized to any integer width rather than {@code BIGINT} specifically: a bare
     * small-integer literal like {@code FOR VERSION AS OF 1} types as {@code INTEGER}, not {@code
     * BIGINT} (confirmed by actually running it, not assumed — Iceberg's own snippet only shows the
     * {@code BIGINT} check, which would reject exactly this case). Dispatching on the runtime {@link
     * Number} type rather than enumerating every Trino integer {@code Type} constant handles
     * TINYINT/SMALLINT/INTEGER/BIGINT uniformly, since VGI's {@code at_value} just needs a decimal
     * string, which is unit-agnostic for a plain version number (unlike a timestamp, where the unit —
     * day vs micros vs packed millis+zone — genuinely depends on the source type; see {@link
     * #temporalValue}, which for that reason can't use this same shortcut).
     */
    private static String targetIdValue(ConnectorTableVersion version) {
        Object value = version.getVersion();
        if (value instanceof Number number) {
            return number.toString();
        }
        if (value instanceof Slice slice) {
            return slice.toStringUtf8();
        }
        throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                "Unsupported type for table version: " + version.getVersionType().getDisplayName());
    }

    /** {@code FOR TIMESTAMP AS OF <expr>}: Trino accepts {@code DATE}, {@code TIMESTAMP}, or {@code
     *  TIMESTAMP WITH TIME ZONE}-typed expressions — each has a short (packed {@code long}) and long
     *  (boxed record) representation depending on declared precision; both are handled, matching Iceberg's
     *  own conversion. */
    private static String temporalValue(ConnectorTableVersion version) {
        Type versionType = version.getVersionType();
        Instant instant;
        if (versionType.equals(DateType.DATE)) {
            long epochDay = (Long) version.getVersion();
            instant = LocalDate.ofEpochDay(epochDay).atStartOfDay(ZoneOffset.UTC).toInstant();
        } else if (versionType instanceof TimestampType timestampType) {
            long epochMicros = timestampType.isShort()
                    ? (Long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            instant = Instant.ofEpochSecond(
                    Math.floorDiv(epochMicros, 1_000_000L),
                    Math.floorMod(epochMicros, 1_000_000L) * 1_000L);
        } else if (versionType instanceof TimestampWithTimeZoneType tzType) {
            long epochMillis = tzType.isShort()
                    ? unpackMillisUtc((Long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            instant = Instant.ofEpochMilli(epochMillis);
        } else {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED,
                    "Unsupported type for temporal table version: " + versionType.getDisplayName());
        }
        return instant.toString();
    }
}
