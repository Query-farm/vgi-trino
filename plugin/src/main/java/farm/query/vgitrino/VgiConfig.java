// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino;

import java.util.Map;

/**
 * Per-catalog configuration, parsed from the {@code etc/catalog/*.properties}
 * file Trino hands {@link VgiConnectorFactory#create}.
 *
 * <p>One Trino catalog is one VGI {@code ATTACH} — the same granularity
 * DuckDB uses. Multiple VGI-backed Trino catalogs are just multiple
 * properties files with {@code connector.name=vgi} and a different
 * {@code vgi.location}.
 *
 * @param location the worker to attach: a bare shell command (subprocess
 *        transport), {@code unix:///path/to.sock}, or {@code tcp://host:port}.
 *        {@code http(s)://} is not yet supported — see the connector README.
 * @param catalogName the VGI-side catalog name to request from
 *        {@code catalog_attach} — one of the names {@code catalog_catalogs()}
 *        advertises (e.g. {@code "example"} for the reference fixture worker),
 *        NOT the Trino catalog name. Trino's own catalog name (the properties
 *        filename) is a separate, purely local alias the worker never sees —
 *        exactly the {@code alias}/{@code name} split in DuckDB's own
 *        {@code ATTACH 'name' AS alias (TYPE vgi, ...)}
 * @param connections how many independent worker connections to pool. VGI's
 *        RPC is lockstep per connection — one request in flight at a time —
 *        so this is also the ceiling on splits Trino can redeem concurrently
 *        against this catalog
 * @param targetSplitBytes requested split size passed to {@code table_function_plan}
 *        as {@code target_split_bytes}, or {@code null} to let the worker decide
 * @param minSplits parallelism floor passed as {@code min_splits}, or {@code null}
 * @param maxSplitsPerResponse pagination cap per {@code table_function_plan} call
 */
public record VgiConfig(
        String location,
        String catalogName,
        int connections,
        Long targetSplitBytes,
        Long minSplits,
        int maxSplitsPerResponse) {

    /** Default connection-pool size when {@code vgi.connections} is unset. */
    public static final int DEFAULT_CONNECTIONS = 4;

    /** Default {@code max_splits_per_response} when {@code vgi.max-splits-per-response} is unset. */
    public static final int DEFAULT_MAX_SPLITS_PER_RESPONSE = 1000;

    /**
     * Parse the catalog properties map Trino provides.
     *
     * @param properties the catalog's {@code connector.*}-stripped property map
     * @return the parsed config
     * @throws IllegalArgumentException if {@code vgi.location} is missing
     */
    public static VgiConfig fromProperties(Map<String, String> properties) {
        String location = properties.get("vgi.location");
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException(
                    "vgi.location is required (a subprocess command, unix://path, or tcp://host:port)");
        }
        String catalogName = properties.get("vgi.catalog-name");
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException(
                    "vgi.catalog-name is required: the VGI-side catalog to attach "
                            + "(see catalog_catalogs() on the worker), not the Trino catalog name");
        }
        int connections = parseInt(properties.get("vgi.connections"), DEFAULT_CONNECTIONS);
        Long targetSplitBytes = parseLongOrNull(properties.get("vgi.target-split-size-bytes"));
        Long minSplits = parseLongOrNull(properties.get("vgi.min-splits"));
        int maxSplitsPerResponse = parseInt(
                properties.get("vgi.max-splits-per-response"), DEFAULT_MAX_SPLITS_PER_RESPONSE);
        return new VgiConfig(location, catalogName, connections, targetSplitBytes, minSplits, maxSplitsPerResponse);
    }

    private static int parseInt(String value, int fallback) {
        return value == null ? fallback : Integer.parseInt(value.trim());
    }

    private static Long parseLongOrNull(String value) {
        return value == null ? null : Long.parseLong(value.trim());
    }
}
