#!/usr/bin/env bash
# Copyright 2026 Query Farm LLC - https://query.farm
#
# Local Trino server + this connector, for the fast interactive dev loop
# (rebuild the plugin, restart, query). Downloads/caches the Trino server
# tarball once, lays out a minimal etc/, symlinks the freshly assembled
# plugin jars into plugin/vgi/, writes a catalog properties file from
# VGI_LOCATION/VGI_CATALOG_NAME, and starts the server in the foreground.
#
# Usage:
#   VGI_LOCATION="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
#   VGI_CATALOG_NAME=example \
#       dev/run-local-trino.sh
#
# Then, in another terminal:
#   trino --server localhost:8080 --catalog vgi_example --schema data
#   (no `trino` CLI installed? use the coordinator's own client jar:
#    curl -o trino-cli.jar https://repo1.maven.org/maven2/io/trino/trino-cli/483/trino-cli-483-executable.jar
#    chmod +x trino-cli.jar && ./trino-cli.jar --server localhost:8080)

set -euo pipefail

TRINO_VERSION="483"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${TRINO_CACHE_DIR:-$HOME/.cache/vgi-trino-dev}"
RUN_DIR="$REPO_ROOT/dev/run"
TARBALL="trino-server-${TRINO_VERSION}.tar.gz"

VGI_LOCATION="${VGI_LOCATION:?set VGI_LOCATION to the worker command/unix:///tcp:// to attach}"
VGI_CATALOG_NAME="${VGI_CATALOG_NAME:?set VGI_CATALOG_NAME to the VGI-side catalog to attach}"
TRINO_CATALOG_NAME="${TRINO_CATALOG_NAME:-vgi_example}"

mkdir -p "$CACHE_DIR" "$RUN_DIR"

if [ ! -d "$CACHE_DIR/trino-server-${TRINO_VERSION}" ]; then
    echo "Downloading Trino server ${TRINO_VERSION} (first run only)..."
    curl -sL -o "$CACHE_DIR/$TARBALL" \
        "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
    tar -xzf "$CACHE_DIR/$TARBALL" -C "$CACHE_DIR"
fi
TRINO_HOME="$CACHE_DIR/trino-server-${TRINO_VERSION}"

echo "Building the connector plugin..."
(cd "$REPO_ROOT" && ./gradlew :plugin:assemblePluginDir -q)

rm -rf "$RUN_DIR/etc" "$TRINO_HOME/plugin/vgi"
mkdir -p "$RUN_DIR/etc/catalog" "$RUN_DIR/data"
ln -sfn "$REPO_ROOT/plugin/build/plugin/vgi" "$TRINO_HOME/plugin/vgi"

cat > "$RUN_DIR/etc/node.properties" <<EOF
node.environment=vgi-dev
node.id=vgi-dev-node
node.data-dir=$RUN_DIR/data
EOF

cat > "$RUN_DIR/etc/jvm.config" <<'EOF'
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-modules=jdk.incubator.vector
--enable-native-access=ALL-UNNAMED
EOF

cat > "$RUN_DIR/etc/config.properties" <<'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF

cat > "$RUN_DIR/etc/log.properties" <<'EOF'
io.trino=INFO
EOF

cat > "$RUN_DIR/etc/catalog/${TRINO_CATALOG_NAME}.properties" <<EOF
connector.name=vgi
vgi.location=$VGI_LOCATION
vgi.catalog-name=$VGI_CATALOG_NAME
vgi.connections=4
EOF

echo "Starting Trino at http://localhost:8080 (catalog: $TRINO_CATALOG_NAME) ..."
exec "$TRINO_HOME/bin/launcher" run --etc-dir "$RUN_DIR/etc"
