# Copyright 2026 Query Farm LLC - https://query.farm
#
# Bakes the VGI connector into a Trino server image. Build the plugin
# distribution first (from the repo root):
#
#   ./gradlew :plugin:assemblePluginDir
#   docker build -f docker/Dockerfile -t vgi-trino .
#
# The resulting image needs a catalog properties file mounted or baked in at
# /etc/trino/catalog/<name>.properties — see docker/docker-compose.yml for a
# worked example, or dev/run-local-trino.sh for the non-Docker equivalent.

FROM trinodb/trino:483

# The plugin directory assembled by `:plugin:assemblePluginDir`: this
# connector's jar plus its runtime dependencies (Arrow, Netty, Jackson, ...),
# deliberately NOT including trino-spi — Trino serves that from its own
# shared classloader, and every plugin's jars get an isolated classloader
# built from exactly this directory.
COPY plugin/build/plugin/vgi /usr/lib/trino/plugin/vgi
