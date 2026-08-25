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

# Arrow's off-heap memory access needs `--add-opens` on java.nio — otherwise
# the coordinator crashes at the FIRST catalog load with
# "InaccessibleObjectException: Unable to make field long java.nio.Buffer.address
# accessible", not something a plugin's own isolated classloader can add for
# itself; it's a whole-JVM flag, so it has to land in the base image's own
# jvm.config. `--add-modules=jdk.incubator.vector` is the same story as
# `dev/run-local-trino.sh`'s local (non-Docker) launcher needs — Trino's own
# SIMD block-encoding support requires it too. `--enable-native-access=ALL-UNNAMED`
# is the same category of concern again, for the `launch:` transport's Foreign
# Function & Memory API use (vgi-rpc-java's launcher package, flock(2)/geteuid())
# — a plugin's isolated classloader can't grant itself native access either.
# None of these three showed up until this connector was actually run inside a
# real `trinodb/trino` image; `./gradlew :plugin:test`'s in-process
# DistributedQueryRunner sets its own test JVM args and never exercises this path.
RUN printf '%s\n' \
    '--add-opens=java.base/java.nio=ALL-UNNAMED' \
    '--add-modules=jdk.incubator.vector' \
    '--enable-native-access=ALL-UNNAMED' \
    >> /etc/trino/jvm.config
