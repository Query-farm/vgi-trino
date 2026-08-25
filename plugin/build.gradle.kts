// Copyright 2026 Query Farm LLC - https://query.farm

val trinoVersion = "483"

dependencies {
    // Trino loads trino-spi (and a handful of adjacent packages) from its own
    // shared classloader, never from a plugin's own lib/ directory — bundling
    // it would be redundant at best and a classloader-identity mismatch at
    // worst, so it's compileOnly here and left out of the plugin distribution
    // (see the packaging task, Phase 7).
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // farm.query:vgi is the VGI client SDK: RpcConnection.proxy(VgiService.class),
    // the protocol records, and the client.* pushdown/projection encoders. Built
    // from source via the composite build in ../settings.gradle.kts so this
    // module sees the table_function_plan client additions.
    implementation("farm.query:vgi:0.27.0")

    // jackson-module-blackbird: Trino's OWN task-update wire protocol carries
    // this connector's ConnectorSplit/ConnectorTableHandle/etc. as polymorphic
    // Jackson payloads, and generates an OPTIMIZED (blackbird) deserializer for
    // each concrete type THE FIRST TIME it needs one — including this
    // connector's own record types, whose classloader is this plugin's, not
    // Trino's. Blackbird's generated deserializer needs its OWN support
    // classes (e.g. CreatorOptimizer) visible from THAT SAME classloader, so
    // without this dependency, deserializing ANY value of one of this
    // connector's own SPI types crashes the coordinator with
    // `NoClassDefFoundError: com.fasterxml.jackson.module.blackbird.deser.
    // CreatorOptimizer` — reproduced against a real `trinodb/trino:483` image
    // (docker/docker-compose.yml) the first time a query actually redeemed a
    // VGI split. See the Jackson version-forcing below for why the version
    // matters too, not just blackbird's presence.
    implementation("com.fasterxml.jackson.module:jackson-module-blackbird:2.22.1")

    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation("io.trino:trino-testing:$trinoVersion")
    testImplementation("io.trino:trino-main:$trinoVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// A DistributedQueryRunner-based test installs this plugin's Plugin instance
// directly (installPlugin(Plugin), not a plugin directory), so it shares ONE
// classpath with the Trino server code under test rather than the isolated
// per-plugin classloader a real deployment gets (see the packaging task,
// Phase 7). Trino 483 resolves Netty up to 4.2.x; arrow-memory-netty-buffer-
// patch:18.1.0 pokes at Netty's pooled-allocator internals and was built
// against the 4.1.11x series, so the two disagree on layout under one
// classloader (ClassCastException: PooledDirectByteBuf -> PooledUnsafeDirect-
// ByteBuf) even though nothing here is actually version-conflicted by
// Gradle's own resolution. Force the test classpath back to the version the
// patch was built against — real deployments never see this, since the
// plugin's Arrow/Netty never share a loader with Trino's.
configurations.testRuntimeClasspath {
    resolutionStrategy {
        force("io.netty:netty-common:4.1.114.Final")
        force("io.netty:netty-buffer:4.1.114.Final")
    }
}

// Align every Jackson artifact on Trino 483's own version (2.22.1/2.22),
// across BOTH classpaths — arrow-vector:18.1.0 otherwise pulls in 2.18.2,
// and the blackbird dependency above is 2.22.1-only bytecode, so leaving the
// two unaligned would put TWO different Jackson versions in the same plugin
// directory (whichever the JVM happens to resolve first at each callsite,
// not a version anyone chose). One consistent version, matching the host
// Trino server's own, plus blackbird above, is what fixed the
// CreatorOptimizer crash end to end against a real trinodb/trino:483 image.
listOf(configurations.runtimeClasspath, configurations.testRuntimeClasspath).forEach {
    it.configure {
        resolutionStrategy {
            force(
                "com.fasterxml.jackson.core:jackson-annotations:2.22",
                "com.fasterxml.jackson.core:jackson-core:2.22.1",
                "com.fasterxml.jackson.core:jackson-databind:2.22.1",
                "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.22.1",
                "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.22.1")
        }
    }
}

// Assemble this connector's jar plus its runtime dependencies (Arrow, Netty,
// Jackson, ...) into build/plugin/vgi/ — the flat-directory-of-jars layout
// Trino's PluginManager expects at plugin/vgi/ under its installation. Every
// plugin gets its own isolated classloader built from exactly this directory
// (never a flat classpath shared with Trino's own dependencies or another
// plugin's), which is also why the Netty version clash the test suite works
// around never occurs in a real deployment. trino-spi is compileOnly and so
// is correctly absent here — Trino serves it from its own shared classloader.
//
// Jackson IS needed here, and deliberately NOT trimmed to just what THIS
// connector's own code calls: farm.query.vgirpc.ServiceIntrospector needs
// jackson-databind directly (confirmed by trying to exclude it entirely —
// NoClassDefFoundError on ByteBufferBackedInputStream at catalog-creation
// time), and Trino's OWN task-update wire protocol needs blackbird
// resolvable from THIS SAME classloader to deserialize this connector's own
// SPI types (see the dependency's own comment above). Trino's
// PluginClassLoader is fully isolated — it does not delegate Jackson
// resolution to Trino's own copy at all (same exclude-and-observe test) — so
// a plugin that needs any Jackson functionality has to bring a complete,
// version-consistent copy, which is exactly what the version-forcing above
// and this jackson-module-blackbird dependency provide.
tasks.register<Copy>("assemblePluginDir") {
    group = "distribution"
    description = "Assemble the connector into build/plugin/vgi/, the layout Trino's plugin loader expects."
    from(tasks.named("jar"))
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("plugin/vgi"))
}

tasks.named("assemble") {
    dependsOn("assemblePluginDir")
}
