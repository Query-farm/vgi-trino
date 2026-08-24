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
