// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    java
}

allprojects {
    group = "farm.query"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        // A Trino plugin jar only ever runs inside a Trino server JVM, and
        // Trino 483 requires Java 25 — so build and target 25 directly,
        // unlike vgi-java's 25-build/21-target split (which exists to let a
        // stand-alone worker run on any JVM >= 21).
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(25)
        options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing", "-parameters"))
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Arrow's memory module needs access to java.nio internals (pulled in
        // transitively via farm.query:vgi's client package).
        jvmArgs(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
            // Trino's own server code (BlockEncodingSimdSupport) uses the JDK
            // incubator Vector API, which needs an explicit opt-in even when
            // just running trino-testing's in-process DistributedQueryRunner.
            "--add-modules=jdk.incubator.vector",
        )
        maxHeapSize = "2g"
    }
}
