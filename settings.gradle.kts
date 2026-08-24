// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    // Auto-provision the JDK toolchain when not installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "vgi-trino"

// Composite-include the sibling vgi-java repo so :vgi is built from source —
// this connector depends on client-side additions (TableFunctionPlanRequest,
// the max_splits_per_response plumbing) that have not been released to Maven
// Central yet. Falls back to the published farm.query:vgi artifact if the
// directory isn't present (e.g. CI before vgi-java ships a release with them).
// VGI_JAVA_DIR overrides the path, mirroring vgi-java's own VGI_RPC_JAVA_DIR
// convention for CI layouts where the sibling isn't checked out next door.
val vgiJavaDir = System.getenv("VGI_JAVA_DIR")?.let { file(it) }
    ?: file("../../vgi-java")
if (vgiJavaDir.isDirectory) {
    includeBuild(vgiJavaDir) {
        dependencySubstitution {
            // Coordinates match what vgi-java actually publishes (group=farm.query,
            // see ~/vgi-java/build.gradle.kts).
            substitute(module("farm.query:vgi")).using(project(":vgi"))
        }
    }
}

include("plugin")
