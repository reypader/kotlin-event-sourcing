plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "kotlin-event-sourcing"

include(
    "core",
    "repository-relational",
    "coordination-kubernetes",
    "coordination-aws",
    "transport-ktor",
)
