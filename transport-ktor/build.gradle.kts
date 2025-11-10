description = "Transport implementation using Ktor"

plugins {
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    // Core dependency
    api(project(":core"))

    // Core dependency
    implementation(libs.kotlinx.json)

    // SLF4J
    implementation(libs.slf4j.api)

    // Ktor
    implementation(libs.ktor.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.config.yaml)
    implementation(libs.ktor.protobuf)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.client.cio)
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.content.negotiation)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.mockk)
    testImplementation("ch.qos.logback:logback-classic:1.5.20")
}
