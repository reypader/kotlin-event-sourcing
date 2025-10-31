plugins {
    alias(libs.plugins.kotlin.serialization)
}

description = "Event Sourcing SPI - Core interfaces and contracts"

dependencies {
    // Serialization
    api(libs.kotlinx.json)

    // Coroutines
    api(libs.kotlinx.coroutines.reactor)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.mockk)
}
