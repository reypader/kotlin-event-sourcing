plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

group = "com.rmpader"
version = "1.0-SNAPSHOT"
description = "kotlin-event-sourcing"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kotlinx.json)


    // Reactor
    implementation(libs.kotlinx.coroutines.reactor)
    implementation(libs.reactor.kotlin.extensions)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit5"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}