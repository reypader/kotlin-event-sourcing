description = "Relational database implementation using R2DBC"

dependencies {
    // Core dependency
    api(project(":core"))

    // R2DBC
    implementation(libs.r2dbc.pool)
    implementation(libs.reactor.kotlin.extensions)

    // SLF4J
    implementation(libs.slf4j.api)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.r2dbc.h2)
    testImplementation(libs.reactor.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
