description = "Relational database implementation using R2DBC"

dependencies {
    // SPI dependency
    api(project(":spi"))

    // R2DBC
    implementation(libs.r2dbc.pool)
    implementation(libs.reactor.kotlin.extensions)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.r2dbc.h2)
    testImplementation(libs.reactor.test)
    testImplementation(libs.kotlinx.coroutines.test)
}
