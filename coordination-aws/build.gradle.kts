description = "AWS Cloud Map-based aggregate coordination"

dependencies {
    // SPI dependency
    api(project(":spi"))

    // AWS Cloud Map
    implementation(libs.cloud.map)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
}
