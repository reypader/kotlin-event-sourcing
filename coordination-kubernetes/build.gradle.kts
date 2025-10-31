description = "Kubernetes-based aggregate coordination using Informers"

dependencies {
    // SPI dependency
    api(project(":spi"))

    // Kubernetes client
    implementation(libs.kubernetes)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
}
