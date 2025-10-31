description = "Kubernetes-based aggregate coordination using Informers"

dependencies {
    // Core dependency
    api(project(":core"))

    // Kubernetes client
    implementation(libs.commons.lang)
    implementation(libs.kubernetes)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
}
