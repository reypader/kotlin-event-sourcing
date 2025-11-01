description = "AWS Cloud Map-based aggregate coordination"

dependencies {
    // Core dependency
    api(project(":core"))

    // AWS Cloud Map
    implementation(libs.cloud.map)

    // SLF4J
    implementation(libs.slf4j.api)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
}
