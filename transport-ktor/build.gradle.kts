import com.google.protobuf.gradle.id


description = "Transport implementation using Ktor"

plugins {
    alias(libs.plugins.protobuf)
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

    // Protobuf
    implementation(libs.protobuf.kotlin)
//    implementation(libs.protobuf.java.util)

    // Test
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.ktor.server.test.host)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.33.0"
    }
    generateProtoTasks {
        all().forEach { task ->
            task.builtins {
                id("kotlin")
            }
        }
    }
}

sourceSets {
    main {
        kotlin {
            srcDirs(
                "build/generated/source/proto/main/kotlin",
            )
        }
    }
}

configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    filter {
        exclude("**/generated/**")
        exclude { element -> element.file.path.contains("generated") }
    }
}
