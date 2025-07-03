import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.ktlint) apply false
    `java-library`
    `maven-publish`
    jacoco
    signing
}

java.sourceCompatibility = JavaVersion.VERSION_11
java.targetCompatibility = JavaVersion.VERSION_11

group = "tel.schich"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(libs.kotlin.coroutines)
    implementation(libs.kotlin.logging)
    testImplementation(libs.junit)
    testImplementation(libs.kotlin.test)
    testImplementation(libs.assertj)
    testImplementation(libs.mockk)
    testImplementation(libs.awaitility)
    testImplementation(libs.logback)
}

tasks {
    withType<KotlinCompile>().configureEach {
        compilerOptions {
            jvmTarget = JvmTarget.JVM_11
        }
    }

    jacoco {
        toolVersion = libs.versions.jacoco.get()
    }

    register<JacocoReport>("codeCoverageReport") {
        dependsOn(test)

        executionData.setFrom(
            fileTree(project.rootDir.absolutePath) {
                include("**/build/jacoco/*.exec")
            },
        )

        reports {
            xml.required = true
            xml.outputLocation = project.layout.buildDirectory.file("reports/jacoco/report.xml")
            html.required = false
            csv.required = false
        }

        sourceSets(sourceSets.main.get())
    }

    check {
        // dependsOn(ktlintCheck)
    }

    test {
        jvmArgs =
            listOf(
                "-Dio.netty.leakDetection.level=PARANOID",
            )
    }
}
