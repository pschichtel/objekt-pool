plugins {
    kotlin("jvm")
    id("org.jlleitschuh.gradle.ktlint")
    `java-library`
    `maven-publish`
    jacoco
    signing
}

java.sourceCompatibility = JavaVersion.VERSION_1_8
java.targetCompatibility = JavaVersion.VERSION_1_8

group = "tel.schich"
version = "0.1.0"

repositories {
    mavenCentral()
}

val KOTLIN_VERSION: String by project
val KOTLIN_COROUTINES_VERSION: String by project
val SL4J_VERSION: String by project
val KOTLIN_LOGGING_VERSION: String by project
val JACOCO_VERSION: String by project

val JUNIT_VERSION: String by project
val ASSERTJ_VERSION: String by project
val MOCKK_VERSION: String by project
val LOGBACK_VERSION: String by project
val AWAITILITY_VERSION: String by project

dependencies {
    api("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$KOTLIN_VERSION")
    api("org.jetbrains.kotlin:kotlin-reflect:$KOTLIN_VERSION")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$KOTLIN_COROUTINES_VERSION")
    implementation("org.slf4j:slf4j-api:$SL4J_VERSION")
    implementation("io.github.oshai:kotlin-logging-jvm:$KOTLIN_LOGGING_VERSION")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:$KOTLIN_COROUTINES_VERSION")
    testImplementation("junit:junit:$JUNIT_VERSION")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit:$KOTLIN_VERSION")
    testImplementation("org.assertj:assertj-core:$ASSERTJ_VERSION")
    testImplementation("io.mockk:mockk:$MOCKK_VERSION")
    testImplementation("org.awaitility:awaitility-kotlin:$AWAITILITY_VERSION")
    testImplementation("ch.qos.logback:logback-classic:$LOGBACK_VERSION")
}

tasks {
    compileKotlin {
        kotlinOptions.suppressWarnings = true
        kotlinOptions.jvmTarget = "1.8"
    }

    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }

    jacoco {
        toolVersion = JACOCO_VERSION
    }

    register<JacocoReport>("codeCoverageReport") {
        dependsOn(test)

        executionData.setFrom(
            fileTree(project.rootDir.absolutePath) {
                include("**/build/jacoco/*.exec")
            }
        )

        reports {
            xml.required.set(true)
            xml.outputLocation.set(project.layout.buildDirectory.file("reports/jacoco/report.xml"))
            html.required.set(false)
            csv.required.set(false)
        }

        subprojects {
            sourceSets(sourceSets.main.get())
        }
    }

    check {
        dependsOn(ktlintCheck)
    }

    test {
        jvmArgs = listOf(
            "-Dio.netty.leakDetection.level=PARANOID"
        )
    }
}
