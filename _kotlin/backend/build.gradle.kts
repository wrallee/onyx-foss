import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.spring") version "2.1.20"
    kotlin("plugin.jpa") version "2.1.20"
    id("org.springframework.boot") version "3.4.5"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.onyx.foss"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_21
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

repositories { mavenCentral() }

dependencies {
    implementation(platform("io.modelcontextprotocol.sdk:mcp-bom:2.0.1"))
    implementation("io.modelcontextprotocol.sdk:mcp-core")
    implementation("io.modelcontextprotocol.sdk:mcp-json-jackson2")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.apache.tika:tika-core:3.1.0")
    implementation("org.apache.tika:tika-parsers-standard-package:3.1.0")
    runtimeOnly("org.postgresql:postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("com.h2database:h2")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation(kotlin("test"))
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}

tasks.withType<Test> {
    useJUnitPlatform()
    maxParallelForks = 1
    maxHeapSize = "768m"
    jvmArgs("-XX:MaxMetaspaceSize=256m")
    systemProperty("api.version", "1.40")
}

tasks.test {
    exclude("**/OpenSearchIndexerIntegrationTest*")
    useJUnitPlatform {
        excludeTags("opensearch-integration")
    }
}

tasks.register<Test>("opensearchIntegrationTest") {
    description = "Runs tests against the shared OpenSearch container."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("opensearch-integration")
    }
    shouldRunAfter(tasks.test)
}
