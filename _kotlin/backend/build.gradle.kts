import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.3.20"
    kotlin("plugin.spring") version "2.3.20"
    kotlin("plugin.jpa") version "2.3.20"
    id("org.springframework.boot") version "4.0.7"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.onyx.foss"
version = "0.1.0"

// Hibernate 7.3+ includes native Jackson 3 support for @JdbcTypeCode(SqlTypes.JSON) mapping
extra["hibernate.version"] = "7.3.13.Final"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_25
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

repositories { mavenCentral() }

dependencies {
    implementation(platform("io.modelcontextprotocol.sdk:mcp-bom:2.0.1"))
    implementation("io.modelcontextprotocol.sdk:mcp-core")
    implementation("io.modelcontextprotocol.sdk:mcp-json-jackson3")
    implementation(platform("org.springframework.ai:spring-ai-bom:2.0.1"))
    implementation("org.springframework.ai:spring-ai-starter-vector-store-opensearch")
    implementation("org.springframework.ai:spring-ai-rag")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.apache.tika:tika-core:3.1.0")
    implementation("org.apache.tika:tika-parsers-standard-package:3.1.0")
    runtimeOnly("org.postgresql:postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testRuntimeOnly("com.h2database:h2:2.3.232")
    testImplementation("org.testcontainers:junit-jupiter:1.20.6")
    testImplementation(kotlin("test"))
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("com.squareup.okhttp3:okhttp-tls:4.12.0")
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
