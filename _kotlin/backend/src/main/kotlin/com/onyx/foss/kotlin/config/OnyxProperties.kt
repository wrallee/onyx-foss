package com.onyx.foss.kotlin.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("onyx")
data class OnyxProperties(
    val multiTenant: Boolean = true,
    val crypto: Crypto = Crypto(),
    val storage: Storage = Storage(),
    val modelServer: ModelServer = ModelServer(),
    val scheduler: Scheduler = Scheduler(),
    val worker: Worker = Worker(),
) {
    data class Crypto(val key: String = "")
    data class Storage(val root: String = "/var/lib/onyx/files")
    data class ModelServer(
        val baseUrl: String = "http://model-server:9000",
        val modelName: String = "",
        val embeddingDimension: Int = 768,
        val maxContextLength: Int = 512,
        val normalizeEmbeddings: Boolean = true,
        val searchCandidates: Int = 50,
        val connectTimeoutMs: Long = 30_000,
        val readTimeoutMs: Long = 600_000,
        val embedMaxRetries: Int = 2,
        val embedRetryInitialBackoffMs: Long = 2_000,
    )
    data class Scheduler(
        val pollDelayMs: Long = 15_000,
    )
    data class Worker(
        val enabled: Boolean = false,
        val pollDelayMs: Long = 5000,
        val heartbeatIntervalMs: Long = 15_000,
    )
}
