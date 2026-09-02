package com.onyx.foss.kotlin.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("onyx")
data class OnyxProperties(
    val crypto: Crypto = Crypto(),
    val storage: Storage = Storage(),
    val modelServer: ModelServer = ModelServer(),
    val opensearch: OpenSearch = OpenSearch(),
    val worker: Worker = Worker(),
) {
    data class Crypto(val key: String = "")
    data class Storage(val root: String = "/var/lib/onyx/files")
    data class ModelServer(
        val baseUrl: String = "http://model-server:9000",
        val modelName: String = "",
        val maxContextLength: Int = 512,
        val normalizeEmbeddings: Boolean = true,
        val rerankerEnabled: Boolean = true,
        val rerankerModelName: String = "Alibaba-NLP/gte-multilingual-reranker-base",
        val rerankerMaxDocuments: Int = 100,
        val connectTimeoutMs: Long = 30_000,
        val readTimeoutMs: Long = 600_000,
        val rerankerFallbackOnError: Boolean = true,
        val embedMaxRetries: Int = 2,
        val embedRetryInitialBackoffMs: Long = 2_000,
    )
    data class OpenSearch(
        val baseUrl: String = "http://opensearch:9200",
        val index: String = "onyx-kotlin-chunks",
        val username: String = "",
        val password: String = "",
        val verifyCerts: Boolean = false,
    )
    data class Worker(
        val enabled: Boolean = false,
        val pollDelayMs: Long = 1000,
        val heartbeatIntervalMs: Long = 15_000,
    )
}
