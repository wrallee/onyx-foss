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
    )
    data class OpenSearch(val baseUrl: String = "http://opensearch:9200", val index: String = "onyx-kotlin-chunks")
    data class Worker(val enabled: Boolean = false, val pollDelayMs: Long = 1000)
}
