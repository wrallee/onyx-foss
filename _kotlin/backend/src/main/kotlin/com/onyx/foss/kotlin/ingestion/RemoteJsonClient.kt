package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.stereotype.Service
import org.springframework.web.client.RestClient
import java.net.URI
import java.time.Duration

data class RemoteTextResponse(
    val statusCode: Int,
    val contentType: String?,
    val body: String,
)

data class RemoteJsonResponse(
    val body: JsonNode,
    val headers: HttpHeaders,
)

@Service
class RemoteJsonClient(
    clientBuilder: RestClient.Builder = RestClient.builder(),
) {
    private val client = clientBuilder.clone()
        .requestFactory(HttpComponentsClientHttpRequestFactory(HttpClients.custom().disableAutomaticRetries().build()))
        .build()

    fun get(base: String, path: String, headers: Map<String, String>): JsonNode =
        getResponse(base, path, headers).body

    fun getResponse(base: String, path: String, headers: Map<String, String>): RemoteJsonResponse {
        val entity = client.get()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .toEntity(JsonNode::class.java)

        return RemoteJsonResponse(
            entity.body ?: error("Remote connector returned an empty response"),
            entity.headers,
        )
    }

    fun post(base: String, path: String, headers: Map<String, String>, body: Any): JsonNode =
        client.post()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .body(body)
            .retrieve()
            .body(JsonNode::class.java)
            ?: error("Remote connector returned an empty response")

    fun getBytes(base: String, path: String, headers: Map<String, String>): ByteArray =
        client.get()
            .uri(URI.create(base.trimEnd('/') + path))
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .body(ByteArray::class.java)
            ?: error("Remote connector returned an empty response")

    fun postText(base: String, path: String, headers: Map<String, String>, body: Any): RemoteTextResponse {
        val response = client.post()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.ALL)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .body(body)
            .exchange { _, clientResponse ->
                val statusCode = clientResponse.statusCode.value()
                val contentType = clientResponse.headers.contentType?.toString()
                val responseBody = clientResponse.body.bufferedReader().use { it.readText() }
                RemoteTextResponse(statusCode, contentType, responseBody)
            }
        return response ?: error("Remote connector returned an empty response")
    }
}

internal val REMOTE_CONNECTOR_TIMEOUT: Duration = Duration.ofSeconds(30)
