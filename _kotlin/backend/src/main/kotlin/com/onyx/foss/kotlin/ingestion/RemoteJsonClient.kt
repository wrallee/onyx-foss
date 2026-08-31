package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

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
    private val clientBuilder: WebClient.Builder,
) {
    private companion object {
        const val MAX_RESPONSE_BYTES = 16 * 1024 * 1024
    }

    fun get(base: String, path: String, headers: Map<String, String>): JsonNode =
        getResponse(base, path, headers).body

    fun getResponse(base: String, path: String, headers: Map<String, String>): RemoteJsonResponse =
        configuredClient().get()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .toEntity(JsonNode::class.java)
            .map { response ->
                RemoteJsonResponse(
                    response.body ?: error("Remote connector returned an empty response"),
                    response.headers,
                )
            }
            .block() ?: error("Remote connector returned an empty response")

    fun post(base: String, path: String, headers: Map<String, String>, body: Any): JsonNode =
        clientBuilder.clone().codecs { codecs ->
            codecs.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES)
        }.build().post()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .bodyValue(body)
            .retrieve()
            .bodyToMono(JsonNode::class.java)
            .block() ?: error("Remote connector returned an empty response")

    fun getBytes(base: String, path: String, headers: Map<String, String>): ByteArray =
        configuredClient().get()
            .uri(URI.create(base.trimEnd('/') + path))
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .bodyToMono(ByteArray::class.java)
            .block() ?: error("Remote connector returned an empty response")

    fun postText(base: String, path: String, headers: Map<String, String>, body: Any): RemoteTextResponse =
        configuredClient().post()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.ALL)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .bodyValue(body)
            .exchangeToMono { response ->
                response.bodyToMono(String::class.java).defaultIfEmpty("").map { responseBody ->
                    RemoteTextResponse(
                        response.statusCode().value(),
                        response.headers().contentType().map(MediaType::toString).orElse(null),
                        responseBody,
                    )
                }
            }
            .block() ?: error("Remote connector returned an empty response")

    private fun configuredClient() = clientBuilder.clone().codecs { codecs ->
        codecs.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES)
    }.build()
}
