package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

@Service
class RemoteJsonClient(
    private val clientBuilder: WebClient.Builder,
) {
    private companion object {
        const val MAX_RESPONSE_BYTES = 16 * 1024 * 1024
    }

    fun get(base: String, path: String, headers: Map<String, String>): JsonNode =
        clientBuilder.clone().codecs { codecs ->
            codecs.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES)
        }.build().get()
            .uri(URI.create(base.trimEnd('/') + path))
            .accept(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .bodyToMono(JsonNode::class.java)
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
}
