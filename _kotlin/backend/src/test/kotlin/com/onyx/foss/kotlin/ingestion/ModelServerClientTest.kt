package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.config.OnyxProperties
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClient
import java.util.concurrent.TimeUnit

class ModelServerClientTest {
    @Test
    fun readTimeoutAppliesToEmbeddingResponse() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse()
                .setHeadersDelay(150, TimeUnit.MILLISECONDS)
                .setHeader("Content-Type", "application/json")
                .setBody("""{"embeddings":[[0.1]]}"""),
        )
        server.start()
        val client = ModelServerClient(
            properties = OnyxProperties(
                modelServer = OnyxProperties.ModelServer(
                    baseUrl = server.url("/").toString(),
                    modelName = "test-model",
                    connectTimeoutMs = 1_000,
                    readTimeoutMs = 50,
                ),
            ),
            clientBuilder = WebClient.builder(),
        )

        assertThrows(WebClientRequestException::class.java) { client.embed(listOf("text")) }
    }
}
