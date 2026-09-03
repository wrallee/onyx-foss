package com.onyx.foss.kotlin.ingestion

import tools.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient
import org.springframework.web.client.RestClientResponseException
import java.util.concurrent.TimeUnit

class ModelServerClientTest {
    @Test
    fun `query embedding sends query text type`() {
        MockWebServer().use { server ->
            server.enqueue(
                MockResponse().setHeader("Content-Type", "application/json")
                    .setBody("""{"embeddings":[[0.1,0.2]]}"""),
            )
            server.start()
            val client = ModelServerClient(
                OnyxProperties(
                    modelServer = OnyxProperties.ModelServer(
                        baseUrl = server.url("/").toString(),
                        modelName = "embedding-model",
                    ),
                ),
                RestClient.builder(),
            )

            assertThat(client.embedQuery("search terms")).containsExactly(0.1, 0.2)

            val request = server.takeRequest()
            val body = jacksonObjectMapper().readTree(request.body.readUtf8())
            assertThat(request.path).isEqualTo("/encoder/bi-encoder-embed")
            assertThat(body.path("texts").toList().map{ it.asText() }).containsExactly("search terms")
            assertThat(body.path("text_type").asText()).isEqualTo("query")
        }
    }

    @Test
    fun readTimeoutAppliesToEmbeddingResponse() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse()
                .setHeadersDelay(150, TimeUnit.MILLISECONDS)
                .setHeader("Content-Type", "application/json")
                .setBody("""{"embeddings":[[0.1]]}"""),
        )
        server.start()
        val client = client(
            server,
            connectTimeoutMs = 1_000,
            readTimeoutMs = 50,
        )

        assertThrows(ResourceAccessException::class.java) { client.embed(listOf("text")) }
    }

    @Test
    fun `embed retries on a 502 and succeeds once connection recovers`() = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(502))
        server.enqueue(MockResponse().setResponseCode(502))
        server.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("""{"embeddings":[[0.1,0.2]]}"""),
        )
        server.start()

        val result = client(server, embedMaxRetries = 2, embedRetryInitialBackoffMs = 5).embed(listOf("hello"))

        assertEquals(listOf(listOf(0.1, 0.2)), result)
        assertEquals(3, server.requestCount)
    }

    @Test
    fun `embed gives up after exhausting retries`() = MockWebServer().use { server ->
        repeat(3) { server.enqueue(MockResponse().setResponseCode(502)) }
        server.start()

        assertThrows(RestClientResponseException::class.java) {
            client(server, embedMaxRetries = 2, embedRetryInitialBackoffMs = 5).embed(listOf("hello"))
        }
        assertEquals(3, server.requestCount)
    }

    private fun client(
        server: MockWebServer,
        connectTimeoutMs: Long = 1_000,
        readTimeoutMs: Long = 30_000,
        embedMaxRetries: Int = 0,
        embedRetryInitialBackoffMs: Long = 0,
    ) = ModelServerClient(
        properties = OnyxProperties(
            modelServer = OnyxProperties.ModelServer(
                baseUrl = server.url("/").toString(),
                modelName = "test-model",
                connectTimeoutMs = connectTimeoutMs,
                readTimeoutMs = readTimeoutMs,
                embedMaxRetries = embedMaxRetries,
                embedRetryInitialBackoffMs = embedRetryInitialBackoffMs,
            ),
        ),
        clientBuilder = RestClient.builder(),
    )
}
