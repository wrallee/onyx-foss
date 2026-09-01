package com.onyx.foss.kotlin.service

import com.onyx.foss.kotlin.api.RerankCandidate
import com.onyx.foss.kotlin.api.RerankCandidatesRequest
import com.onyx.foss.kotlin.config.OnyxProperties
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient
import java.util.concurrent.TimeUnit

class RerankingServiceTest {
    private val server = MockWebServer()

    @AfterEach
    fun close() = server.shutdown()

    @Test
    fun `sorts candidates by model score and preserves original index`() {
        server.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("""{"scores":[0.1,0.9]}"""),
        )
        server.start()
        val service = service(fallback = true)

        val response = service.rerank(request())

        assertTrue(response.reranked)
        assertEquals(listOf("second", "first"), response.candidates.map { it.id })
        assertEquals(listOf(1, 0), response.candidates.map { it.originalIndex })
        val recorded = server.takeRequest()
        assertEquals("/encoder/cross-encoder-scores", recorded.path)
        assertTrue(recorded.body.readUtf8().contains("Alibaba-NLP/gte-multilingual-reranker-base"))
    }

    @Test
    fun `keeps retrieval order when reranker fails`() {
        server.enqueue(MockResponse().setResponseCode(503))
        server.start()

        val response = service(fallback = true).rerank(request())

        assertFalse(response.reranked)
        assertEquals(listOf("first", "second"), response.candidates.map { it.id })
        assertTrue(response.warning!!.startsWith("Reranker unavailable:"))
    }

    @Test
    fun `read timeout applies to reranker response`() {
        server.enqueue(
            MockResponse()
                .setHeadersDelay(150, TimeUnit.MILLISECONDS)
                .setHeader("Content-Type", "application/json")
                .setBody("""{"scores":[0.1,0.9]}"""),
        )
        server.start()

        val response = service(fallback = true, readTimeoutMs = 50).rerank(request())

        assertFalse(response.reranked)
        assertTrue(response.warning!!.startsWith("Reranker unavailable:"))
    }

    private fun service(fallback: Boolean, readTimeoutMs: Long = 600_000) = RerankingService(
        properties = OnyxProperties(
            modelServer = OnyxProperties.ModelServer(
                baseUrl = server.url("/").toString(),
                rerankerEnabled = true,
                connectTimeoutMs = 30_000,
                readTimeoutMs = readTimeoutMs,
                rerankerFallbackOnError = fallback,
            ),
        ),
        clientBuilder = WebClient.builder(),
    )

    private fun request() = RerankCandidatesRequest(
        query = "query",
        candidates = listOf(
            RerankCandidate("first", content = "first document", retrievalScore = 2.0),
            RerankCandidate("second", content = "second document", retrievalScore = 1.0),
        ),
    )
}
