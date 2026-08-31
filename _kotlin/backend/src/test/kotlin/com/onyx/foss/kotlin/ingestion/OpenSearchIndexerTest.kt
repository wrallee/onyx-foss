package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient

class OpenSearchIndexerTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun deletesOnlySelectedDocumentIds() = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(200).setBody("{}"))
        server.start()
        val properties = OnyxProperties(
            opensearch = OnyxProperties.OpenSearch(
                baseUrl = server.url("/").toString().trimEnd('/'),
                index = "documents",
            ),
        )
        val indexer = OpenSearchIndexer(properties, WebClient.builder(), mapper)

        indexer.deleteDocuments(7, setOf("one", "two"))

        val request = server.takeRequest()
        val body = mapper.readTree(request.body.readUtf8())
        assertThat(request.path).isEqualTo("/documents/_delete_by_query?refresh=true")
        assertThat(body.path("query").path("bool").path("filter").first().path("term").path("cc_pair_id").asLong())
            .isEqualTo(7)
        assertThat(body.path("query").path("bool").path("filter").get(1).path("terms").path("source_document_id").map { it.asText() })
            .containsExactlyInAnyOrder("one", "two")
    }
}
