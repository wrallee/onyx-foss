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
    fun deletesOnlySelectedDocumentIds() {
        MockWebServer().use { server ->
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

    @Test
    fun updatesAclFieldsForOnlyTheSelectedDocuments() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody("{}"))
            server.start()
            val properties = OnyxProperties(
                opensearch = OnyxProperties.OpenSearch(
                    baseUrl = server.url("/").toString().trimEnd('/'),
                    index = "documents",
                ),
            )
            val indexer = OpenSearchIndexer(properties, WebClient.builder(), mapper)
            val access = ExternalAccess(setOf("reader@example.com"), setOf("team-1"), isPublic = false)

            indexer.updateAccess(7, mapOf("one" to access))

            val request = server.takeRequest()
            val body = mapper.readTree(request.body.readUtf8())
            assertThat(request.path).isEqualTo("/documents/_update_by_query?refresh=true&conflicts=proceed")
            assertThat(body.path("query").path("bool").path("filter").first().path("term").path("cc_pair_id").asLong())
                .isEqualTo(7)
            assertThat(body.path("query").path("bool").path("filter").get(1).path("terms").path("source_document_id").map { it.asText() })
                .containsExactly("one")
            val storedAccess = body.path("script").path("params").path("access_by_document").path("one")
            assertThat(storedAccess.path("external_user_emails").map { it.asText() }).containsExactly("reader@example.com")
            assertThat(storedAccess.path("external_user_group_ids").map { it.asText() }).containsExactly("team-1")
            assertThat(storedAccess.path("is_public").asBoolean()).isFalse()
        }
    }
}
