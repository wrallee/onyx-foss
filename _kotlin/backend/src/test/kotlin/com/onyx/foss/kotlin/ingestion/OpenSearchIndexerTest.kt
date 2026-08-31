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
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody(successfulUpdateResponse()),
            )
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

    @Test
    fun updatesDocumentSetsForOnlyOneConnectorPair() {
        MockWebServer().use { server ->
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody("""{"timed_out":false,"total":2,"updated":0,"noops":2,"version_conflicts":0,"failures":[]}"""),
            )
            server.start()
            val indexer = OpenSearchIndexer(
                OnyxProperties(
                    opensearch = OnyxProperties.OpenSearch(
                        baseUrl = server.url("/").toString().trimEnd('/'),
                        index = "documents",
                    ),
                ),
                WebClient.builder(),
                mapper,
            )

            indexer.updateDocumentSets(7, setOf("one", "two"), listOf("first", "second"))

            val request = server.takeRequest()
            val body = mapper.readTree(request.body.readUtf8())
            assertThat(request.path).isEqualTo("/documents/_update_by_query?refresh=true&conflicts=proceed")
            assertThat(body.path("query").path("bool").path("filter").first().path("term").path("cc_pair_id").asLong())
                .isEqualTo(7)
            assertThat(body.path("query").path("bool").path("filter").get(1).path("terms").path("source_document_id").map { it.asText() })
                .containsExactlyInAnyOrder("one", "two")
            assertThat(body.path("script").path("params").path("document_sets").map { it.asText() })
                .containsExactly("first", "second")
        }
    }

    @Test
    fun rejectsTimedOutFailedAndIncompleteAclUpdates() {
        listOf(
            """{"timed_out":true,"total":1,"updated":1,"noops":0,"version_conflicts":0,"failures":[]}""",
            """{"timed_out":false,"total":1,"updated":1,"noops":0,"version_conflicts":0,"failures":[{"cause":"failed"}]}""",
            """{"timed_out":false,"total":0,"updated":0,"noops":0,"version_conflicts":0,"failures":[]}""",
            """{"timed_out":false,"total":1,"updated":0,"noops":0,"version_conflicts":0,"failures":[]}""",
        ).forEach { response ->
            MockWebServer().use { server ->
                server.enqueue(
                    MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(response),
                )
                server.start()
                val indexer = OpenSearchIndexer(
                    OnyxProperties(
                        opensearch = OnyxProperties.OpenSearch(
                            baseUrl = server.url("/").toString().trimEnd('/'),
                            index = "documents",
                        ),
                    ),
                    WebClient.builder(),
                    mapper,
                )

                org.junit.jupiter.api.assertThrows<IllegalStateException> {
                    indexer.updateAccess(7, mapOf("one" to ExternalAccess(isPublic = false)))
                }
            }
        }
    }

    private fun successfulUpdateResponse(): String =
        """{"timed_out":false,"total":1,"updated":1,"noops":0,"version_conflicts":0,"failures":[]}"""
}
