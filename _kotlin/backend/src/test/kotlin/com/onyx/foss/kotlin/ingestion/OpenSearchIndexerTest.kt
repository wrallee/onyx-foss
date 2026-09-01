package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class OpenSearchIndexerTest {
    private val mapper = jacksonObjectMapper()
    private val externalWrites = mock(PairExternalWriteFence::class.java).also { fence ->
        doAnswer { invocation -> invocation.getArgument<() -> Unit>(1).invoke() }
            .`when`(fence).withOpenSearchIndex(anyString(), any<() -> Unit>() ?: {})
    }

    @Test
    fun deletesOnlySelectedDocumentIds() {
        MockWebServer().use { server ->
            enqueueKeywordMapping(server)
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody("""{"timed_out":false,"total":2,"deleted":2,"version_conflicts":0,"failures":[]}"""),
            )
            server.start()
            val properties = OnyxProperties(
                opensearch = OnyxProperties.OpenSearch(
                    baseUrl = server.url("/").toString().trimEnd('/'),
                    index = "documents",
                ),
            )
            val indexer = OpenSearchIndexer(properties, WebClient.builder(), mapper, externalWrites)

            indexer.deleteDocuments(7, setOf("one", "two"))

            val request = takeOperationRequest(server)
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
            enqueueKeywordMapping(server)
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
            val indexer = OpenSearchIndexer(properties, WebClient.builder(), mapper, externalWrites)
            val access = ExternalAccess(setOf("reader@example.com"), setOf("team-1"), isPublic = false)

            indexer.updateAccess(7, mapOf("one" to access))

            val request = takeOperationRequest(server)
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
            enqueueKeywordMapping(server)
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
                externalWrites,
            )

            indexer.updateDocumentSets(7, setOf("one", "two"), listOf("first", "second"))

            val request = takeOperationRequest(server)
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
    fun documentSetUpdateTimeoutIsBelowTheLease() {
        assertThat(DOCUMENT_SET_UPDATE_TIMEOUT).isLessThan(Duration.ofHours(1))
    }

    @Test
    fun newChunksStartWithExplicitPrivateAccess() {
        MockWebServer().use { server ->
            enqueueKeywordMapping(server)
            server.enqueue(MockResponse().setResponseCode(200))
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
                externalWrites,
            )

            indexer.upsert(7, "one", 0, "One", "content", null, emptyMap(), listOf(0.1))

            val body = mapper.readTree(takeOperationRequest(server).body.readUtf8())
            assertThat(body.has("external_user_emails")).isTrue()
            assertThat(body.has("external_user_group_ids")).isTrue()
            assertThat(body.has("is_public")).isTrue()
            assertThat(body.path("external_user_emails")).isEmpty()
            assertThat(body.path("external_user_group_ids")).isEmpty()
            assertThat(body.path("is_public").asBoolean()).isFalse()
        }
    }

    @Test
    fun existingIndexGetsMissingTypedSourceMetadataMappings() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody("""{"documents":{"mappings":{"properties":{"source_document_id":{"type":"keyword"}}}}}"""),
            )
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(MockResponse().setResponseCode(200))
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
                externalWrites,
            )

            indexer.upsert(
                7,
                "one",
                0,
                "One",
                "content",
                null,
                emptyMap(),
                listOf(0.1),
                primaryOwners = listOf("owner@example.com"),
            )

            server.takeRequest()
            server.takeRequest()
            val mappingRequest = server.takeRequest()
            val mapping = mapper.readTree(mappingRequest.body.readUtf8()).path("properties")
            assertThat(mappingRequest.path).isEqualTo("/documents/_mapping")
            assertThat(mapping.path("doc_updated_at").path("type").asText()).isEqualTo("date")
            assertThat(mapping.path("primary_owners").path("type").asText()).isEqualTo("keyword")
            assertThat(mapping.path("secondary_owners").path("type").asText()).isEqualTo("keyword")
        }
    }

    @Test
    fun uncertainAliasSwapPreservesTheReindexedCopyForRecovery() {
        MockWebServer().use { server ->
            server.dispatcher = migrationDispatcher(aliasAppliedDespiteResponse = false)
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
                externalWrites,
            )

            org.junit.jupiter.api.assertThrows<IllegalStateException> {
                indexer.deleteDocuments(7, setOf("one"))
            }

            val requests = recordedRequests(server)
            assertThat(requests).contains("PUT /documents/_block/write")
            assertThat(requests.none { it.startsWith("DELETE /documents-exact-") }).isTrue()
        }
    }

    @Test
    fun uncertainAliasResponseRecoversWhenTheLogicalIndexIsAlreadyExact() {
        MockWebServer().use { server ->
            server.dispatcher = migrationDispatcher(aliasAppliedDespiteResponse = true)
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
                externalWrites,
            )

            indexer.deleteDocuments(7, setOf("one"))

            assertThat(recordedRequests(server)).contains("POST /documents/_delete_by_query?refresh=true")
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
                enqueueKeywordMapping(server)
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
                    externalWrites,
                )

                org.junit.jupiter.api.assertThrows<IllegalStateException> {
                    indexer.updateAccess(7, mapOf("one" to ExternalAccess(isPublic = false)))
                }
            }
        }
    }

    private fun successfulUpdateResponse(): String =
        """{"timed_out":false,"total":1,"updated":1,"noops":0,"version_conflicts":0,"failures":[]}"""

    private fun migrationDispatcher(aliasAppliedDespiteResponse: Boolean): Dispatcher {
        val logicalMappingReads = AtomicInteger()
        val replacementExists = AtomicBoolean(false)
        return object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = requireNotNull(request.path)
                val method = requireNotNull(request.method)
                return when {
                    method == "HEAD" && path == "/documents" -> MockResponse().setResponseCode(200)
                    method == "HEAD" && path.startsWith("/documents-exact-") ->
                        MockResponse().setResponseCode(if (replacementExists.get()) 200 else 404)
                    method == "GET" && path == "/documents/_mapping" -> {
                        val exact = aliasAppliedDespiteResponse && logicalMappingReads.getAndIncrement() > 0
                        jsonResponse(if (exact) exactMappingResponse() else legacyMappingResponse())
                    }
                    method == "PUT" && path == "/documents/_block/write" ->
                        jsonResponse("""{"acknowledged":true,"shards_acknowledged":true}""")
                    method == "PUT" && path.startsWith("/documents-exact-") -> {
                        replacementExists.set(true)
                        jsonResponse("""{"acknowledged":true}""")
                    }
                    method == "GET" && path.startsWith("/documents-exact-") && path.endsWith("/_mapping") ->
                        jsonResponse(exactMappingResponse())
                    method == "GET" && path.endsWith("/_count") -> jsonResponse("""{"count":1}""")
                    method == "POST" && path.startsWith("/_reindex") -> jsonResponse(
                        """{"timed_out":false,"total":1,"created":1,"updated":0,"version_conflicts":0,"failures":[]}""",
                    )
                    method == "POST" && path == "/_aliases" -> jsonResponse("""{"acknowledged":false}""")
                    method == "POST" && path.startsWith("/documents/_delete_by_query") -> jsonResponse(
                        """{"timed_out":false,"total":0,"deleted":0,"version_conflicts":0,"failures":[]}""",
                    )
                    else -> MockResponse().setResponseCode(500).setBody("Unexpected $method $path")
                }
            }
        }
    }

    private fun legacyMappingResponse(): String =
        """{"documents":{"mappings":{"properties":{"source_document_id":{"type":"text"}}}}}"""

    private fun exactMappingResponse(): String = mapper.writeValueAsString(
        mapOf(
            "documents-exact-v1" to mapOf(
                "mappings" to mapOf(
                    "properties" to mapOf(
                        "cc_pair_id" to mapOf("type" to "long"),
                        "source_document_id" to mapOf("type" to "keyword"),
                        "chunk_id" to mapOf("type" to "integer"),
                        "external_user_emails" to mapOf("type" to "keyword"),
                        "external_user_group_ids" to mapOf("type" to "keyword"),
                        "is_public" to mapOf("type" to "boolean"),
                        "document_sets" to mapOf("type" to "keyword"),
                        "doc_updated_at" to mapOf("type" to "date"),
                        "primary_owners" to mapOf("type" to "keyword"),
                        "secondary_owners" to mapOf("type" to "keyword"),
                    ),
                ),
            ),
        ),
    )

    private fun jsonResponse(body: String): MockResponse = MockResponse()
        .setResponseCode(200)
        .setHeader("Content-Type", "application/json")
        .setBody(body)

    private fun recordedRequests(server: MockWebServer): List<String> = buildList {
        while (true) {
            val request = server.takeRequest(200, TimeUnit.MILLISECONDS) ?: break
            add("${request.method} ${request.path}")
        }
    }

    private fun enqueueKeywordMapping(server: MockWebServer) {
        server.enqueue(MockResponse().setResponseCode(200))
        server.enqueue(
            MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                .setBody("""{"documents":{"mappings":{"properties":{"source_document_id":{"type":"keyword"}}}}}"""),
        )
        server.enqueue(MockResponse().setResponseCode(200))
    }

    private fun takeOperationRequest(server: MockWebServer) = server.run {
        takeRequest()
        takeRequest()
        takeRequest()
        takeRequest()
    }
}
