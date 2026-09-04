package com.onyx.foss.kotlin.ingestion

import tools.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.ConnectorSource
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.SelfSignedCertificate
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
import reactor.core.publisher.Mono
import reactor.netty.http.server.HttpServer
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
    fun `candidate search applies document set union to keyword and vector queries`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(jsonResponse(exactMappingResponse()))
            server.enqueue(jsonResponse(searchResponse("keyword", 2.0)))
            server.enqueue(jsonResponse(searchResponse("vector", 1.5)))
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

            val results = indexer.searchCandidates(
                query = "deployment guide",
                queryEmbedding = List(768) { 0.1 },
                documentSets = listOf("Engineering", "Operations"),
                count = 30,
            )

            server.takeRequest()
            server.takeRequest()
            val keyword = mapper.readTree(server.takeRequest().body.readUtf8())
            val vector = mapper.readTree(server.takeRequest().body.readUtf8())
            assertThat(keyword.path("size").asInt()).isEqualTo(30)
            assertThat(keyword.path("query").path("bool").path("filter").first().path("terms")
                .path("document_sets").toList().map{ it.asText() })
                .containsExactly("Engineering", "Operations")
            assertThat(vector.path("query").path("knn").path("embedding").path("filter").first()
                .path("terms").path("document_sets").toList().map{ it.asText() })
                .containsExactly("Engineering", "Operations")
            assertThat(results.keyword.single().id).isEqualTo("keyword")
            assertThat(results.vector.single().id).isEqualTo("vector")
        }
    }

    @Test
    fun `candidate search applies source type and updated-after filters`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(jsonResponse(exactMappingResponse()))
            server.enqueue(jsonResponse(searchResponse("keyword", 2.0)))
            server.enqueue(jsonResponse(searchResponse("vector", 1.5)))
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

            indexer.searchCandidates(
                query = "deployment guide",
                queryEmbedding = List(768) { 0.1 },
                documentSets = emptyList(),
                count = 30,
                sourceTypes = listOf("jira", "github"),
                updatedAfter = java.time.Instant.parse("2026-01-01T00:00:00Z"),
            )

            server.takeRequest()
            server.takeRequest()
            val keyword = mapper.readTree(server.takeRequest().body.readUtf8())
            val filters = keyword.path("query").path("bool").path("filter").toList()
            assertThat(filters.map { it.path("terms").path("source_type") }.filter { !it.isMissingNode }
                .single().toList().map { it.asText() }).containsExactly("jira", "github")
            assertThat(filters.map { it.path("range").path("doc_updated_at").path("gte") }
                .filter { !it.isMissingNode }.single().asText()).isEqualTo("2026-01-01T00:00:00Z")
        }
    }

    @Test
    fun `chunksInRange fetches ordered chunks for a document`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(jsonResponse(exactMappingResponse()))
            server.enqueue(
                jsonResponse(
                    """{"hits":{"hits":[
                        {"_id":"a","_score":1.0,"_source":{"source_document_id":"doc-1","chunk_id":1,"title":"T","content":"above","link":null,"metadata":{}}},
                        {"_id":"b","_score":1.0,"_source":{"source_document_id":"doc-1","chunk_id":2,"title":"T","content":"center","link":null,"metadata":{}}},
                        {"_id":"c","_score":1.0,"_source":{"source_document_id":"doc-1","chunk_id":3,"title":"T","content":"below","link":null,"metadata":{}}}
                    ]}}""",
                ),
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

            val chunks = indexer.chunksInRange("doc-1", minChunkId = 1, maxChunkId = 3)

            server.takeRequest()
            server.takeRequest()
            val request = mapper.readTree(server.takeRequest().body.readUtf8())
            assertThat(request.path("size").asInt()).isEqualTo(3)
            val filters = request.path("query").path("bool").path("filter").toList()
            assertThat(filters.map { it.path("term").path("source_document_id") }.filter { !it.isMissingNode }
                .single().asText()).isEqualTo("doc-1")
            val range = filters.map { it.path("range").path("chunk_id") }.filter { !it.isMissingNode }.single()
            assertThat(range.path("gte").asInt()).isEqualTo(1)
            assertThat(range.path("lte").asInt()).isEqualTo(3)
            assertThat(chunks.map { it.content }).containsExactly("above", "center", "below")
        }
    }

    @Test
    fun `new index stores embeddings as 768 dimensional knn vectors`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(404))
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(MockResponse().setResponseCode(200))
            server.start()
            val indexer = OpenSearchIndexer(
                OnyxProperties(
                    modelServer = OnyxProperties.ModelServer(embeddingDimension = 768),
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

            server.takeRequest()
            val create = server.takeRequest()
            val body = mapper.readTree(create.body.readUtf8())
            val embedding = body.path("mappings").path("properties").path("embedding")
            assertThat(create.path).isEqualTo("/documents")
            assertThat(body.path("settings").path("index").path("knn").asBoolean()).isTrue()
            assertThat(embedding.path("type").asText()).isEqualTo("knn_vector")
            assertThat(embedding.path("dimension").asInt()).isEqualTo(768)
            assertThat(embedding.path("method").path("engine").asText()).isEqualTo("lucene")
            assertThat(embedding.path("method").path("space_type").asText()).isEqualTo("cosinesimil")
        }
    }

    @Test
    fun `upsert stores the connector source type on each chunk`() {
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

            indexer.upsert(
                7, "one", 0, "One", "content", null, emptyMap(), listOf(0.1),
                sourceType = ConnectorSource.JIRA,
            )

            val request = takeOperationRequest(server)
            val body = mapper.readTree(request.body.readUtf8())
            assertThat(body.path("source_type").asText()).isEqualTo("jira")
        }
    }

    @Test
    fun `existing numeric embedding mapping requires an explicit index reset`() {
        MockWebServer().use { server ->
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody(exactMappingResponse("float")),
            )
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

            val error = org.junit.jupiter.api.assertThrows<IllegalStateException> {
                indexer.upsert(7, "one", 0, "One", "content", null, emptyMap(), listOf(0.1))
            }

            assertThat(error.message).contains("delete the OpenSearch index")
        }
    }

    @Test
    fun `accepts self-signed OpenSearch certificate when verification is disabled`() {
        val certificate = SelfSignedCertificate("localhost")
        val server = HttpServer.create()
            .host("localhost")
            .port(0)
            .secure { ssl ->
                ssl.sslContext(SslContextBuilder.forServer(certificate.certificate(), certificate.privateKey()).build())
            }
            .handle { request, response ->
                when {
                    request.method().name() == "HEAD" -> response.send()
                    request.uri().endsWith("/_mapping") -> response
                        .header("Content-Type", "application/json")
                        .sendString(Mono.just(exactMappingResponse()))
                    else -> response.header("Content-Type", "application/json").sendString(
                        Mono.just("""{"timed_out":false,"total":0,"deleted":0,"version_conflicts":0,"failures":[]}"""),
                    )
                }
            }
            .bindNow()
        try {
            val properties = OnyxProperties(
                opensearch = OnyxProperties.OpenSearch(
                    baseUrl = "https://localhost:${server.port()}",
                    verifyCerts = false,
                ),
            )

            OpenSearchIndexer(properties, WebClient.builder(), mapper, externalWrites).deletePair(1)
        } finally {
            server.disposeNow()
            certificate.delete()
        }
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
            assertThat(body.path("query").path("bool").path("filter").path(1).path("terms").path("source_document_id").toList().map{ it.asText() })
                .containsExactlyInAnyOrder("one", "two")
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
            assertThat(body.path("query").path("bool").path("filter").path(1).path("terms").path("source_document_id").toList().map{ it.asText() })
                .containsExactlyInAnyOrder("one", "two")
            assertThat(body.path("script").path("params").path("document_sets").toList().map{ it.asText() })
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
            assertThat(body.path("is_public").asBoolean()).isTrue()
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

    private fun searchResponse(id: String, score: Double): String =
        """{"hits":{"hits":[{"_id":"$id","_score":$score,"_source":{"source_document_id":"doc-$id","chunk_id":0,"title":"Title","content":"Content","link":"https://example.test/$id","metadata":{"type":"guide"}}}]}}"""

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

    private fun exactMappingResponse(embeddingType: String = "knn_vector"): String = mapper.writeValueAsString(
        mapOf(
            "documents-exact-v1" to mapOf(
                "mappings" to mapOf(
                    "properties" to mapOf(
                        "cc_pair_id" to mapOf("type" to "long"),
                        "source_document_id" to mapOf("type" to "keyword"),
                        "chunk_id" to mapOf("type" to "integer"),
                        "source_type" to mapOf("type" to "keyword"),
                        "external_user_emails" to mapOf("type" to "keyword"),
                        "external_user_group_ids" to mapOf("type" to "keyword"),
                        "is_public" to mapOf("type" to "boolean"),
                        "document_sets" to mapOf("type" to "keyword"),
                        "doc_updated_at" to mapOf("type" to "date"),
                        "primary_owners" to mapOf("type" to "keyword"),
                        "secondary_owners" to mapOf("type" to "keyword"),
                        "embedding" to if (embeddingType == "knn_vector") {
                            mapOf("type" to embeddingType, "dimension" to 768)
                        } else {
                            mapOf("type" to embeddingType)
                        },
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
