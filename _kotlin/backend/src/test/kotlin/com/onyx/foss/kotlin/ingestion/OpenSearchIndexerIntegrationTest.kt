package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import tools.jackson.databind.SerializationFeature
import tools.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.wait.strategy.Wait
import reactor.netty.http.client.HttpClient
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import okio.Buffer

@Testcontainers
@Tag("opensearch-integration")
class OpenSearchIndexerIntegrationTest {
    private val mapper = jacksonObjectMapper()
    private val client = insecureClient()
    private val index = "indexer-test-${UUID.randomUUID()}"
    private val baseUrl get() = "https://${openSearch.host}:${openSearch.getMappedPort(9200)}"
    private val migrationLock = ReentrantLock()
    private val externalWrites = mock(PairExternalWriteFence::class.java).also { fence ->
        doAnswer { invocation ->
            migrationLock.withLock { invocation.getArgument<() -> Unit>(1).invoke() }
        }.`when`(fence).withOpenSearchIndex(anyString(), any<() -> Unit>() ?: {})
    }

    @AfterEach
    fun deleteIndex() {
        get("/_cat/indices/$index*?format=json").forEach { row ->
            client.delete().uri("$baseUrl/${row.path("index").asText()}")
                .retrieve().toBodilessEntity().block(Duration.ofSeconds(30))
        }
    }

    @Test
    fun exactIdsSupportAclDocumentSetAndSelectiveDeleteMaintenance() {
        val indexer = indexer()
        val urlId = "https://example.test/wiki/Engineering?id=ABC-123"
        val fileId = "FILE_CONNECTOR__file-123"
        val updatedAt = Instant.parse("2026-08-01T00:00:00Z")
        indexer.upsert(
            7,
            urlId,
            0,
            "URL",
            "url content",
            urlId,
            emptyMap(),
            vector(0.1),
            updatedAt = updatedAt,
            primaryOwners = listOf("owner@example.com"),
            secondaryOwners = listOf("reviewer@example.com"),
        )
        indexer.upsert(7, fileId, 0, "File", "file content", null, emptyMap(), vector(0.2))

        indexer.updateAccess(7, mapOf(urlId to ExternalAccess(setOf("reader@example.com"), isPublic = false)))
        indexer.updateDocumentSets(7, setOf(urlId, fileId), listOf("Engineering"))
        indexer.deleteDocuments(7, setOf(fileId))

        val mapping = get("/$index/_mapping")
        assertThat(mapping.path(index).path("mappings").path("properties").path("source_document_id").path("type").asText())
            .isEqualTo("keyword")
        assertThat(mapping.path(index).path("mappings").path("properties").path("doc_updated_at").path("type").asText())
            .isEqualTo("date")
        assertThat(mapping.path(index).path("mappings").path("properties").path("primary_owners").path("type").asText())
            .isEqualTo("keyword")
        val urlDocument = exactDocuments(urlId).single()
        assertThat(urlDocument.path("external_user_emails").toList().map(JsonNode::asText)).containsExactly("reader@example.com")
        assertThat(urlDocument.path("is_public").asBoolean()).isFalse()
        assertThat(urlDocument.path("document_sets").toList().map(JsonNode::asText)).containsExactly("Engineering")
        assertThat(urlDocument.path("doc_updated_at").asText()).isEqualTo(updatedAt.toString())
        assertThat(urlDocument.path("primary_owners").toList().map(JsonNode::asText)).containsExactly("owner@example.com")
        assertThat(urlDocument.path("secondary_owners").toList().map(JsonNode::asText)).containsExactly("reviewer@example.com")
        assertThat(exactDocuments(fileId)).isEmpty()
    }

    @Test
    fun reindexingShorterDocumentRemovesOldTailChunks() {
        val indexer = indexer()
        val documentId = "https://example.test/document/shorter"
        indexer.upsert(7, documentId, 0, "Old", "old head", documentId, emptyMap(), vector(0.1))
        indexer.upsert(7, documentId, 1, "Old", "stale tail", documentId, emptyMap(), vector(0.2))

        indexer.upsert(7, documentId, 0, "New", "new content", documentId, emptyMap(), vector(0.3))
        indexer.deleteStaleChunks(7, documentId, 1)

        assertThat(exactDocuments(documentId).map { it.path("content").asText() }).containsExactly("new content")
    }

    @Test
    fun candidateSearchUsesTheUnionOfSelectedDocumentSets() {
        val indexer = indexer()
        indexer.upsert(7, "engineering", 0, "Guide", "deployment needle", null, emptyMap(), vector(0.1), listOf("Engineering"))
        indexer.upsert(7, "operations", 0, "Guide", "deployment needle", null, emptyMap(), vector(0.2), listOf("Operations"))
        indexer.upsert(7, "finance", 0, "Guide", "deployment needle", null, emptyMap(), vector(0.3), listOf("Finance"))

        val results = indexer.searchCandidates(
            "deployment needle",
            vector(0.1),
            listOf("Engineering", "Operations"),
            10,
        )

        assertThat(results.keyword.map(SearchCandidate::sourceDocumentId))
            .containsExactlyInAnyOrder("engineering", "operations")
        assertThat(results.vector.map(SearchCandidate::sourceDocumentId))
            .containsExactlyInAnyOrder("engineering", "operations")
    }

    @Test
    fun dynamicallyMappedTextIdsAreReindexedWithoutLosingExactIdentity() {
        val urlId = "https://example.test/wiki/Engineering?id=ABC-123"
        val fileId = "FILE_CONNECTOR__file-123"
        putRawDocument("legacy-url", urlId, "url content")
        putRawDocument("legacy-file", fileId, "file content")
        assertThat(mappingProperties().path("source_document_id").path("type").asText()).isEqualTo("text")

        val indexer = indexer()
        indexer.updateAccess(7, mapOf(urlId to ExternalAccess(setOf("reader@example.com"), isPublic = false)))
        indexer.updateDocumentSets(7, setOf(urlId, fileId), listOf("Engineering"))
        indexer.deleteDocuments(7, setOf(fileId))

        assertThat(mappingProperties().path("source_document_id").path("type").asText()).isEqualTo("keyword")
        val urlDocument = exactDocuments(urlId).single()
        assertThat(urlDocument.path("content").asText()).isEqualTo("url content")
        assertThat(urlDocument.path("external_user_emails").toList().map(JsonNode::asText)).containsExactly("reader@example.com")
        assertThat(urlDocument.path("document_sets").toList().map(JsonNode::asText)).containsExactly("Engineering")
        assertThat(exactDocuments(fileId)).isEmpty()
    }

    @Test
    fun concurrentWriterCompletesWhileAnotherReplicaIsMigratingTheLegacyIndex() {
        val legacyId = "legacy-document"
        val concurrentId = "concurrent-document"
        putRawDocument("legacy", legacyId, "legacy content")
        val firstReindex = CountDownLatch(1)
        val releaseFirstReindex = CountDownLatch(1)
        val writerStarted = CountDownLatch(1)
        MockWebServer().use { proxy ->
            proxy.dispatcher = pausingProxy(firstReindex, releaseFirstReindex)
            proxy.start()
            val migratingReplica = indexer(proxy.url("/").toString().trimEnd('/'))
            val writingReplica = indexer(proxy.url("/").toString().trimEnd('/'))
            val executor = Executors.newFixedThreadPool(2)
            try {
                val migration = executor.submit {
                    migratingReplica.updateAccess(
                        7,
                        mapOf(legacyId to ExternalAccess(setOf("reader@example.com"), isPublic = false)),
                    )
                }
                assertThat(firstReindex.await(10, TimeUnit.SECONDS)).isTrue()

                val concurrentWrite = executor.submit {
                    writerStarted.countDown()
                    writingReplica.upsert(
                        7,
                        concurrentId,
                        0,
                        "Concurrent",
                        "concurrent content",
                        null,
                        emptyMap(),
                        vector(0.2),
                    )
                }
                assertThat(writerStarted.await(10, TimeUnit.SECONDS)).isTrue()
                assertThat(concurrentWrite.isDone).isFalse()

                releaseFirstReindex.countDown()
                migration.get(30, TimeUnit.SECONDS)
                concurrentWrite.get(30, TimeUnit.SECONDS)
            } finally {
                releaseFirstReindex.countDown()
                executor.shutdownNow()
            }
        }

        assertThat(exactDocuments(legacyId).single().path("external_user_emails").toList().map(JsonNode::asText))
            .containsExactly("reader@example.com")
        assertThat(exactDocuments(concurrentId).single().path("content").asText()).isEqualTo("concurrent content")
    }

    @Test
    fun concurrentMigratorsRunOneReindexAndPreserveThePostSwapWrite() {
        val sourceDocumentId = "shared-document"
        val documentId = Base64.getUrlEncoder().withoutPadding().encodeToString(
            "7:$sourceDocumentId:0".toByteArray(StandardCharsets.UTF_8),
        )
        putRawDocument(documentId, sourceDocumentId, "legacy content")
        val firstReindex = CountDownLatch(1)
        val secondReindex = CountDownLatch(1)
        val releaseFirstReindex = CountDownLatch(1)
        val reindexCalls = AtomicInteger()
        MockWebServer().use { proxy ->
            proxy.dispatcher = pausingProxy(
                firstReindex,
                releaseFirstReindex,
                reindexCalls,
                secondReindex,
            )
            proxy.start()
            val firstReplica = indexer(proxy.url("/").toString().trimEnd('/'))
            val secondReplica = indexer(proxy.url("/").toString().trimEnd('/'))
            val executor = Executors.newFixedThreadPool(2)
            try {
                val firstMigration = executor.submit {
                    firstReplica.deleteDocuments(7, setOf("not-present"))
                }
                assertThat(firstReindex.await(10, TimeUnit.SECONDS)).isTrue()
                val postSwapWrite = executor.submit {
                    secondReplica.upsert(
                        7,
                        sourceDocumentId,
                        0,
                        "Updated",
                        "post-swap content",
                        null,
                        emptyMap(),
                        vector(0.2),
                    )
                }
                val migrationsOverlapped = secondReindex.await(3, TimeUnit.SECONDS)
                if (migrationsOverlapped) postSwapWrite.get(30, TimeUnit.SECONDS)
                releaseFirstReindex.countDown()
                firstMigration.get(30, TimeUnit.SECONDS)
                if (!migrationsOverlapped) postSwapWrite.get(30, TimeUnit.SECONDS)
            } finally {
                releaseFirstReindex.countDown()
                executor.shutdownNow()
            }
        }

        assertThat(reindexCalls.get()).isEqualTo(1)
        assertThat(exactDocuments(sourceDocumentId).single().path("content").asText())
            .isEqualTo("post-swap content")
    }

    @Test
    fun restartedReplicaCompletesTheDurableBlockedMigration() {
        val firstId = "legacy-one"
        val secondId = "legacy-two"
        putRawDocument("legacy-one", firstId, "first")
        putRawDocument("legacy-two", secondId, "second")
        put("/$index/_block/write")
        val replacement = "$index-exact-v1"
        put("/$replacement", exactIndexDefinition())
        putRawDocument(replacement, "legacy-one", firstId, "first")

        indexer().upsert(7, "after-restart", 0, "Restarted", "new", null, emptyMap(), vector(0.3))

        assertThat((get("/_alias/$index") as tools.jackson.databind.node.ObjectNode).properties().map { it.key }).containsExactly(replacement)
        assertThat(exactDocuments(firstId)).hasSize(1)
        assertThat(exactDocuments(secondId)).hasSize(1)
        assertThat(exactDocuments("after-restart")).hasSize(1)
    }

    private fun indexer(url: String = baseUrl): OpenSearchIndexer = OpenSearchIndexer(
        OnyxProperties(
            opensearch = OnyxProperties.OpenSearch(
                baseUrl = url,
                index = index,
                username = ADMIN_USERNAME,
                password = ADMIN_PASSWORD,
                verifyCerts = false,
            ),
        ),
        WebClient.builder(),
        mapper,
        externalWrites,
    )

    private fun exactDocuments(sourceDocumentId: String): List<JsonNode> = client.post()
        .uri("$baseUrl/$index/_search")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(mapOf("query" to mapOf("term" to mapOf("source_document_id" to sourceDocumentId))))
        .retrieve()
        .bodyToMono(JsonNode::class.java)
        .block(Duration.ofSeconds(30))
        ?.path("hits")?.path("hits")?.toList()?.map { it.path("_source") }
        .orEmpty()

    private fun putRawDocument(documentId: String, sourceDocumentId: String, content: String) {
        putRawDocument(index, documentId, sourceDocumentId, content)
    }

    private fun putRawDocument(
        targetIndex: String,
        documentId: String,
        sourceDocumentId: String,
        content: String,
    ) {
        client.put().uri("$baseUrl/$targetIndex/_doc/$documentId?refresh=true")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(
                mapOf(
                    "cc_pair_id" to 7,
                    "source_document_id" to sourceDocumentId,
                    "chunk_id" to 0,
                    "content" to content,
                ),
            )
            .retrieve().toBodilessEntity().block(Duration.ofSeconds(30))
    }

    private fun put(path: String, body: Any? = null) {
        val request = client.put().uri(baseUrl + path)
        val response = if (body == null) request.retrieve() else {
            request.contentType(MediaType.APPLICATION_JSON).bodyValue(body).retrieve()
        }
        response.toBodilessEntity().block(Duration.ofSeconds(30))
    }

    private fun exactIndexDefinition(): Map<String, Any> = mapOf(
        "settings" to mapOf("index" to mapOf("knn" to true)),
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
                "embedding" to mapOf(
                    "type" to "knn_vector",
                    "dimension" to 768,
                    "method" to mapOf(
                        "name" to "hnsw",
                        "space_type" to "cosinesimil",
                        "engine" to "lucene",
                    ),
                ),
            ),
        ),
    )

    private fun vector(value: Double): List<Double> = List(768) { value }

    private fun pausingProxy(
        firstReindex: CountDownLatch,
        releaseFirstReindex: CountDownLatch,
        reindexCalls: AtomicInteger = AtomicInteger(),
        secondReindex: CountDownLatch = CountDownLatch(0),
    ): Dispatcher {
        return object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if (request.path?.startsWith("/_reindex") == true) {
                    when (reindexCalls.incrementAndGet()) {
                        1 -> {
                            firstReindex.countDown()
                            check(releaseFirstReindex.await(30, TimeUnit.SECONDS))
                        }
                        2 -> secondReindex.countDown()
                    }
                }
                val method = requireNotNull(request.method)
                val upstream = client.method(HttpMethod.valueOf(method)).uri(baseUrl + requireNotNull(request.path))
                val contentType = request.getHeader("Content-Type")
                if (contentType != null) upstream.header("Content-Type", contentType)
                val response = (if (method in setOf("POST", "PUT", "PATCH")) {
                    upstream.bodyValue(request.body.clone().readByteArray())
                } else {
                    upstream
                }).exchangeToMono { result ->
                    result.bodyToMono(ByteArray::class.java).defaultIfEmpty(ByteArray(0)).map { body ->
                        Triple(result.statusCode().value(), result.headers().asHttpHeaders().contentType, body)
                    }
                }.block(Duration.ofSeconds(30)) ?: error("OpenSearch proxy returned no response")
                return MockResponse().setResponseCode(response.first).apply {
                    response.second?.let { setHeader("Content-Type", it.toString()) }
                    setBody(Buffer().write(response.third))
                }
            }
        }
    }

    private fun insecureClient(): WebClient {
        val sslContext = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build()
        return WebClient.builder()
            .defaultHeaders { it.setBasicAuth(ADMIN_USERNAME, ADMIN_PASSWORD) }
            .clientConnector(ReactorClientHttpConnector(HttpClient.create().secure { it.sslContext(sslContext) }))
            .build()
    }

    private fun mappingProperties(): JsonNode = get("/$index/_mapping").properties().first().value
        .path("mappings").path("properties")

    private fun get(path: String): JsonNode = requireNotNull(
        client.get().uri(baseUrl + path).retrieve().bodyToMono(JsonNode::class.java).block(Duration.ofSeconds(30)),
    )

    companion object {
        private const val ADMIN_USERNAME = "admin"
        private const val ADMIN_PASSWORD = "OpenSearchTest1!"

        @Container
        @JvmStatic
        val openSearch: GenericContainer<Nothing> = GenericContainer<Nothing>(
            DockerImageName.parse("opensearchproject/opensearch:3.6.0"),
        ).apply {
            withEnv("discovery.type", "single-node")
            withEnv("OPENSEARCH_INITIAL_ADMIN_PASSWORD", ADMIN_PASSWORD)
            withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
            withCreateContainerCmdModifier { command ->
                command.withHostConfig(
                    requireNotNull(command.hostConfig)
                        .withMemory(1536L * 1024 * 1024)
                        .withMemorySwap(1536L * 1024 * 1024)
                        .withNanoCPUs(1_000_000_000L)
                        .withPidsLimit(512L),
                )
            }
            withExposedPorts(9200)
            waitingFor(
                Wait.forSuccessfulCommand(
                    "curl -fkSs -u admin:${'$'}OPENSEARCH_INITIAL_ADMIN_PASSWORD " +
                        "https://localhost:9200/_cluster/health >/dev/null",
                ),
            )
            withStartupTimeout(Duration.ofMinutes(2))
        }
    }
}
