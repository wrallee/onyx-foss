package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.WebClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.wait.strategy.Wait
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Testcontainers
class OpenSearchIndexerIntegrationTest {
    private val mapper = jacksonObjectMapper().findAndRegisterModules()
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    private val client = WebClient.builder().build()
    private val index = "indexer-test-${UUID.randomUUID()}"
    private val baseUrl get() = "http://${openSearch.host}:${openSearch.getMappedPort(9200)}"

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
            listOf(0.1),
            updatedAt = updatedAt,
            primaryOwners = listOf("owner@example.com"),
            secondaryOwners = listOf("reviewer@example.com"),
        )
        indexer.upsert(7, fileId, 0, "File", "file content", null, emptyMap(), listOf(0.2))

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
        assertThat(urlDocument.path("external_user_emails").map(JsonNode::asText)).containsExactly("reader@example.com")
        assertThat(urlDocument.path("is_public").asBoolean()).isFalse()
        assertThat(urlDocument.path("document_sets").map(JsonNode::asText)).containsExactly("Engineering")
        assertThat(urlDocument.path("doc_updated_at").asText()).isEqualTo(updatedAt.toString())
        assertThat(urlDocument.path("primary_owners").map(JsonNode::asText)).containsExactly("owner@example.com")
        assertThat(urlDocument.path("secondary_owners").map(JsonNode::asText)).containsExactly("reviewer@example.com")
        assertThat(exactDocuments(fileId)).isEmpty()
    }

    @Test
    fun reindexingShorterDocumentRemovesOldTailChunks() {
        val indexer = indexer()
        val documentId = "https://example.test/document/shorter"
        indexer.upsert(7, documentId, 0, "Old", "old head", documentId, emptyMap(), listOf(0.1))
        indexer.upsert(7, documentId, 1, "Old", "stale tail", documentId, emptyMap(), listOf(0.2))

        indexer.upsert(7, documentId, 0, "New", "new content", documentId, emptyMap(), listOf(0.3))
        indexer.deleteStaleChunks(7, documentId, 1)

        assertThat(exactDocuments(documentId).map { it.path("content").asText() }).containsExactly("new content")
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
        assertThat(urlDocument.path("external_user_emails").map(JsonNode::asText)).containsExactly("reader@example.com")
        assertThat(urlDocument.path("document_sets").map(JsonNode::asText)).containsExactly("Engineering")
        assertThat(exactDocuments(fileId)).isEmpty()
    }

    private fun indexer(): OpenSearchIndexer = OpenSearchIndexer(
        OnyxProperties(opensearch = OnyxProperties.OpenSearch(baseUrl = baseUrl, index = index)),
        WebClient.builder(),
        mapper,
    )

    private fun exactDocuments(sourceDocumentId: String): List<JsonNode> = client.post()
        .uri("$baseUrl/$index/_search")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(mapOf("query" to mapOf("term" to mapOf("source_document_id" to sourceDocumentId))))
        .retrieve()
        .bodyToMono(JsonNode::class.java)
        .block(Duration.ofSeconds(30))
        ?.path("hits")?.path("hits")?.map { it.path("_source") }
        .orEmpty()

    private fun putRawDocument(documentId: String, sourceDocumentId: String, content: String) {
        client.put().uri("$baseUrl/$index/_doc/$documentId?refresh=true")
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

    private fun mappingProperties(): JsonNode = get("/$index/_mapping").fields().next().value
        .path("mappings").path("properties")

    private fun get(path: String): JsonNode = requireNotNull(
        client.get().uri(baseUrl + path).retrieve().bodyToMono(JsonNode::class.java).block(Duration.ofSeconds(30)),
    )

    companion object {
        @Container
        @JvmStatic
        val openSearch: GenericContainer<Nothing> = GenericContainer<Nothing>(
            DockerImageName.parse("opensearchproject/opensearch:3.6.0"),
        ).apply {
            withEnv("discovery.type", "single-node")
            withEnv("DISABLE_SECURITY_PLUGIN", "true")
            withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
            withCreateContainerCmdModifier { command ->
                command.withHostConfig(
                    requireNotNull(command.hostConfig)
                        .withMemory(2L * 1024 * 1024 * 1024)
                        .withMemorySwap(2L * 1024 * 1024 * 1024)
                        .withNanoCPUs(1_000_000_000L)
                        .withPidsLimit(256L),
                )
            }
            withExposedPorts(9200)
            waitingFor(Wait.forHttp("/_cluster/health"))
            withStartupTimeout(Duration.ofMinutes(2))
        }
    }
}
