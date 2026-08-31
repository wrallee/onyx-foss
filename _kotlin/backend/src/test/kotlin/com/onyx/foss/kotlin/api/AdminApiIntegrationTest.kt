package com.onyx.foss.kotlin.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.service.FileStorageService
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockMultipartFile
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put

@AutoConfigureMockMvc
class AdminApiIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var mvc: MockMvc
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var admin: AdminService
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var sets: DocumentSetRepository
    @Autowired private lateinit var jobs: IngestionJobRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var storedFiles: FileStorageService
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE ingestion_errors, ingestion_jobs, ingestion_attempts, ingestion_checkpoints, " +
                "indexed_documents, document_set_cc_pairs, document_sets, connector_credential_pairs, " +
                "connectors, credentials RESTART IDENTITY CASCADE",
        )
    }

    @Test
    fun credentialSecretsAreMaskedAndNeverReturned() {
        val response = postJson("/manage/credential", credential("github", "secret-token"))

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("credential").path("credential_json").path("token").asText()).isEqualTo("********")
        assertThat(response.raw).doesNotContain("secret-token")
        assertThat(credentials.count()).isEqualTo(1)
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun credentialUpdatePreservesMaskedSecretFields() {
        val credentialId = createCredential("github", "secret-token")

        val response = putJson(
            "/manage/admin/credential/$credentialId",
            mapOf("name" to "renamed", "credential_json" to mapOf("token" to "********", "url" to "updated")),
        )

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("credential_json").path("token").asText()).isEqualTo("********")
        assertThat(admin.credentialSecret(credentialId).path("token").asText()).isEqualTo("secret-token")
        assertThat(credentials.count()).isEqualTo(1)
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun associatedCredentialCannotBeDeleted() {
        val credentialId = createCredential("github", "secret-token")
        val connectorId = createConnector("github")
        associate(connectorId, credentialId)

        val response = request(delete("/manage/credential/$credentialId"))

        assertThat(response.status).isEqualTo(409)
        assertThat(response.body.path("detail").asText()).isEqualTo("Credential is still associated with a connector")
        assertThat(credentials.count()).isEqualTo(1)
        assertThat(pairs.count()).isEqualTo(1)
        assertThat(queuedJobs()).isEqualTo(1)
    }

    @Test
    fun connectorRejectsCredentialFromAnotherSource() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("jira", "jira-secret")

        val response = putJson("/manage/connector/$connectorId/credential/$credentialId", pairMetadata())

        assertThat(response.status).isEqualTo(400)
        assertThat(response.body.path("detail").asText()).isEqualTo("Connector and credential source do not match")
        assertThat(connectors.count()).isEqualTo(1)
        assertThat(pairs.count()).isZero()
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun duplicateAssociationReturnsTheExistingPairWithoutASecondJob() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("github", "secret-token")
        val first = associate(connectorId, credentialId)

        val second = putJson("/manage/connector/$connectorId/credential/$credentialId", pairMetadata("new pair name"))

        assertThat(second.status).isEqualTo(200)
        assertThat(second.body.path("success").asBoolean()).isTrue()
        assertThat(second.body.path("data").asLong()).isEqualTo(first)
        assertThat(pairs.count()).isEqualTo(1)
        assertThat(queuedJobs()).isEqualTo(1)
    }

    @Test
    fun overlappingConnectorsRemainIndependent() {
        val credentialId = createCredential("github", "secret-token")
        val firstConnectorId = createConnector("github")
        val secondConnectorId = createConnector("github")
        val firstPairId = associate(firstConnectorId, credentialId, "first")
        val secondPairId = associate(secondConnectorId, credentialId, "second")

        val pause = putJson("/manage/admin/cc-pair/$firstPairId/status", mapOf("status" to "PAUSED"))
        val run = postJson("/manage/admin/connector/run-once", mapOf("connector_id" to secondConnectorId))

        assertThat(pause.status).isEqualTo(200)
        assertThat(pause.body.path("status").asText()).isEqualTo("PAUSED")
        assertThat(run.status).isEqualTo(200)
        assertThat(run.body.path("success").asBoolean()).isTrue()
        assertThat(pairs.findById(firstPairId).orElseThrow().status.name).isEqualTo("PAUSED")
        assertThat(pairs.findById(secondPairId).orElseThrow().status.name).isEqualTo("SCHEDULED")
        assertThat(connectors.count()).isEqualTo(2)
        assertThat(pairs.count()).isEqualTo(2)
        assertThat(queuedJobs()).isEqualTo(3)
    }

    @Test
    fun deletingOneOverlappingPairKeepsTheConnector() {
        val connectorId = createConnector("github")
        val firstCredentialId = createCredential("github", "first-secret")
        val secondCredentialId = createCredential("github", "second-secret")
        associate(connectorId, firstCredentialId, "first")
        associate(connectorId, secondCredentialId, "second")

        val response = postJson(
            "/manage/admin/deletion-attempt",
            mapOf("connector_id" to connectorId, "credential_id" to firstCredentialId),
        )

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("success").asBoolean()).isTrue()
        assertThat(connectors.existsById(connectorId)).isTrue()
        assertThat(pairs.count()).isEqualTo(1)
        assertThat(credentials.count()).isEqualTo(2)
        assertThat(queuedJobs()).isEqualTo(1)
    }

    @Test
    fun deletingTheLastPairDeletesTheConnector() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("github", "secret-token")
        associate(connectorId, credentialId)

        val response = postJson(
            "/manage/admin/deletion-attempt",
            mapOf("connector_id" to connectorId, "credential_id" to credentialId),
        )

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("data").asLong()).isPositive()
        assertThat(connectors.existsById(connectorId)).isFalse()
        assertThat(pairs.count()).isZero()
        assertThat(credentials.count()).isEqualTo(1)
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun documentSetRejectsMissingPair() {
        val response = postJson("/manage/admin/document-set", documentSet("missing", listOf(999L)))

        assertThat(response.status).isEqualTo(400)
        assertThat(response.body.path("detail").asText()).isEqualTo("Document set references a missing connector")
        assertThat(sets.count()).isZero()
        assertThat(joinCount()).isZero()
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun documentSetNameIsUnique() {
        val first = postJson("/manage/admin/document-set", documentSet("unique", emptyList()))
        val duplicate = postJson("/manage/admin/document-set", documentSet("unique", emptyList()))

        assertThat(first.status).isEqualTo(200)
        assertThat(duplicate.status).isEqualTo(409)
        assertThat(duplicate.body.path("detail").asText()).isEqualTo("Document set name already exists")
        assertThat(sets.count()).isEqualTo(1)
        assertThat(joinCount()).isZero()
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun documentSetRenameRejectsAnExistingName() {
        val firstId = postJson("/manage/admin/document-set", documentSet("first", emptyList())).body.asLong()
        val secondId = postJson("/manage/admin/document-set", documentSet("second", emptyList())).body.asLong()

        val response = patchJson(
            "/manage/admin/document-set",
            mapOf("id" to secondId, "name" to "first", "cc_pair_ids" to emptyList<Long>()),
        )

        assertThat(response.status).isEqualTo(409)
        assertThat(response.body.path("detail").asText()).isEqualTo("Document set name already exists")
        assertThat(sets.count()).isEqualTo(2)
        assertThat(sets.findById(firstId).orElseThrow().name).isEqualTo("first")
        assertThat(sets.findById(secondId).orElseThrow().name).isEqualTo("second")
        assertThat(joinCount()).isZero()
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun deletingPairRemovesItsDocumentSetMembership() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("github", "secret-token")
        val pairId = associate(connectorId, credentialId)
        val setId = postJson("/manage/admin/document-set", documentSet("pair-set", listOf(pairId))).body.asLong()

        val response = postJson(
            "/manage/admin/deletion-attempt",
            mapOf("connector_id" to connectorId, "credential_id" to credentialId),
        )

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("success").asBoolean()).isTrue()
        assertThat(sets.existsById(setId)).isTrue()
        assertThat(joinCount()).isZero()
        assertThat(pairs.count()).isZero()
        assertThat(queuedJobs()).isZero()
    }

    @Test
    fun indexAttemptPaginationUsesZeroBasedPages() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("github", "secret-token")
        val pairId = associate(connectorId, credentialId)
        postJson("/manage/admin/connector/run-once", mapOf("connector_id" to connectorId))

        val pageZero = request(get("/manage/admin/cc-pair/$pairId/index-attempts?page_num=0&page_size=1"))
        val pageOne = request(get("/manage/admin/cc-pair/$pairId/index-attempts?page_num=1&page_size=1"))

        assertThat(pageZero.status).isEqualTo(200)
        assertThat(pageOne.status).isEqualTo(200)
        assertThat(pageZero.body.path("items").size()).isEqualTo(1)
        assertThat(pageOne.body.path("items").size()).isEqualTo(1)
        assertThat(pageZero.body.path("items").first().path("id").asLong())
            .isGreaterThan(pageOne.body.path("items").first().path("id").asLong())
        assertThat(pageZero.body.path("total_items").asInt()).isEqualTo(2)
        assertThat(pairs.count()).isEqualTo(1)
        assertThat(queuedJobs()).isEqualTo(2)
    }

    @Test
    fun paginationRejectsNegativePageAndNonPositivePageSize() {
        val connectorId = createConnector("github")
        val credentialId = createCredential("github", "secret-token")
        val pairId = associate(connectorId, credentialId)

        val negativePage = request(get("/manage/admin/cc-pair/$pairId/index-attempts?page_num=-1&page_size=1"))
        val zeroPageSize = request(get("/manage/admin/cc-pair/$pairId/errors?page_num=0&page_size=0"))

        assertThat(negativePage.status).isEqualTo(400)
        assertThat(negativePage.body.path("detail").asText()).isEqualTo("page_num must be non-negative")
        assertThat(zeroPageSize.status).isEqualTo(400)
        assertThat(zeroPageSize.body.path("detail").asText()).isEqualTo("page_size must be positive")
        assertThat(pairs.count()).isEqualTo(1)
        assertThat(queuedJobs()).isEqualTo(1)
    }

    @Test
    fun fileMetadataKeepsNamesAlignedWithLocations() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        associate(connectorId, credentialId)
        val uploaded = request(
            multipart("/manage/admin/connector/file/upload")
                .file(MockMultipartFile("files", "a.txt", MediaType.TEXT_PLAIN_VALUE, "a".toByteArray()))
                .file(MockMultipartFile("files", "b.txt", MediaType.TEXT_PLAIN_VALUE, "b".toByteArray())),
        )
        val aId = uploaded.body.path("file_paths").first().asText()
        val bId = uploaded.body.path("file_paths").get(1).asText()
        val configured = patchJson(
            "/manage/admin/connector/$connectorId",
            connector("file", mapOf("file_locations" to listOf(aId, bId), "file_names" to listOf("a.txt", "b.txt"))),
        )

        val response = request(
            multipart("/manage/admin/connector/$connectorId/files/update")
                .file(MockMultipartFile("files", "c.txt", MediaType.TEXT_PLAIN_VALUE, "c".toByteArray()))
                .param("file_ids_to_remove", mapper.writeValueAsString(listOf(aId))),
        )
        val config = requireNotNull(connectors.findById(connectorId).orElseThrow().connectorSpecificConfig)

        assertThat(uploaded.status).isEqualTo(200)
        assertThat(configured.status).isEqualTo(200)
        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("file_names").map(JsonNode::asText)).containsExactly("c.txt")
        assertThat(config.path("file_locations").map(JsonNode::asText)).containsExactly(bId, response.body.path("file_paths").first().asText())
        assertThat(config.path("file_names").map(JsonNode::asText)).containsExactly("b.txt", "c.txt")
        assertThat(connectors.count()).isEqualTo(1)
        assertThat(queuedJobs()).isEqualTo(2)
    }

    @Test
    fun zipUploadStoresFilesAndMetadataSeparately() {
        val response = request(
            multipart("/manage/admin/connector/file/upload")
                .file(MockMultipartFile("files", "files.zip", "application/zip", zipFile())),
        )

        assertThat(response.status).isEqualTo(200)
        assertThat(response.body.path("file_paths").map(JsonNode::asText)).hasSize(1)
        assertThat(response.body.path("file_names").map(JsonNode::asText)).containsExactly("one.txt")
        val metadataId = response.body.path("zip_metadata_file_id").asText()
        assertThat(metadataId).isNotBlank()
        assertThat(Files.readString(storedFiles.filePath(metadataId))).contains("one.txt")
    }

    @Test
    fun connectorFileUpdateMergesZipMetadata() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        associate(connectorId, credentialId)
        val initial = request(
            multipart("/manage/admin/connector/file/upload")
                .file(MockMultipartFile("files", "first.zip", "application/zip", zipFile("one.txt", "One"))),
        )
        patchJson(
            "/manage/admin/connector/$connectorId",
            connector(
                "file",
                mapOf(
                    "file_locations" to initial.body.path("file_paths").map(JsonNode::asText),
                    "file_names" to initial.body.path("file_names").map(JsonNode::asText),
                    "zip_metadata_file_id" to initial.body.path("zip_metadata_file_id").asText(),
                ),
            ),
        )

        val response = request(
            multipart("/manage/admin/connector/$connectorId/files/update")
                .file(MockMultipartFile("files", "second.zip", "application/x-zip", zipFile("two.txt", "Two"))),
        )
        val config = requireNotNull(connectors.findById(connectorId).orElseThrow().connectorSpecificConfig)
        val metadata = Files.readString(storedFiles.filePath(config.path("zip_metadata_file_id").asText()))

        assertThat(response.status).isEqualTo(200)
        assertThat(config.path("file_names").map(JsonNode::asText)).containsExactly("one.txt", "two.txt")
        assertThat(metadata).contains("one.txt", "two.txt")
    }

    @Test
    fun zipUploadRecognizesFossMimeVariants() {
        listOf("application/x-zip-compressed", "application/x-zip", "multipart/x-zip").forEach { contentType ->
            val response = request(
                multipart("/manage/admin/connector/file/upload")
                    .file(MockMultipartFile("files", "archive", contentType, zipFile("archive.txt", contentType))),
            )

            assertThat(response.status).isEqualTo(200)
            assertThat(response.body.path("file_paths").size()).isEqualTo(1)
            assertThat(response.body.path("zip_metadata_file_id").asText()).isNotBlank()
        }
    }

    @Test
    fun zipUploadRejectsEntriesLargerThanTheUploadLimit() {
        val response = request(
            multipart("/manage/admin/connector/file/upload")
                .file(MockMultipartFile("files", "large.zip", "application/zip", oversizedZipFile())),
        )

        assertThat(response.status).isEqualTo(400)
    }

    private fun createCredential(source: String, token: String): Long =
        postJson("/manage/credential", credential(source, token)).body.path("id").asLong()

    private fun zipFile(fileName: String = "one.txt", displayName: String = "One"): ByteArray = ByteArrayOutputStream().use { bytes ->
        ZipOutputStream(bytes).use { zip ->
            zip.putNextEntry(ZipEntry(".onyx_metadata.json"))
            zip.write("""[{"filename":"$fileName","file_display_name":"$displayName"}]""".toByteArray())
            zip.closeEntry()
            zip.putNextEntry(ZipEntry(fileName))
            zip.write("one".toByteArray())
            zip.closeEntry()
        }
        bytes.toByteArray()
    }

    private fun oversizedZipFile(): ByteArray = ByteArrayOutputStream().use { bytes ->
        ZipOutputStream(bytes).use { zip ->
            zip.putNextEntry(ZipEntry("large.txt"))
            repeat(101) { zip.write(ByteArray(1024 * 1024)) }
            zip.closeEntry()
        }
        bytes.toByteArray()
    }

    private fun createConnector(source: String): Long =
        postJson("/manage/admin/connector", connector(source)).body.path("id").asLong()

    private fun associate(connectorId: Long, credentialId: Long, name: String = "pair"): Long {
        val response = putJson("/manage/connector/$connectorId/credential/$credentialId", pairMetadata(name))
        assertThat(response.status).isEqualTo(200)
        return response.body.path("data").asLong()
    }

    private fun credential(source: String, token: String): Map<String, Any> = mapOf(
        "source" to source,
        "name" to "$source credential",
        "credential_json" to mapOf("token" to token, "url" to "https://example.test"),
    )

    private fun connector(source: String, config: Map<String, Any> = emptyMap()): Map<String, Any> = mapOf(
        "name" to "$source connector",
        "source" to source,
        "connector_specific_config" to config,
    )

    private fun pairMetadata(name: String = "pair"): Map<String, String> = mapOf("name" to name)

    private fun documentSet(name: String, pairIds: List<Long>): Map<String, Any> = mapOf(
        "name" to name,
        "cc_pair_ids" to pairIds,
    )

    private fun postJson(url: String, body: Any): Response = request(
        post(url).contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(body)),
    )

    private fun putJson(url: String, body: Any): Response = request(
        put(url).contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(body)),
    )

    private fun patchJson(url: String, body: Any): Response = request(
        patch(url).contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(body)),
    )

    private fun request(request: org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder): Response {
        val result = mvc.perform(request).andReturn().response
        return Response(result.status, mapper.readTree(result.contentAsString), result.contentAsString)
    }

    private fun queuedJobs(): Long = jobs.count()

    private fun joinCount(): Long = jdbc.queryForObject(
        "SELECT COUNT(*) FROM document_set_cc_pairs",
        Long::class.java,
    ) ?: 0

    private data class Response(val status: Int, val body: JsonNode, val raw: String)
}
