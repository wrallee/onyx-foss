package com.onyx.foss.kotlin.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncStatus
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.IngestionAttemptEntity
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionErrorEntity
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
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
import java.sql.Timestamp
import java.time.Instant
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
    @Autowired private lateinit var documentSetSyncOutbox: DocumentSetSyncOutboxRepository
    @Autowired private lateinit var jobs: IngestionJobRepository
    @Autowired private lateinit var attempts: IngestionAttemptRepository
    @Autowired private lateinit var errors: IngestionErrorRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var permissionAttempts: PermissionSyncAttemptRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var storedFiles: FileStorageService
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE permission_sync_staging, permission_sync_attempts, document_set_sync_outbox, ingestion_errors, " +
                "ingestion_jobs, ingestion_attempts, ingestion_checkpoints, indexed_documents, document_set_cc_pairs, " +
                "document_sets, connector_credential_pairs, " +
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
    fun connectorCreation() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")

        val pairId = associate(connectorId, credentialId)
        val detail = request(get("/manage/admin/cc-pair/$pairId"))

        assertThat(detail.status).isEqualTo(200)
        assertThat(detail.body.path("connector").path("id").asLong()).isEqualTo(connectorId)
        assertThat(detail.body.path("credential").path("id").asLong()).isEqualTo(credentialId)
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId)).hasSize(1)
        assertThat(queuedJobs()).isEqualTo(1)
    }

    @Test
    fun overlappingConnectorCreation() {
        val credentialId = createCredential("github", "secret-token")
        val firstConnectorId = createConnector("github")
        val secondConnectorId = createConnector("github")

        val firstPairId = associate(firstConnectorId, credentialId, "first")
        val secondPairId = associate(secondConnectorId, credentialId, "second")

        assertThat(firstPairId).isNotEqualTo(secondPairId)
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(firstPairId)).hasSize(1)
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(secondPairId)).hasSize(1)
        assertThat(connectors.count()).isEqualTo(2)
        assertThat(pairs.count()).isEqualTo(2)
        assertThat(queuedJobs()).isEqualTo(2)
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
        assertThat(queuedJobs()).isEqualTo(2)
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
    fun multipleDocumentSetsSyncingSameConnnector() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        val pairId = associate(connectorId, credentialId)
        saveIndexedDocument(pairId, "shared")

        postJson("/manage/admin/document-set", documentSet("first", listOf(pairId)))
        val secondId = postJson("/manage/admin/document-set", documentSet("second", listOf(pairId))).body.asLong()
        patchJson(
            "/manage/admin/document-set",
            mapOf("id" to secondId, "name" to "second", "cc_pair_ids" to emptyList<Long>()),
        )

        assertThat(documentSetSyncOutbox.findAll()).hasSize(3)
        assertThat(documentSetSyncOutbox.findAll().map { row -> row.ccPairIds?.map { it.asLong() } })
            .containsOnly(listOf(pairId))
    }

    @Test
    fun renamingDocumentSet() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        val pairId = associate(connectorId, credentialId)
        saveIndexedDocument(pairId, "shared")
        val setId = postJson("/manage/admin/document-set", documentSet("original", listOf(pairId))).body.asLong()

        patchJson(
            "/manage/admin/document-set",
            mapOf("id" to setId, "name" to "renamed", "cc_pair_ids" to listOf(pairId)),
        )
        request(delete("/manage/admin/document-set/$setId"))

        assertThat(documentSetSyncOutbox.findAll()).hasSize(3)
        assertThat(documentSetSyncOutbox.findAll().map { row -> row.ccPairIds?.map { it.asLong() } })
            .containsOnly(listOf(pairId))
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
        jobs.saveAndFlush(jobs.findAll().single().apply { state = JobState.SUCCEEDED })
        attempts.saveAndFlush(
            attempts.findAllByCcPairIdOrderByIdDesc(pairId).single().apply { status = AttemptStatus.SUCCESS },
        )
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
    fun lastIndexedMixedStatuses() {
        val pairId = createPairWithoutQueuedAttempt()
        val olderSuccess = Instant.parse("2026-08-31T01:00:00Z")
        val newerPartial = Instant.parse("2026-08-31T02:00:00Z")
        saveAttempt(pairId, AttemptStatus.SUCCESS, olderSuccess)
        saveAttempt(pairId, AttemptStatus.FAILED, Instant.parse("2026-08-31T04:00:00Z"))
        saveAttempt(pairId, AttemptStatus.COMPLETED_WITH_ERRORS, newerPartial)
        saveAttempt(pairId, AttemptStatus.IN_PROGRESS, Instant.parse("2026-08-31T05:00:00Z"))

        assertLastIndexed(pairId, newerPartial)
    }

    @Test
    fun lastIndexedCompletedWithErrors() {
        val pairId = createPairWithoutQueuedAttempt()
        val partial = Instant.parse("2026-08-31T02:00:00Z")
        saveAttempt(pairId, AttemptStatus.SUCCESS, Instant.parse("2026-08-31T01:00:00Z"))
        saveAttempt(pairId, AttemptStatus.COMPLETED_WITH_ERRORS, partial)
        repeat(10) { offset ->
            saveAttempt(pairId, AttemptStatus.FAILED, Instant.parse("2026-08-31T03:00:00Z").plusSeconds(offset.toLong()))
        }

        assertLastIndexed(pairId, partial)
    }

    @Test
    fun lastIndexedFirstPageAllErrors() {
        val pairId = createPairWithoutQueuedAttempt()
        val success = Instant.parse("2026-08-31T01:00:00Z")
        saveAttempt(pairId, AttemptStatus.SUCCESS, success)
        repeat(10) { offset ->
            saveAttempt(pairId, AttemptStatus.FAILED, Instant.parse("2026-08-31T02:00:00Z").plusSeconds(offset.toLong()))
        }

        assertLastIndexed(pairId, success)
    }

    @Test
    fun lastIndexedCredentialSwapScenario() {
        val connectorId = createConnector("github")
        val firstCredentialId = createCredential("github", "first-secret")
        val secondCredentialId = createCredential("github", "second-secret")
        val firstPairId = associate(connectorId, firstCredentialId, "first")
        val secondPairId = associate(connectorId, secondCredentialId, "second")
        jdbc.update("DELETE FROM ingestion_jobs")
        jdbc.update("DELETE FROM ingestion_attempts")
        saveAttempt(firstPairId, AttemptStatus.SUCCESS, Instant.parse("2026-08-31T01:00:00Z"))
        val secondSuccess = Instant.parse("2026-08-31T02:00:00Z")
        saveAttempt(secondPairId, AttemptStatus.SUCCESS, secondSuccess)
        repeat(10) { offset ->
            saveAttempt(secondPairId, AttemptStatus.FAILED, Instant.parse("2026-08-31T03:00:00Z").plusSeconds(offset.toLong()))
        }

        assertLastIndexed(secondPairId, secondSuccess)
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
    fun entityErrorsReturnStoredFailureContext() {
        val pairId = createPairWithoutQueuedAttempt()
        val attempt = attempts.save(IngestionAttemptEntity(ccPairId = pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))
        val missedStart = Instant.parse("2026-08-01T00:00:00Z")
        val missedEnd = Instant.parse("2026-08-02T00:00:00Z")
        errors.save(
            IngestionErrorEntity(
                attemptId = requireNotNull(attempt.id),
                entityId = "repository:test/project",
                failedTimeRangeStart = missedStart,
                failedTimeRangeEnd = missedEnd,
                failureMessage = "Repository enumeration failed",
            ),
        )

        val response = request(get("/manage/admin/cc-pair/$pairId/errors?page_num=0&page_size=10"))

        val item = response.body.path("items").single()
        assertThat(item.path("entity_id").asText()).isEqualTo("repository:test/project")
        assertThat(Instant.parse(item.path("failed_time_range_start").asText())).isEqualTo(missedStart)
        assertThat(Instant.parse(item.path("failed_time_range_end").asText())).isEqualTo(missedEnd)
    }

    @Test
    fun pairDetailDerivesPruneAndPermissionStatusFromDurableRows() {
        val pairId = createPairWithoutQueuedAttempt()
        val prunedAt = Instant.parse("2026-08-01T00:00:00Z")
        val successfulAt = Instant.parse("2026-08-02T00:00:00Z")
        val failedAt = Instant.parse("2026-08-03T00:00:00Z")
        pairs.findById(pairId).orElseThrow().also {
            it.lastPrunedAt = prunedAt
            pairs.saveAndFlush(it)
        }
        permissionAttempts.save(
            PermissionSyncAttemptEntity(
                ccPairId = pairId,
                status = AttemptStatus.SUCCESS,
                timeFinished = successfulAt,
            ),
        )
        permissionAttempts.save(
            PermissionSyncAttemptEntity(
                ccPairId = pairId,
                status = AttemptStatus.FAILED,
                errorMessage = "permission lookup failed",
                timeFinished = failedAt,
            ),
        )

        val terminal = request(get("/manage/admin/cc-pair/$pairId")).body

        assertThat(Instant.parse(terminal.path("last_pruned").asText())).isEqualTo(prunedAt)
        assertThat(Instant.parse(terminal.path("last_full_permission_sync").asText())).isEqualTo(successfulAt)
        assertThat(terminal.path("last_permission_sync_attempt_status").asText()).isEqualTo("failed")
        assertThat(terminal.path("permission_syncing").asBoolean()).isFalse()
        assertThat(Instant.parse(terminal.path("last_permission_sync_attempt_finished").asText())).isEqualTo(failedAt)
        assertThat(terminal.path("last_permission_sync_attempt_error_message").asText())
            .isEqualTo("permission lookup failed")

        permissionAttempts.save(PermissionSyncAttemptEntity(ccPairId = pairId))

        val pending = request(get("/manage/admin/cc-pair/$pairId")).body
        assertThat(pending.path("last_permission_sync_attempt_status").asText()).isEqualTo("not_started")
        assertThat(pending.path("permission_syncing").asBoolean()).isTrue()
        assertThat(Instant.parse(pending.path("last_full_permission_sync").asText())).isEqualTo(successfulAt)
    }

    @Test
    fun documentSetDetailIsCurrentOnlyAfterDurableOutboxCompletes() {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        val pairId = associate(connectorId, credentialId)
        val setId = postJson("/manage/admin/document-set", documentSet("Engineering", listOf(pairId))).body.asLong()

        val pending = request(get("/manage/admin/document-set/$setId")).body

        assertThat(pending.path("is_up_to_date").asBoolean()).isFalse()
        documentSetSyncOutbox.saveAllAndFlush(
            documentSetSyncOutbox.findAll().onEach { it.status = DocumentSetSyncStatus.DONE },
        )

        val completed = request(get("/manage/admin/document-set/$setId")).body
        assertThat(completed.path("is_up_to_date").asBoolean()).isTrue()
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
        assertThat(queuedJobs()).isEqualTo(1)
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

    private fun createPairWithoutQueuedAttempt(): Long {
        val connectorId = createConnector("file")
        val credentialId = createCredential("file", "file-secret")
        val pairId = associate(connectorId, credentialId)
        jdbc.update("DELETE FROM ingestion_jobs")
        jdbc.update("DELETE FROM ingestion_attempts")
        return pairId
    }

    private fun saveAttempt(pairId: Long, status: AttemptStatus, started: Instant) {
        val attempt = attempts.save(
            IngestionAttemptEntity(ccPairId = pairId, status = status, timeStarted = started),
        )
        jdbc.update(
            "UPDATE ingestion_attempts SET time_started = ?, time_updated = ? WHERE id = ?",
            Timestamp.from(started),
            Timestamp.from(started.plusSeconds(30)),
            requireNotNull(attempt.id),
        )
    }

    private fun saveIndexedDocument(pairId: Long, sourceDocumentId: String) {
        documents.save(
            IndexedDocumentEntity(
                ccPairId = pairId,
                sourceDocumentId = sourceDocumentId,
                title = sourceDocumentId,
                contentHash = sourceDocumentId,
                metadata = mapper.createObjectNode(),
            ),
        )
    }

    private fun assertLastIndexed(pairId: Long, expected: Instant) {
        val detail = request(get("/manage/admin/cc-pair/$pairId"))
        val listing = postJson("/manage/admin/connector/indexing-status", emptyMap<String, Any>())
            .body.flatMap { it.path("indexing_statuses").toList() }
            .first { it.path("cc_pair_id").asLong() == pairId }

        assertThat(Instant.parse(detail.body.path("last_indexed").asText())).isEqualTo(expected)
        assertThat(Instant.parse(listing.path("last_success").asText())).isEqualTo(expected)
    }

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
