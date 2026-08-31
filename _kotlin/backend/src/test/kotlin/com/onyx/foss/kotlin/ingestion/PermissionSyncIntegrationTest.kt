package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@AutoConfigureMockMvc
class PermissionSyncIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var worker: PermissionSyncWorker
    @Autowired private lateinit var mvc: MockMvc
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var attempts: PermissionSyncAttemptRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var remoteLoaders: RemoteConnectorLoaders
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE permission_sync_attempts, ingestion_errors, ingestion_jobs, ingestion_attempts, " +
                "ingestion_checkpoints, indexed_documents, connector_credential_pairs, connectors, credentials " +
                "RESTART IDENTITY CASCADE",
        )
    }

    @Test
    fun permissionAttemptsReturnApplicableTrueForSupportedConnector() {
        val pairId = createPair()

        val response = getJson("/manage/admin/cc-pair/$pairId/permission-sync-attempts")

        assertThat(response.path("applicable").asBoolean()).isTrue()
        assertThat(response.path("items")).isEmpty()
        assertThat(response.path("total_items").asInt()).isZero()
    }

    @Test
    fun permissionAttemptsAreNewestFirstAndPaginated() {
        val pairId = createPair()
        val ids = listOf(AttemptStatus.SUCCESS, AttemptStatus.FAILED, AttemptStatus.COMPLETED_WITH_ERRORS).map { status ->
            requireNotNull(attempts.save(PermissionSyncAttemptEntity(ccPairId = pairId, status = status)).id)
        }

        val first = getJson("/manage/admin/cc-pair/$pairId/permission-sync-attempts?page_num=0&page_size=2")
        val second = getJson("/manage/admin/cc-pair/$pairId/permission-sync-attempts?page_num=1&page_size=2")

        assertThat(first.path("items").map { it.path("id").asLong() }).containsExactly(ids[2], ids[1])
        assertThat(second.path("items").map { it.path("id").asLong() }).containsExactly(ids[0])
        assertThat(first.path("total_items").asInt()).isEqualTo(3)
        assertThat(second.path("total_items").asInt()).isEqualTo(3)
    }

    @Test
    fun permissionAttemptsExposeFailedAndPartialStates() {
        val pairId = createPair()
        attempts.save(
            PermissionSyncAttemptEntity(
                ccPairId = pairId,
                status = AttemptStatus.FAILED,
                errorMessage = "fatal lookup",
            ),
        )
        attempts.save(PermissionSyncAttemptEntity(ccPairId = pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))

        val items = getJson("/manage/admin/cc-pair/$pairId/permission-sync-attempts").path("items")

        assertThat(items.map { it.path("status").asText() })
            .containsExactly("completed_with_errors", "failed")
        assertThat(items.get(1).path("error_message").asText()).isEqualTo("fatal lookup")
    }

    @Test
    fun externalGroupAttemptsRemainNotApplicable() {
        val pairId = createPair()

        val response = getJson("/manage/admin/cc-pair/$pairId/external-group-sync-attempts")

        assertThat(response.path("applicable").asBoolean()).isFalse()
        assertThat(response.path("items")).isEmpty()
        assertThat(response.path("total_items").asInt()).isZero()
    }

    @Test
    fun storesAclForEveryReturnedDocument() {
        val pairId = createPair()
        saveDocument(pairId, "one")
        saveDocument(pairId, "two")
        val firstAccess = ExternalAccess(setOf("one@example.com"), setOf("team-1"), isPublic = false)
        val secondAccess = ExternalAccess(isPublic = true)
        permissionLoad(
            ConnectorBatch(checkpoint = checkpoint(hasMore = true)),
            ConnectorBatch(
                documents = listOf(permissionDocument("one", firstAccess)),
                checkpoint = checkpoint(hasMore = true),
            ),
            ConnectorBatch(
                documents = listOf(permissionDocument("two", secondAccess)),
                checkpoint = checkpoint(),
            ),
        )

        worker.process(pairId)

        assertAccess(pairId, "one", firstAccess)
        assertAccess(pairId, "two", secondAccess)
        verify(indexer).updateAccess(pairId, mapOf("one" to firstAccess, "two" to secondAccess))
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId).single().status).isEqualTo(AttemptStatus.SUCCESS)
    }

    @Test
    fun fileAclUsesThePairPublicAccessFlag() {
        val publicPairId = createPair(source = ConnectorSource.FILE, accessType = "public")
        val privatePairId = createPair(source = ConnectorSource.FILE, accessType = "sync")
        saveDocument(publicPairId, "public")
        saveDocument(privatePairId, "private")

        worker.process(publicPairId)
        worker.process(privatePairId)

        assertAccess(publicPairId, "public", ExternalAccess(isPublic = true))
        assertAccess(privatePairId, "private", ExternalAccess(isPublic = false))
        verifyNoInteractions(remoteLoaders)
    }

    @Test
    fun failedPermissionLookupStoresPrivateAclAndPartialStatus() {
        val pairId = createPair()
        saveDocument(pairId, "unresolved")
        permissionLoad(
            ConnectorBatch(
                failures = listOf(
                    ConnectorFailure(FailureTarget.Document("unresolved"), "permission lookup failed"),
                ),
                checkpoint = checkpoint(),
            ),
        )

        worker.process(pairId)

        val privateAccess = ExternalAccess(isPublic = false)
        assertAccess(pairId, "unresolved", privateAccess)
        verify(indexer).updateAccess(pairId, mapOf("unresolved" to privateAccess))
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId).single().status)
            .isEqualTo(AttemptStatus.COMPLETED_WITH_ERRORS)
    }

    @Test
    fun fatalPermissionFailureLeavesPreviousAclUntouched() {
        val pairId = createPair()
        val previous = ExternalAccess(setOf("previous@example.com"), isPublic = false)
        saveDocument(pairId, "one", previous)
        doReturn(
            sequence {
                yield(
                    ConnectorBatch(
                        documents = listOf(permissionDocument("one", ExternalAccess(isPublic = true))),
                        checkpoint = checkpoint(hasMore = true),
                    ),
                )
                throw IllegalStateException("fatal permission failure")
            },
        ).`when`(remoteLoaders).loadSlim(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            start = null,
            end = null,
            includePermissions = true,
        )

        worker.process(pairId)

        assertAccess(pairId, "one", previous)
        verifyNoInteractions(indexer)
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.FAILED)
        assertThat(attempt.errorMessage).isEqualTo("fatal permission failure")
    }

    @Test
    fun openSearchFailureLeavesPreviousDatabaseAclUntouched() {
        val pairId = createPair()
        val previous = ExternalAccess(setOf("previous@example.com"), isPublic = false)
        val replacement = ExternalAccess(isPublic = true)
        saveDocument(pairId, "one", previous)
        permissionLoad(
            ConnectorBatch(
                documents = listOf(permissionDocument("one", replacement)),
                checkpoint = checkpoint(),
            ),
        )
        doThrow(IllegalStateException("OpenSearch unavailable"))
            .`when`(indexer).updateAccess(pairId, mapOf("one" to replacement))

        worker.process(pairId)

        assertAccess(pairId, "one", previous)
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.FAILED)
        assertThat(attempt.errorMessage).isEqualTo("OpenSearch unavailable")
    }

    @Test
    fun duplicateDocumentUpsertIsIdempotent() {
        val pairId = createPair()
        saveDocument(pairId, "one")
        val access = ExternalAccess(setOf("reader@example.com"), isPublic = false)
        permissionLoad(
            ConnectorBatch(
                documents = listOf(permissionDocument("one", access), permissionDocument("one", access)),
                checkpoint = checkpoint(),
            ),
        )

        worker.process(pairId)
        worker.process(pairId)

        assertThat(documents.countByCcPairId(pairId)).isEqualTo(1)
        assertAccess(pairId, "one", access)
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId)).hasSize(2)
    }

    @Test
    fun activeAttemptUniquenessPreventsConcurrentSync() {
        val pairId = createPair()
        saveDocument(pairId, "one")
        val loading = CountDownLatch(1)
        val release = CountDownLatch(1)
        doAnswer {
            sequence {
                loading.countDown()
                check(release.await(10, TimeUnit.SECONDS))
                yield(
                    ConnectorBatch(
                        documents = listOf(permissionDocument("one", ExternalAccess(isPublic = true))),
                        checkpoint = checkpoint(),
                    ),
                )
            }
        }.`when`(remoteLoaders).loadSlim(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            start = null,
            end = null,
            includePermissions = true,
        )
        val executor = Executors.newFixedThreadPool(2)
        try {
            val first = executor.submit { worker.process(pairId) }
            assertThat(loading.await(10, TimeUnit.SECONDS)).isTrue()
            val second = executor.submit { worker.process(pairId) }
            second.get(10, TimeUnit.SECONDS)
            assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId)).hasSize(1)

            release.countDown()
            first.get(10, TimeUnit.SECONDS)
        } finally {
            release.countDown()
            executor.shutdownNow()
        }

        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId)).hasSize(1)
    }

    @Test
    fun jiraPermissionSyncReceivesTheConnectorIndexingStart() {
        val indexingStart = Instant.parse("2025-06-01T00:00:00Z")
        val pairId = createPair(indexingStart = indexingStart)
        doReturn(sequenceOf(ConnectorBatch(checkpoint = checkpoint()))).`when`(remoteLoaders).loadSlim(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            start = indexingStart,
            end = null,
            includePermissions = true,
        )

        worker.process(pairId)

        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId).single().status).isEqualTo(AttemptStatus.SUCCESS)
    }

    private fun createPair(
        source: ConnectorSource = ConnectorSource.JIRA,
        accessType: String = "sync",
        indexingStart: Instant? = null,
    ): Long {
        val connector = connectors.save(
            ConnectorEntity(
                name = source.value,
                source = source,
                connectorSpecificConfig = mapper.createObjectNode(),
                indexingStart = indexingStart,
            ),
        )
        val credential = credentials.save(
            CredentialEntity(
                source = source,
                secretJson = cipher.encrypt(mapper.createObjectNode()),
            ),
        )
        return requireNotNull(
            pairs.save(
                ConnectorCredentialPairEntity(
                    connectorId = requireNotNull(connector.id),
                    credentialId = requireNotNull(credential.id),
                    name = source.value,
                    accessType = accessType,
                ),
            ).id,
        )
    }

    private fun saveDocument(pairId: Long, sourceDocumentId: String, access: ExternalAccess? = null) {
        documents.save(
            IndexedDocumentEntity(
                ccPairId = pairId,
                sourceDocumentId = sourceDocumentId,
                title = sourceDocumentId,
                contentHash = sourceDocumentId,
                metadata = mapper.createObjectNode(),
                externalAccess = access?.let(mapper::valueToTree),
            ),
        )
    }

    private fun permissionLoad(vararg batches: ConnectorBatch) {
        doReturn(batches.asSequence()).`when`(remoteLoaders).loadSlim(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            start = null,
            end = null,
            includePermissions = true,
        )
    }

    private fun permissionDocument(id: String, access: ExternalAccess?): SourceDocument =
        SourceDocument(id = id, title = id, content = "", externalAccess = access)

    private fun checkpoint(hasMore: Boolean = false): ConnectorCheckpoint =
        ConnectorCheckpoint(mapper.createObjectNode().put("hasMore", hasMore), hasMore)

    private fun assertAccess(pairId: Long, sourceDocumentId: String, expected: ExternalAccess) {
        val stored = documents.findByCcPairIdAndSourceDocumentId(pairId, sourceDocumentId)?.externalAccess
        assertThat(mapper.treeToValue(stored, ExternalAccess::class.java)).isEqualTo(expected)
    }

    private fun getJson(path: String): JsonNode {
        val response = mvc.perform(get(path)).andReturn().response
        assertThat(response.status).isEqualTo(200)
        return mapper.readTree(response.contentAsString)
    }
}
