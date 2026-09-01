package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
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
import com.onyx.foss.kotlin.domain.PermissionSyncStageRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mockingDetails
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.web.reactive.function.client.WebClient
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@AutoConfigureMockMvc
class PermissionSyncIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var worker: PermissionSyncWorker
    @Autowired private lateinit var claims: PermissionSyncClaimService
    @Autowired private lateinit var mvc: MockMvc
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var admin: AdminService
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var attempts: PermissionSyncAttemptRepository
    @Autowired private lateinit var staging: PermissionSyncStageRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var remoteLoaders: RemoteConnectorLoaders
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE permission_sync_staging, permission_sync_attempts, ingestion_errors, ingestion_jobs, ingestion_attempts, " +
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
                fullExceptionTrace = "trace",
                totalDocsSynced = 7,
                docsWithPermissionErrors = 2,
            ),
        )
        attempts.save(PermissionSyncAttemptEntity(ccPairId = pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))

        val items = getJson("/manage/admin/cc-pair/$pairId/permission-sync-attempts").path("items")

        assertThat(items.map { it.path("status").asText() })
            .containsExactly("completed_with_errors", "failed")
        assertThat(items.get(1).path("error_message").asText()).isEqualTo("fatal lookup")
        assertThat(items.get(1).path("full_exception_trace").asText()).isEqualTo("trace")
        assertThat(items.get(1).path("total_docs_synced").asInt()).isEqualTo(7)
        assertThat(items.get(1).path("docs_with_permission_errors").asInt()).isEqualTo(2)
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
        val privateAccess = ExternalAccess(isPublic = false)
        inOrder(indexer).apply {
            verify(indexer).updateAccess(pairId, mapOf("one" to privateAccess, "two" to privateAccess))
            verify(indexer).updateAccess(pairId, mapOf("one" to firstAccess, "two" to secondAccess))
        }
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.SUCCESS)
        assertThat(attempt.totalDocsSynced).isEqualTo(2)
        assertThat(attempt.docsWithPermissionErrors).isZero()
        assertThat(staging.countForAttempt(requireNotNull(attempt.id))).isZero()
    }

    @Test
    fun durablePermissionWorkWaitsForScheduledWorker() {
        val pairId = createPair()
        saveDocument(pairId, "one")

        worker.enqueue(pairId)

        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.NOT_STARTED)
        assertThat(documents.findByCcPairIdAndSourceDocumentId(pairId, "one")?.externalAccess).isNull()
        verifyNoInteractions(indexer, remoteLoaders)
    }

    @Test
    fun crashedPermissionAttemptIsReclaimedWithFreshToken() {
        val pairId = createPair()
        val now = Instant.parse("2026-09-01T00:00:00Z")
        worker.enqueue(pairId)
        val first = requireNotNull(claims.claimNext(now))
        jdbc.update(
            "UPDATE permission_sync_attempts SET lease_expires_at = ? WHERE id = ?",
            java.sql.Timestamp.from(now.minusSeconds(1)),
            first.attemptId,
        )

        val reclaimed = requireNotNull(claims.claimNext(now.plusSeconds(1)))

        assertThat(reclaimed.attemptId).isEqualTo(first.attemptId)
        assertThat(reclaimed.token).isNotEqualTo(first.token)
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId)).hasSize(1)
    }

    @Test
    fun reclaimedAttemptStopsStaleWorkerBeforeAnotherOpenSearchWrite() {
        val pairId = createPair()
        saveDocument(pairId, "one")
        permissionLoad(
            ConnectorBatch(
                documents = listOf(permissionDocument("one", ExternalAccess(isPublic = true))),
                checkpoint = checkpoint(),
            ),
        )
        worker.enqueue(pairId)
        val oldClaim = requireNotNull(claims.claimNext())
        val firstWriteStarted = CountDownLatch(1)
        val releaseFirstWrite = CountDownLatch(1)
        val writes = AtomicInteger()
        doAnswer {
            if (writes.incrementAndGet() == 1) {
                firstWriteStarted.countDown()
                check(releaseFirstWrite.await(10, TimeUnit.SECONDS))
            }
            Unit
        }.`when`(indexer).updateAccess(org.mockito.ArgumentMatchers.eq(pairId), org.mockito.ArgumentMatchers.anyMap())
        val executor = Executors.newSingleThreadExecutor()
        try {
            val staleWorker = executor.submit { worker.process(oldClaim) }
            assertThat(firstWriteStarted.await(10, TimeUnit.SECONDS)).isTrue()
            jdbc.update(
                "UPDATE permission_sync_attempts SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?",
                oldClaim.attemptId,
            )
            val reclaimed = requireNotNull(claims.claimNext())
            assertThat(reclaimed.token).isNotEqualTo(oldClaim.token)
            releaseFirstWrite.countDown()
            staleWorker.get(10, TimeUnit.SECONDS)
        } finally {
            releaseFirstWrite.countDown()
            executor.shutdownNow()
        }

        assertThat(writes.get()).isEqualTo(1)
        assertThat(attempts.findById(oldClaim.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.IN_PROGRESS)
    }

    @Test
    fun externalTimeoutIsBelowPermissionLease() {
        assertThat(OPENSEARCH_TIMEOUT).isLessThan(PERMISSION_SYNC_LEASE)
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
        verify(indexer, times(2)).updateAccess(pairId, mapOf("unresolved" to privateAccess))
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.COMPLETED_WITH_ERRORS)
        assertThat(attempt.totalDocsSynced).isEqualTo(1)
        assertThat(attempt.docsWithPermissionErrors).isEqualTo(1)
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
        assertThat(attempt.fullExceptionTrace).contains("IllegalStateException", "fatal permission failure")
        assertThat(staging.countForAttempt(requireNotNull(attempt.id))).isZero()
    }

    @Test
    fun failedFinalPublicationKeepsCommittedTargetWhenRecoveryFenceFails() = MockWebServer().use { server ->
        server.start()
        val pairId = createPair()
        val replacement = ExternalAccess(isPublic = true)
        saveDocument(pairId, "one")
        assertThat(documents.findByCcPairIdAndSourceDocumentId(pairId, "one")?.externalAccess).isNull()
        permissionLoad(
            ConnectorBatch(
                documents = listOf(permissionDocument("one", replacement)),
                checkpoint = checkpoint(),
            ),
        )
        enqueueKeywordMapping(server)
        server.enqueue(successfulOpenSearchUpdate())
        server.enqueue(
            MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(
                """{"timed_out":false,"total":1,"updated":0,"noops":0,"version_conflicts":0,"failures":[]}""",
            ),
        )
        server.enqueue(MockResponse().setResponseCode(500).setBody("recovery fence unavailable"))

        realWorker(server).process(pairId)

        assertAccess(pairId, "one", replacement)
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.FAILED)
        assertThat(attempt.errorMessage).contains("OpenSearch did not fully apply")
        val privateAccess = ExternalAccess(isPublic = false)
        assertThat(openSearchAclBodies(server, 3)).containsExactly(
            aclBody(privateAccess),
            aclBody(replacement),
            aclBody(privateAccess),
        )
        Unit
    }

    @Test
    fun databaseFailureAfterPrivateFenceDoesNotPublishNewAcl() = MockWebServer().use { server ->
        server.start()
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
        enqueueKeywordMapping(server)
        repeat(3) { server.enqueue(successfulOpenSearchUpdate()) }
        jdbc.execute(
            """
                CREATE FUNCTION reject_permission_acl_update() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    RAISE EXCEPTION 'database ACL write failed';
                END;
                ${'$'}${'$'} LANGUAGE plpgsql
            """.trimIndent(),
        )
        jdbc.execute(
            """
                CREATE TRIGGER reject_permission_acl_update
                BEFORE UPDATE OF external_access ON indexed_documents
                FOR EACH ROW EXECUTE FUNCTION reject_permission_acl_update()
            """.trimIndent(),
        )
        try {
            realWorker(server).process(pairId)
        } finally {
            jdbc.execute("DROP TRIGGER reject_permission_acl_update ON indexed_documents")
            jdbc.execute("DROP FUNCTION reject_permission_acl_update()")
        }

        assertAccess(pairId, "one", previous)
        val privateAccess = ExternalAccess(isPublic = false)
        assertThat(openSearchAclBodies(server, 3)).containsExactly(
            aclBody(privateAccess),
            aclBody(privateAccess),
            aclBody(previous),
        )
        assertThat(attempts.findAllByCcPairIdOrderByIdDesc(pairId).single().status).isEqualTo(AttemptStatus.FAILED)
        Unit
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
    fun staleActiveAttemptsAreReclaimedInPlace() {
        listOf(AttemptStatus.NOT_STARTED, AttemptStatus.IN_PROGRESS).forEach { status ->
            val pairId = createPair()
            saveDocument(pairId, "${status.value}-document")
            val stale = attempts.save(
                PermissionSyncAttemptEntity(
                    ccPairId = pairId,
                    status = status,
                    timeStarted = if (status == AttemptStatus.IN_PROGRESS) Instant.now().minusSeconds(7200) else null,
                ),
            )
            jdbc.update(
                "UPDATE permission_sync_attempts SET created_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minusSeconds(7200)),
                requireNotNull(stale.id),
            )
            permissionLoad(ConnectorBatch(checkpoint = checkpoint()))

            worker.process(pairId)

            val saved = attempts.findAllByCcPairIdOrderByIdDesc(pairId)
            assertThat(saved).hasSize(1)
            assertThat(saved.single().status).isEqualTo(AttemptStatus.SUCCESS)
        }
    }

    @Test
    fun stagesAndPublishesMoreThanOneBoundedPage() {
        val pairId = createPair()
        val access = ExternalAccess(setOf("reader@example.com"), isPublic = false)
        val returned = (1..501).map { index -> permissionDocument("document-${index.toString().padStart(3, '0')}", access) }
        returned.forEach { saveDocument(pairId, it.id) }
        permissionLoad(ConnectorBatch(documents = returned, checkpoint = checkpoint()))

        worker.process(pairId)

        val requestSizes = mockingDetails(indexer).invocations
            .filter { it.method.name == "updateAccess" }
            .map { invocation -> (invocation.arguments[1] as Map<*, *>).size }
        assertThat(requestSizes).containsExactly(500, 500, 1, 1)
        assertThat(documents.findAllByCcPairId(pairId).map { mapper.treeToValue(it.externalAccess, ExternalAccess::class.java) })
            .allMatch(access::equals)
        val attempt = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        assertThat(attempt.totalDocsSynced).isEqualTo(501)
        assertThat(staging.countForAttempt(requireNotNull(attempt.id))).isZero()
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

    private fun realWorker(server: MockWebServer): PermissionSyncWorker = PermissionSyncWorker(
        admin,
        pairs,
        attempts,
        claims,
        documents,
        staging,
        remoteLoaders,
        OpenSearchIndexer(
            OnyxProperties(
                opensearch = OnyxProperties.OpenSearch(
                    baseUrl = server.url("/").toString().trimEnd('/'),
                    index = "documents",
                ),
            ),
            WebClient.builder(),
            mapper,
        ),
        mapper,
    )

    private fun successfulOpenSearchUpdate(): MockResponse = MockResponse()
        .setResponseCode(200)
        .setHeader("Content-Type", "application/json")
        .setBody("""{"timed_out":false,"total":1,"updated":1,"noops":0,"version_conflicts":0,"failures":[]}""")

    private fun enqueueKeywordMapping(server: MockWebServer) {
        server.enqueue(MockResponse().setResponseCode(200))
        server.enqueue(
            MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                .setBody("""{"documents":{"mappings":{"properties":{"source_document_id":{"type":"keyword"}}}}}"""),
        )
    }

    private fun openSearchAclBodies(server: MockWebServer, count: Int): List<JsonNode> {
        server.takeRequest()
        server.takeRequest()
        return (1..count).map {
        mapper.readTree(server.takeRequest().body.readUtf8())
            .path("script").path("params").path("access_by_document").path("one")
        }
    }

    private fun aclBody(access: ExternalAccess): JsonNode = mapper.valueToTree(
        mapOf(
            "external_user_emails" to access.externalUserEmails,
            "external_user_group_ids" to access.externalUserGroupIds,
            "is_public" to access.isPublic,
        ),
    )

    private fun getJson(path: String): JsonNode {
        val response = mvc.perform(get(path)).andReturn().response
        assertThat(response.status).isEqualTo(200)
        return mapper.readTree(response.contentAsString)
    }
}
