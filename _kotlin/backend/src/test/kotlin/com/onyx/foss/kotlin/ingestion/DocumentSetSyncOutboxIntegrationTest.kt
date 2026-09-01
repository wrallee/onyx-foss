package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.api.DocumentSetRequest
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxEntity
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncStatus
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.support.H2IntegrationTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.QueueDispatcher
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class DocumentSetSyncOutboxIntegrationTest : H2IntegrationTest() {
    @Autowired private lateinit var admin: AdminService
    @Autowired private lateinit var worker: DocumentSetSyncWorker
    @Autowired private lateinit var claims: DocumentSetSyncClaimService
    @Autowired private lateinit var indexer: OpenSearchIndexer
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var sets: DocumentSetRepository
    @Autowired private lateinit var outbox: DocumentSetSyncOutboxRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun resetDatabase() {
        while (server.takeRequest(1, TimeUnit.MILLISECONDS) != null) {
            // Drain requests from the prior test.
        }
        truncateTables(
            "document_set_sync_outbox", "ingestion_errors", "ingestion_jobs", "ingestion_attempts",
            "ingestion_checkpoints", "indexed_documents", "document_set_cc_pairs", "document_sets",
            "connector_credential_pairs", "connectors", "credentials",
        )
        primeOpenSearchIndex()
    }

    private fun primeOpenSearchIndex() {
        if (!indexPrimed.compareAndSet(false, true)) return
        try {
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(
                MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                    .setBody(
                        """{"documents":{"mappings":{"properties":{"source_document_id":{"type":"keyword"}}}}}""",
                    ),
            )
            server.enqueue(MockResponse().setResponseCode(200))
            server.enqueue(success(1))
            indexer.updateDocumentSets(0, setOf("index-probe"), emptyList())
            while (server.takeRequest(1, TimeUnit.MILLISECONDS) != null) {
                // Keep index initialization outside outbox request assertions.
            }
        } catch (error: Exception) {
            indexPrimed.set(false)
            throw error
        }
    }

    @Test
    fun adminCommitsWhileOpenSearchIsUnavailable() {
        val pairId = createPair()
        saveDocuments(pairId, 1)
        val requestsBefore = server.requestCount

        val setId = admin.createSet(DocumentSetRequest(name = "committed", ccPairIds = listOf(pairId)))

        assertThat(sets.existsById(setId)).isTrue()
        val pending = outbox.findAll().single()
        assertThat(pending.status).isEqualTo(DocumentSetSyncStatus.PENDING)
        assertThat(pending.ccPairIds?.map { it.asLong() }).containsExactly(pairId)
        assertThat(server.requestCount).isEqualTo(requestsBefore)
    }

    @Test
    fun pageFailureStaysPendingAndRetryReplaysFromTheBeginning() {
        val pairId = createPair()
        saveDocuments(pairId, 501)
        admin.createSet(DocumentSetRequest(name = "replay", ccPairIds = listOf(pairId)))
        server.enqueue(success(500))
        server.enqueue(MockResponse().setResponseCode(503).setBody("unavailable"))

        assertThat(worker.processNext()).isTrue()

        val failed = outbox.findAll().single()
        assertThat(failed.status).isEqualTo(DocumentSetSyncStatus.PENDING)
        assertThat(failed.attemptCount).isEqualTo(1)
        assertThat(failed.lastError).contains("unavailable")
        val firstPage = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
        requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))

        server.enqueue(success(500))
        server.enqueue(success(1))
        assertThat(worker.processNext()).isTrue()

        val completed = outbox.findAll().single()
        assertThat(completed.status).isEqualTo(DocumentSetSyncStatus.DONE)
        assertThat(completed.attemptCount).isEqualTo(2)
        assertThat(completed.lastError).isNull()
        val replayedFirstPage = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
        val replayedLastPage = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
        assertThat(sourceIds(firstPage)).hasSize(500)
        assertThat(sourceIds(replayedFirstPage)).containsExactlyInAnyOrderElementsOf(sourceIds(firstPage))
        assertThat(sourceIds(replayedLastPage)).hasSize(1)
    }

    @Test
    fun concurrentClaimsReturnOneOutboxRow() {
        outbox.save(
            DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(1L))),
        )
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val results = List(2) {
                executor.submit<Long?> {
                    start.await()
                    claims.claimNext()?.id
                }
            }
            start.countDown()

            assertThat(results.map { it.get(10, TimeUnit.SECONDS) }.filterNotNull()).containsExactly(1L)
            assertThat(outbox.findById(1L).orElseThrow().attemptCount).isEqualTo(1)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun freshLeaseBlocksNewerPendingRows() {
        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(1L))))
        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(2L))))
        val now = Instant.parse("2026-09-01T00:00:00Z")

        val first = claims.claimNext(now)
        val blocked = claims.claimNext(now.plusSeconds(59 * 60))

        assertThat(first?.id).isEqualTo(1L)
        assertThat(blocked).isNull()
        assertThat(outbox.findById(1L).orElseThrow().status).isEqualTo(DocumentSetSyncStatus.IN_PROGRESS)
        assertThat(outbox.findById(2L).orElseThrow().status).isEqualTo(DocumentSetSyncStatus.PENDING)
    }

    @Test
    fun currentTokenCanRenewCompleteAndRetry() {
        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(1L))))
        val now = Instant.parse("2026-09-01T00:00:00Z")
        val completedClaim = requireNotNull(claims.claimNext(now))

        assertThat(claims.renew(completedClaim.id, completedClaim.token, now.plusSeconds(30))).isTrue()
        assertThat(claims.complete(completedClaim.id, completedClaim.token)).isTrue()
        assertThat(outbox.findById(completedClaim.id).orElseThrow().status).isEqualTo(DocumentSetSyncStatus.DONE)

        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(2L))))
        val retriedClaim = requireNotNull(claims.claimNext(now.plusSeconds(60)))
        assertThat(claims.retry(retriedClaim.id, retriedClaim.token, IllegalStateException("retry me"))).isTrue()
        val retried = outbox.findById(retriedClaim.id).orElseThrow()
        assertThat(retried.status).isEqualTo(DocumentSetSyncStatus.PENDING)
        assertThat(retried.lastError).isEqualTo("retry me")
    }

    @Test
    fun staleTokenCannotRenewCompleteOrRetryReclaimedWork() {
        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(1L))))
        val old = Instant.parse("2026-09-01T00:00:00Z")
        val stale = requireNotNull(claims.claimNext(old))
        val owner = requireNotNull(claims.claimNext(old.plusSeconds(61 * 60)))

        assertThat(owner.id).isEqualTo(stale.id)
        assertThat(owner.token).isNotEqualTo(stale.token)
        assertThat(claims.renew(stale.id, stale.token, old.plusSeconds(62 * 60))).isFalse()
        assertThat(claims.complete(stale.id, stale.token)).isFalse()
        assertThat(claims.retry(stale.id, stale.token, IllegalStateException("stale"))).isFalse()
        val row = outbox.findById(owner.id).orElseThrow()
        assertThat(row.status).isEqualTo(DocumentSetSyncStatus.IN_PROGRESS)
        assertThat(row.claimToken).isEqualTo(owner.token)
    }

    @Test
    fun crashedStaleClaimIsReclaimedBeforeNewerWorkAndCompletes() {
        val pairId = createPair()
        saveDocuments(pairId, 1)
        admin.createSet(DocumentSetRequest(name = "stale", ccPairIds = listOf(pairId)))
        outbox.save(DocumentSetSyncOutboxEntity(ccPairIds = mapper.valueToTree(listOf(pairId))))
        val old = Instant.now().minusSeconds(2 * 60 * 60)
        assertThat(claims.claimNext(old)?.id).isEqualTo(1L)
        server.enqueue(success(1))

        assertThat(worker.processNext()).isTrue()

        val reclaimed = outbox.findById(1L).orElseThrow()
        assertThat(reclaimed.status).isEqualTo(DocumentSetSyncStatus.DONE)
        assertThat(reclaimed.attemptCount).isEqualTo(2)
        assertThat(outbox.findById(2L).orElseThrow().status).isEqualTo(DocumentSetSyncStatus.PENDING)
    }

    @Test
    fun twoWorkersPreserveEventOrderAndLatestCommittedState() {
        val pairId = createPair()
        saveDocuments(pairId, 1)
        val setId = admin.createSet(DocumentSetRequest(name = "old", ccPairIds = listOf(pairId)))
        val requestsBefore = server.requestCount
        val firstRequestStarted = CountDownLatch(1)
        val releaseFirstResponse = CountDownLatch(1)
        val firstWorkerCompleted = CountDownLatch(1)
        val secondClaimAttempted = CountDownLatch(1)
        val secondClaimedWhileFirstActive = AtomicBoolean(true)
        server.dispatcher = object : Dispatcher() {
            private var count = 0

            override fun dispatch(request: RecordedRequest): MockResponse {
                count += 1
                if (count == 1) {
                    firstRequestStarted.countDown()
                    releaseFirstResponse.await(10, TimeUnit.SECONDS)
                }
                return success(1)
            }
        }
        val executor = Executors.newFixedThreadPool(2)
        try {
            val firstWorker = executor.submit<Boolean> {
                try {
                    worker.processNext()
                } finally {
                    firstWorkerCompleted.countDown()
                }
            }
            assertThat(firstRequestStarted.await(10, TimeUnit.SECONDS)).isTrue()
            admin.updateSet(DocumentSetRequest(id = setId, name = "latest", ccPairIds = listOf(pairId)))
            val secondWorker = executor.submit<Boolean> {
                secondClaimedWhileFirstActive.set(worker.processNext())
                secondClaimAttempted.countDown()
                firstWorkerCompleted.await(10, TimeUnit.SECONDS)
                worker.processNext()
            }

            assertThat(secondClaimAttempted.await(10, TimeUnit.SECONDS)).isTrue()
            assertThat(secondClaimedWhileFirstActive).isFalse()
            assertThat(server.requestCount).isEqualTo(requestsBefore + 1)
            releaseFirstResponse.countDown()
            assertThat(firstWorker.get(10, TimeUnit.SECONDS)).isTrue()
            assertThat(secondWorker.get(10, TimeUnit.SECONDS)).isTrue()

            val firstRequest = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
            val secondRequest = requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))
            assertThat(documentSetNames(firstRequest)).containsExactly("old")
            assertThat(documentSetNames(secondRequest)).containsExactly("latest")
            assertThat(outbox.findAll().map { it.id to it.status }).containsExactly(
                1L to DocumentSetSyncStatus.DONE,
                2L to DocumentSetSyncStatus.DONE,
            )
        } finally {
            releaseFirstResponse.countDown()
            executor.shutdownNow()
            server.dispatcher = QueueDispatcher()
        }
    }

    @Test
    fun reclaimedTokenFencesLateWorkerAndPreservesLatestFinalState() {
        val pairId = createPair()
        saveDocuments(pairId, 501)
        val setId = admin.createSet(DocumentSetRequest(name = "old", ccPairIds = listOf(pairId)))
        val claimA = requireNotNull(claims.claimNext())
        val requestsBefore = server.requestCount
        val firstRequestStarted = CountDownLatch(1)
        val releaseFirstResponse = CountDownLatch(1)
        server.dispatcher = object : Dispatcher() {
            private var count = 0

            override fun dispatch(request: RecordedRequest): MockResponse {
                count += 1
                if (count == 1) {
                    firstRequestStarted.countDown()
                    releaseFirstResponse.await(10, TimeUnit.SECONDS)
                }
                return success(if (sourceIds(request).size == 500) 500 else 1)
            }
        }
        val executor = Executors.newSingleThreadExecutor()
        try {
            val workerA = executor.submit<Boolean> { worker.process(claimA) }
            assertThat(firstRequestStarted.await(10, TimeUnit.SECONDS)).isTrue()
            admin.updateSet(DocumentSetRequest(id = setId, name = "latest", ccPairIds = listOf(pairId)))
            jdbc.update(
                "UPDATE document_set_sync_outbox SET locked_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minusSeconds(2 * 60 * 60)),
                claimA.id,
            )
            val claimB = requireNotNull(claims.claimNext())

            assertThat(claimB.id).isEqualTo(claimA.id)
            assertThat(claimB.token).isNotEqualTo(claimA.token)
            assertThat(claims.complete(claimA.id, claimA.token)).isFalse()
            assertThat(claims.retry(claimA.id, claimA.token, IllegalStateException("late"))).isFalse()
            assertThat(claims.claimNext()).isNull()
            releaseFirstResponse.countDown()
            assertThat(workerA.get(10, TimeUnit.SECONDS)).isTrue()
            assertThat(server.requestCount).isEqualTo(requestsBefore + 1)

            assertThat(worker.process(claimB)).isTrue()
            val claimRowTwo = requireNotNull(claims.claimNext())
            assertThat(claimRowTwo.id).isEqualTo(2L)
            assertThat(worker.process(claimRowTwo)).isTrue()

            val requests = List(5) { requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)) }
            assertThat(documentSetNames(requests.first())).containsExactly("old")
            assertThat(requests.drop(1).map(::documentSetNames)).allSatisfy {
                assertThat(it).containsExactly("latest")
            }
            assertThat(documentSetNames(requests.last())).containsExactly("latest")
            assertThat(outbox.findAll().map { it.id to it.status }).containsExactly(
                1L to DocumentSetSyncStatus.DONE,
                2L to DocumentSetSyncStatus.DONE,
            )
        } finally {
            releaseFirstResponse.countDown()
            executor.shutdownNow()
            server.dispatcher = QueueDispatcher()
        }
    }

    @Test
    fun workerRecalculatesRenameAndDeleteFromCommittedState() {
        val pairId = createPair()
        saveDocuments(pairId, 1)
        val setId = admin.createSet(DocumentSetRequest(name = "old", ccPairIds = listOf(pairId)))
        admin.updateSet(DocumentSetRequest(id = setId, name = "renamed", ccPairIds = listOf(pairId)))
        server.enqueue(success(1))

        worker.processNext()

        assertThat(documentSetNames(requireNotNull(server.takeRequest(5, TimeUnit.SECONDS))))
            .containsExactly("renamed")
        admin.deleteSet(setId)
        server.enqueue(success(1))
        server.enqueue(success(1))

        worker.processNext()
        worker.processNext()

        assertThat(documentSetNames(requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)))).isEmpty()
        assertThat(documentSetNames(requireNotNull(server.takeRequest(5, TimeUnit.SECONDS)))).isEmpty()
        assertThat(outbox.findAll().map { it.status }).containsOnly(DocumentSetSyncStatus.DONE)
    }

    private fun createPair(): Long {
        val connector = connectors.save(
            ConnectorEntity(name = "file", source = ConnectorSource.FILE, connectorSpecificConfig = mapper.createObjectNode()),
        )
        val credential = credentials.save(
            CredentialEntity(source = ConnectorSource.FILE, secretJson = cipher.encrypt(mapper.createObjectNode())),
        )
        return requireNotNull(
            pairs.save(
                ConnectorCredentialPairEntity(
                    connectorId = requireNotNull(connector.id),
                    credentialId = requireNotNull(credential.id),
                    name = "file pair",
                ),
            ).id,
        )
    }

    private fun saveDocuments(pairId: Long, count: Int) {
        documents.saveAll(
            (1..count).map { number ->
                IndexedDocumentEntity(
                    ccPairId = pairId,
                    sourceDocumentId = "document-$number",
                    title = "title",
                    contentHash = "hash",
                    metadata = mapper.createObjectNode(),
                )
            },
        )
    }

    private fun success(total: Int): MockResponse = MockResponse()
        .setResponseCode(200)
        .setHeader("Content-Type", "application/json")
        .setBody("""{"timed_out":false,"total":$total,"updated":$total,"noops":0,"version_conflicts":0,"failures":[]}""")

    private fun sourceIds(request: okhttp3.mockwebserver.RecordedRequest): List<String> = mapper
        .readTree(request.body.clone().readUtf8())
        .path("query").path("bool").path("filter").get(1).path("terms").path("source_document_id")
        .map { it.asText() }

    private fun documentSetNames(request: okhttp3.mockwebserver.RecordedRequest): List<String> = mapper
        .readTree(request.body.clone().readUtf8())
        .path("script").path("params").path("document_sets")
        .map { it.asText() }

    companion object {
        private val server = MockWebServer().apply { start() }
        private val indexPrimed = AtomicBoolean()

        @JvmStatic
        @DynamicPropertySource
        fun opensearch(registry: DynamicPropertyRegistry) {
            registry.add("onyx.opensearch.base-url") { server.url("/").toString().trimEnd('/') }
            registry.add("onyx.opensearch.index") { "documents" }
        }

        @JvmStatic
        @AfterAll
        fun stopServer() {
            server.shutdown()
        }
    }
}
