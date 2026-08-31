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
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
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

class DocumentSetSyncOutboxIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var admin: AdminService
    @Autowired private lateinit var worker: DocumentSetSyncWorker
    @Autowired private lateinit var claims: DocumentSetSyncClaimService
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var sets: DocumentSetRepository
    @Autowired private lateinit var outbox: DocumentSetSyncOutboxRepository
    @Autowired private lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun resetDatabase() {
        while (server.takeRequest(1, TimeUnit.MILLISECONDS) != null) {
            // Drain requests from the prior test.
        }
        jdbc.execute(
            "TRUNCATE document_set_sync_outbox, ingestion_errors, ingestion_jobs, ingestion_attempts, " +
                "ingestion_checkpoints, indexed_documents, document_set_cc_pairs, document_sets, " +
                "connector_credential_pairs, connectors, credentials RESTART IDENTITY CASCADE",
        )
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
        jdbc.update(
            """
                INSERT INTO indexed_documents
                    (cc_pair_id, source_document_id, title, content_hash, metadata)
                SELECT ?, 'document-' || generate_series, 'title', 'hash', '{}'::jsonb
                FROM generate_series(1, ?)
            """.trimIndent(),
            pairId,
            count,
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
