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
import com.onyx.foss.kotlin.domain.IngestionAttemptEntity
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionCheckpointRepository
import com.onyx.foss.kotlin.domain.IngestionErrorEntity
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.IngestionJobEntity
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.Mockito.anyList
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class IngestionProcessorIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var processor: IngestionProcessor
    @Autowired private lateinit var claims: JobClaimService
    @Autowired private lateinit var scheduler: IngestionScheduler
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var attempts: IngestionAttemptRepository
    @Autowired private lateinit var jobs: IngestionJobRepository
    @Autowired private lateinit var checkpoints: IngestionCheckpointRepository
    @Autowired private lateinit var errors: IngestionErrorRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var fileLoader: FileConnectorLoader
    @MockitoBean private lateinit var remoteLoaders: RemoteConnectorLoaders
    @MockitoBean private lateinit var embedder: ModelServerClient
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE ingestion_errors, ingestion_jobs, ingestion_attempts, ingestion_checkpoints, " +
                "indexed_documents, connector_credential_pairs, connectors, credentials RESTART IDENTITY CASCADE",
        )
        doAnswer { invocation ->
            invocation.getArgument<List<String>>(0).map { listOf(0.1) }
        }.`when`(embedder).embed(anyList<String>())
    }

    @Test
    fun savesCheckpointAfterEachSuccessfulBatch() {
        val run = createRun()
        load(
            sequenceOf(
                batch(1, true, document("one")),
                batch(2, false, document("two")),
            ),
        )

        processor.process(run.jobId)

        assertThat(checkpoints.findById(run.pairId).orElseThrow().checkpointJson?.path("cursor")?.asInt()).isEqualTo(2)
        assertThat(documents.countByCcPairId(run.pairId)).isEqualTo(2)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.SUCCESS)
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.SUCCEEDED)
        assertThat(pairs.findById(run.pairId).orElseThrow().status).isEqualTo(PairStatus.ACTIVE)
    }

    @Test
    fun passesAttemptPollWindowToRemoteLoader() {
        val start = Instant.parse("2026-08-31T00:00:00Z")
        val end = Instant.parse("2026-09-01T00:00:00Z")
        val run = createRun(source = ConnectorSource.JIRA, pollRangeStart = start, pollRangeEnd = end)
        var observedStart: Instant? = null
        var observedEnd: Instant? = null
        doAnswer { invocation ->
            observedStart = invocation.getArgument(4)
            observedEnd = invocation.getArgument(5)
            sequenceOf(batch(1, false))
        }.`when`(remoteLoaders).load(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            null,
            start,
            end,
        )

        processor.process(run.jobId)

        assertThat(observedStart).isEqualTo(start)
        assertThat(observedEnd).isEqualTo(end)
        val savedAttempt = attempts.findById(run.attemptId).orElseThrow()
        assertThat(savedAttempt.pollRangeStart).isEqualTo(start)
        assertThat(savedAttempt.pollRangeEnd).isEqualTo(end)
    }

    @Test
    fun doesNotSaveFailedBatchCheckpoint() {
        val run = createRun()
        load(
            sequence {
                yield(batch(1, true, document("one")))
                throw IllegalStateException("second batch failed")
            },
        )

        processor.process(run.jobId)

        assertThat(checkpoints.findById(run.pairId).orElseThrow().checkpointJson?.path("cursor")?.asInt()).isEqualTo(1)
        assertThat(documents.countByCcPairId(run.pairId)).isEqualTo(1)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.FAILED)
    }

    @Test
    fun documentFailureFinishesCompletedWithErrors() {
        val run = createRun(fromBeginning = true, inRepeatedErrorState = true)
        saveDocument(run.pairId, "missing")
        load(
            sequenceOf(
                ConnectorBatch(
                    failures = listOf(
                        ConnectorFailure(
                            target = FailureTarget.Document("missing", "https://example.test/missing"),
                            message = "retrieval failed",
                            errorType = "RemoteFailure",
                        ),
                    ),
                    checkpoint = checkpoint(1, false),
                ),
            ),
        )

        processor.process(run.jobId)

        val error = errors.findAllByAttemptIdOrderByIdDesc(run.attemptId).single()
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.COMPLETED_WITH_ERRORS)
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.SUCCEEDED)
        assertThat(error.sourceDocumentId).isEqualTo("missing")
        assertThat(error.documentLink).isEqualTo("https://example.test/missing")
        assertThat(error.failureMessage).isEqualTo("retrieval failed")
        assertThat(error.errorType).isEqualTo("RemoteFailure")
        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "missing")).isNotNull()
        assertThat(pairs.findById(run.pairId).orElseThrow().lastPrunedAt).isNotNull()
        assertThat(pairs.findById(run.pairId).orElseThrow().inRepeatedErrorState).isFalse()
    }

    @Test
    fun fatalFailureFinishesFailed() {
        val run = createRun()
        load(sequence { throw IllegalStateException("fatal retrieval failure") })

        processor.process(run.jobId)

        val attempt = attempts.findById(run.attemptId).orElseThrow()
        val error = errors.findAllByAttemptIdOrderByIdDesc(run.attemptId).single()
        assertThat(attempt.status).isEqualTo(AttemptStatus.FAILED)
        assertThat(attempt.errorMessage).isEqualTo("fatal retrieval failure")
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.FAILED)
        assertThat(error.failureMessage).isEqualTo("fatal retrieval failure")
    }

    @Test
    fun successfulDocumentResolvesOnlyItsOwnPriorErrors() {
        val run = createRun()
        val priorAttempt = attempts.save(IngestionAttemptEntity(ccPairId = run.pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))
        val resolvedCandidate = errors.save(
            IngestionErrorEntity(attemptId = requireNotNull(priorAttempt.id), sourceDocumentId = "one", failureMessage = "old one"),
        )
        val unrelated = errors.save(
            IngestionErrorEntity(attemptId = requireNotNull(priorAttempt.id), sourceDocumentId = "two", failureMessage = "old two"),
        )
        load(sequenceOf(batch(1, false, document("one"))))

        processor.process(run.jobId)

        assertThat(errors.findById(requireNotNull(resolvedCandidate.id)).orElseThrow().isResolved).isTrue()
        assertThat(errors.findById(requireNotNull(unrelated.id)).orElseThrow().isResolved).isFalse()
    }

    @Test
    fun successfulAttemptResolvesPriorEntityErrors() {
        val run = createRun()
        val priorAttempt = attempts.save(IngestionAttemptEntity(ccPairId = run.pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))
        val entityError = errors.save(
            IngestionErrorEntity(attemptId = requireNotNull(priorAttempt.id), entityId = "space-1", failureMessage = "old entity"),
        )
        load(sequenceOf(batch(1, false)))

        processor.process(run.jobId)

        assertThat(errors.findById(requireNotNull(entityError.id)).orElseThrow().isResolved).isTrue()
    }

    @Test
    fun failedDocumentDoesNotResolveItsPriorErrorWhenAlsoReturned() {
        val run = createRun()
        val priorAttempt = attempts.save(IngestionAttemptEntity(ccPairId = run.pairId, status = AttemptStatus.COMPLETED_WITH_ERRORS))
        val priorError = errors.save(
            IngestionErrorEntity(attemptId = requireNotNull(priorAttempt.id), sourceDocumentId = "one", failureMessage = "old one"),
        )
        load(
            sequenceOf(
                ConnectorBatch(
                    documents = listOf(document("one")),
                    failures = listOf(
                        ConnectorFailure(FailureTarget.Document("one"), "still failed"),
                    ),
                    checkpoint = checkpoint(1, false),
                ),
            ),
        )

        processor.process(run.jobId)

        assertThat(errors.findById(requireNotNull(priorError.id)).orElseThrow().isResolved).isFalse()
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.COMPLETED_WITH_ERRORS)
    }

    @Test
    fun scheduledConnectorNeedsFiveConsecutiveFailures() {
        val failures = List(5) { IngestionAttemptEntity(status = AttemptStatus.FAILED) }

        assertThat(isRepeatedError(60, failures.take(4))).isFalse()
        assertThat(isRepeatedError(60, failures)).isTrue()
    }

    @Test
    fun manualConnectorNeedsOneFailure() {
        assertThat(isRepeatedError(null, listOf(IngestionAttemptEntity(status = AttemptStatus.FAILED)))).isTrue()
    }

    @Test
    fun successfulAttemptClearsRepeatedErrorState() {
        val run = createRun(inRepeatedErrorState = true)
        load(sequenceOf(batch(1, false)))

        processor.process(run.jobId)

        assertThat(pairs.findById(run.pairId).orElseThrow().inRepeatedErrorState).isFalse()
    }

    @Test
    fun fullRunPrunesAfterCompleteEnumeration() {
        val run = createRun(fromBeginning = true)
        saveDocument(run.pairId, "obsolete")
        load(sequenceOf(batch(1, false, document("seen"))))

        processor.process(run.jobId)

        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("seen")
        assertThat(attempts.findById(run.attemptId).orElseThrow().docsRemovedFromIndex).isEqualTo(1)
        assertThat(pairs.findById(run.pairId).orElseThrow().lastPrunedAt).isNotNull()
    }

    @Test
    fun pruningFailureDoesNotSetLastPrunedAt() {
        val run = createRun(fromBeginning = true)
        saveDocument(run.pairId, "obsolete")
        load(sequenceOf(batch(1, false)))
        doThrow(IllegalStateException("OpenSearch failed"))
            .`when`(indexer).deleteDocuments(run.pairId, setOf("obsolete"))

        processor.process(run.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "obsolete")).isNotNull()
        assertThat(pairs.findById(run.pairId).orElseThrow().lastPrunedAt).isNull()
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.FAILED)
    }

    @Test
    fun concurrentClaimsReturnOneJobId() {
        val run = createRun()
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val results = List(2) {
                executor.submit<Long?> {
                    start.await()
                    claims.claimNext()
                }
            }
            start.countDown()

            assertThat(results.map { it.get() }.filterNotNull()).containsExactly(run.jobId)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun queuesIncrementalRunWhenRefreshFrequencyIsDue() {
        val now = Instant.parse("2026-09-01T00:00:00Z")
        val pairId = createPair(refreshFreq = 60)
        val previous = attempts.save(IngestionAttemptEntity(ccPairId = pairId, status = AttemptStatus.SUCCESS))
        jdbc.update(
            "UPDATE ingestion_attempts SET time_updated = ? WHERE id = ?",
            Timestamp.from(now.minusSeconds(61)),
            requireNotNull(previous.id),
        )

        scheduler.scheduleDue(now)

        val queued = attempts.findAllByCcPairIdOrderByIdDesc(pairId).first()
        assertThat(queued.fromBeginning).isFalse()
        assertThat(queued.status).isEqualTo(AttemptStatus.NOT_STARTED)
        assertThat(jobs.count()).isEqualTo(1)
    }

    @Test
    fun doesNotQueueOverlappingRun() {
        createRun(refreshFreq = 1)

        scheduler.scheduleDue(Instant.now().plusSeconds(60))

        assertThat(attempts.count()).isEqualTo(1)
        assertThat(jobs.count()).isEqualTo(1)
    }

    @Test
    fun doesNotQueuePausedPair() {
        createPair(refreshFreq = 1, status = PairStatus.PAUSED)

        scheduler.scheduleDue(Instant.now().plusSeconds(60))

        assertThat(attempts.count()).isZero()
        assertThat(jobs.count()).isZero()
    }

    @Test
    fun scheduledPruneUsesPersistedSlimPathWithoutEmbeddingOrFullLoading() {
        val now = Instant.parse("2026-09-01T00:00:00Z")
        val pairId = createPair(
            pruneFreq = 60,
            lastPrunedAt = now.minusSeconds(61),
            source = ConnectorSource.GITHUB,
        )
        saveDocument(pairId, "seen")
        saveDocument(pairId, "obsolete")
        doReturn(
            sequenceOf(
                batch(1, false, SourceDocument(id = "seen", title = "", content = "")),
            ),
        ).`when`(remoteLoaders).loadSlim(
            ConnectorSource.GITHUB,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
        )

        scheduler.scheduleDue(now)
        val queued = attempts.findAllByCcPairIdOrderByIdDesc(pairId).single()
        processor.process(requireNotNull(jobs.findAll().single().id))

        val saved = attempts.findById(requireNotNull(queued.id)).orElseThrow()
        assertThat(saved.pruneOnly).isTrue()
        assertThat(saved.fromBeginning).isFalse()
        assertThat(saved.docsRemovedFromIndex).isEqualTo(1)
        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("seen")
        assertThat(pairs.findById(pairId).orElseThrow().lastPrunedAt).isNotNull()
        verify(remoteLoaders).loadSlim(
            ConnectorSource.GITHUB,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
        )
        verifyNoMoreInteractions(remoteLoaders)
        verifyNoInteractions(embedder)
        assertThat(jobs.count()).isEqualTo(1)
    }

    @Test
    fun concurrentSchedulerCallsQueueOneJob() {
        val now = Instant.parse("2026-09-01T00:00:00Z")
        createPair(refreshFreq = 1)
        jdbc.execute(
            """
                CREATE FUNCTION delay_ingestion_attempt_insert() RETURNS trigger AS ${'$'}${'$'}
                BEGIN
                    PERFORM pg_sleep(0.5);
                    RETURN NEW;
                END;
                ${'$'}${'$'} LANGUAGE plpgsql
            """.trimIndent(),
        )
        jdbc.execute(
            """
                CREATE TRIGGER delay_ingestion_attempt_insert
                BEFORE INSERT ON ingestion_attempts
                FOR EACH ROW EXECUTE FUNCTION delay_ingestion_attempt_insert()
            """.trimIndent(),
        )
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val results = List(2) {
                executor.submit {
                    start.await()
                    scheduler.scheduleDue(now)
                }
            }
            start.countDown()
            results.forEach { it.get(10, TimeUnit.SECONDS) }

            assertThat(attempts.count()).isEqualTo(1)
            assertThat(jobs.count()).isEqualTo(1)
        } finally {
            executor.shutdownNow()
            jdbc.execute("DROP TRIGGER delay_ingestion_attempt_insert ON ingestion_attempts")
            jdbc.execute("DROP FUNCTION delay_ingestion_attempt_insert()")
        }
    }

    private fun createRun(
        refreshFreq: Long? = null,
        fromBeginning: Boolean = false,
        inRepeatedErrorState: Boolean = false,
        source: ConnectorSource = ConnectorSource.FILE,
        pollRangeStart: Instant? = null,
        pollRangeEnd: Instant? = null,
    ): Run {
        val pairId = createPair(refreshFreq = refreshFreq, inRepeatedErrorState = inRepeatedErrorState, source = source)
        val attempt = attempts.save(
            IngestionAttemptEntity(
                ccPairId = pairId,
                fromBeginning = fromBeginning,
                pollRangeStart = pollRangeStart,
                pollRangeEnd = pollRangeEnd,
            ),
        )
        val job = jobs.save(IngestionJobEntity(attemptId = requireNotNull(attempt.id)))
        return Run(pairId, requireNotNull(attempt.id), requireNotNull(job.id))
    }

    private fun createPair(
        refreshFreq: Long? = null,
        pruneFreq: Long? = null,
        status: PairStatus = PairStatus.SCHEDULED,
        lastPrunedAt: Instant? = null,
        inRepeatedErrorState: Boolean = false,
        source: ConnectorSource = ConnectorSource.FILE,
    ): Long {
        val connector = connectors.save(
            ConnectorEntity(
                name = source.value,
                source = source,
                connectorSpecificConfig = mapper.createObjectNode(),
                refreshFreq = refreshFreq,
                pruneFreq = pruneFreq,
            ),
        )
        val credential = credentials.save(
            CredentialEntity(
                source = source,
                secretJson = cipher.encrypt(mapper.createObjectNode()),
            ),
        )
        val pair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = requireNotNull(connector.id),
                credentialId = requireNotNull(credential.id),
                name = "${source.value} pair",
                status = status,
                inRepeatedErrorState = inRepeatedErrorState,
                lastPrunedAt = lastPrunedAt,
            ),
        )
        return requireNotNull(pair.id)
    }

    private fun load(batches: Sequence<ConnectorBatch>) {
        doReturn(batches).`when`(fileLoader).load(any(JsonNode::class.java))
    }

    private fun batch(cursor: Int, hasMore: Boolean, vararg documents: SourceDocument): ConnectorBatch =
        ConnectorBatch(documents = documents.toList(), checkpoint = checkpoint(cursor, hasMore))

    private fun checkpoint(cursor: Int, hasMore: Boolean): ConnectorCheckpoint =
        ConnectorCheckpoint(mapper.createObjectNode().put("cursor", cursor), hasMore)

    private fun document(id: String): SourceDocument = SourceDocument(id = id, title = id, content = "$id content")

    private fun saveDocument(pairId: Long, sourceDocumentId: String) {
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

    private data class Run(val pairId: Long, val attemptId: Long, val jobId: Long)
}
