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
import com.onyx.foss.kotlin.domain.DocumentSetEntity
import com.onyx.foss.kotlin.domain.DocumentSetPairEntity
import com.onyx.foss.kotlin.domain.DocumentSetPairRepository
import com.onyx.foss.kotlin.domain.DocumentSetRepository
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
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.api.DeletionAttemptRequest
import com.onyx.foss.kotlin.support.H2IntegrationTest
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
import org.mockito.Mockito.never
import org.mockito.Mockito.mockingDetails
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class IngestionProcessorIntegrationTest : H2IntegrationTest() {
    @Autowired private lateinit var processor: IngestionProcessor
    @Autowired private lateinit var admin: AdminService
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
    @Autowired private lateinit var sets: DocumentSetRepository
    @Autowired private lateinit var setPairs: DocumentSetPairRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var fileLoader: FileConnectorLoader
    @MockitoBean private lateinit var remoteLoaders: RemoteConnectorLoaders
    @MockitoBean private lateinit var embedder: ModelServerClient
    @MockitoBean private lateinit var indexer: OpenSearchIndexer
    @MockitoBean private lateinit var permissionSync: PermissionSyncWorker

    @BeforeEach
    fun resetDatabase() {
        truncateTables(
            "document_set_sync_outbox", "document_set_cc_pairs", "document_sets", "ingestion_errors",
            "ingestion_jobs", "ingestion_attempts", "ingestion_checkpoints", "indexed_documents",
            "connector_credential_pairs", "connectors", "credentials",
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
        verify(permissionSync).enqueue(run.pairId)
    }

    @Test
    fun overlappingConnectorCreation() {
        val first = createRun()
        val second = createRun()
        load(sequenceOf(batch(1, false, document("shared"))))
        processor.process(first.jobId)
        load(sequenceOf(batch(1, false, document("shared"))))

        processor.process(second.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(first.pairId, "shared")).isNotNull()
        assertThat(documents.findByCcPairIdAndSourceDocumentId(second.pairId, "shared")).isNotNull()
        assertThat(documents.count()).isEqualTo(2)
        assertThat(attempts.findById(first.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.SUCCESS)
        assertThat(attempts.findById(second.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.SUCCESS)
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
    fun connectorPauseWhileIndexing() {
        val run = createRun()
        var requestedSecondBatch = false
        doAnswer {
            val pair = pairs.findById(run.pairId).orElseThrow()
            pair.status = PairStatus.PAUSED
            pairs.saveAndFlush(pair)
            listOf(listOf(0.1))
        }.`when`(embedder).embed(anyList<String>())
        load(
            sequence {
                yield(batch(1, true, document("one")))
                requestedSecondBatch = true
                yield(batch(2, false, document("two")))
            },
        )

        processor.process(run.jobId)

        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("one")
        assertThat(requestedSecondBatch).isFalse()
        assertThat(checkpoints.findById(run.pairId).orElseThrow().checkpointJson?.path("cursor")?.asInt()).isEqualTo(1)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.CANCELED)
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.SUCCEEDED)
        assertThat(pairs.findById(run.pairId).orElseThrow().status).isEqualTo(PairStatus.PAUSED)
        verifyNoInteractions(permissionSync)
    }

    @Test
    fun pollConnectorTimeRanges() {
        val first = createRun()
        load(sequenceOf(batch(1, false)))
        val beforeFirst = Instant.now()

        processor.process(first.jobId)

        val afterFirst = Instant.now()
        val firstAttempt = attempts.findById(first.attemptId).orElseThrow()
        assertThat(firstAttempt.pollRangeStart).isEqualTo(Instant.EPOCH)
        assertThat(firstAttempt.pollRangeEnd).isBetween(beforeFirst, afterFirst)

        val secondAttempt = attempts.save(IngestionAttemptEntity(ccPairId = first.pairId))
        val secondJob = jobs.save(IngestionJobEntity(attemptId = requireNotNull(secondAttempt.id), ccPairId = first.pairId))
        load(sequenceOf(batch(2, false)))
        val beforeSecond = Instant.now()

        processor.process(requireNotNull(secondJob.id))

        val afterSecond = Instant.now()
        val savedSecond = attempts.findById(requireNotNull(secondAttempt.id)).orElseThrow()
        assertThat(savedSecond.pollRangeStart).isEqualTo(requireNotNull(firstAttempt.pollRangeEnd).minusSeconds(30 * 60))
        assertThat(savedSecond.pollRangeEnd).isBetween(beforeSecond, afterSecond)
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
        verify(permissionSync).enqueue(run.pairId)
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
        verifyNoInteractions(permissionSync)
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
    fun persistsCanonicalEnumerationAndDoesNotPruneAfterIncompleteBatch() {
        val run = createRun(fromBeginning = true)
        val canonicalId = "https://example.test/wiki/page?id=ABC-123"
        saveDocument(run.pairId, "obsolete")
        load(
            sequenceOf(
                ConnectorBatch(
                    documents = listOf(SourceDocument(canonicalId, "page", "content")),
                    failures = listOf(
                        ConnectorFailure(FailureTarget.Entity("unknown-page"), "Page identifier was unavailable"),
                    ),
                    checkpoint = checkpoint(1, false),
                    enumerationComplete = false,
                ),
            ),
        )

        processor.process(run.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "obsolete")).isNotNull()
        assertThat(
            jdbc.queryForList(
                "SELECT source_document_id FROM ingestion_enumerated_documents WHERE attempt_id = ?",
                String::class.java,
                run.attemptId,
            ),
        ).containsExactly(canonicalId)
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
    fun staleChunkCleanupFailureDoesNotCommitDocumentOrSuccessfulAttempt() {
        val run = createRun()
        load(sequenceOf(batch(1, false, document("one"))))
        doThrow(IllegalStateException("tail cleanup failed"))
            .`when`(indexer).deleteStaleChunks(run.pairId, "one", 1)

        processor.process(run.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "one")).isNull()
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.FAILED)
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.FAILED)
        verifyNoInteractions(permissionSync)
    }

    @Test
    fun embeddingCountMismatchDoesNotReplaceOrCommitTheDocument() {
        val run = createRun()
        load(
            sequenceOf(
                batch(
                    1,
                    false,
                    SourceDocument(id = "one", title = "one", content = "x".repeat(1501)),
                ),
            ),
        )
        doReturn(listOf(listOf(0.1))).`when`(embedder).embed(anyList<String>())

        processor.process(run.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "one")).isNull()
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.FAILED)
        assertThat(attempts.findById(run.attemptId).orElseThrow().errorMessage).contains("embedding")
        verifyNoInteractions(indexer, permissionSync)
    }

    @Test
    fun newAndReindexedDocumentsKeepCurrentDocumentSetMembership() {
        val run = createRun()
        saveDocument(run.pairId, "reindexed")
        val setId = requireNotNull(sets.save(DocumentSetEntity(name = "Engineering")).id)
        setPairs.save(DocumentSetPairEntity(setId, run.pairId))
        load(sequenceOf(batch(1, false, document("new"), document("reindexed"))))

        processor.process(run.jobId)

        val indexedMemberships = mockingDetails(indexer).invocations
            .filter { it.method.name == "upsert" }
            .map { it.arguments.getOrNull(8) }
        assertThat(indexedMemberships).containsExactly(listOf("Engineering"), listOf("Engineering"))
    }

    @Test
    fun fileAndRemoteSourceMetadataReachPostgresAndOpenSearch() {
        val fileUpdatedAt = Instant.parse("2026-08-01T00:00:00Z")
        val fileRun = createRun()
        load(
            sequenceOf(
                batch(
                    1,
                    false,
                    SourceDocument(
                        id = "file",
                        title = "file",
                        content = "file content",
                        updatedAt = fileUpdatedAt,
                        primaryOwners = listOf("file-owner@example.com"),
                        secondaryOwners = listOf("file-reviewer@example.com"),
                    ),
                ),
            ),
        )
        processor.process(fileRun.jobId)

        val remoteUpdatedAt = Instant.parse("2026-08-02T00:00:00Z")
        val pollStart = Instant.parse("2026-07-01T00:00:00Z")
        val pollEnd = Instant.parse("2026-09-01T00:00:00Z")
        val remoteRun = createRun(source = ConnectorSource.JIRA, pollRangeStart = pollStart, pollRangeEnd = pollEnd)
        doReturn(
            sequenceOf(
                batch(
                    1,
                    false,
                    SourceDocument(
                        id = "remote",
                        title = "remote",
                        content = "remote content",
                        updatedAt = remoteUpdatedAt,
                        primaryOwners = listOf("remote-owner@example.com"),
                        secondaryOwners = listOf("remote-reviewer@example.com"),
                    ),
                ),
            ),
        ).`when`(remoteLoaders).load(
            ConnectorSource.JIRA,
            mapper.createObjectNode(),
            mapper.createObjectNode(),
            null,
            pollStart,
            pollEnd,
        )
        processor.process(remoteRun.jobId)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(fileRun.pairId, "file")?.lastModified)
            .isEqualTo(fileUpdatedAt)
        assertThat(documents.findByCcPairIdAndSourceDocumentId(remoteRun.pairId, "remote")?.lastModified)
            .isEqualTo(remoteUpdatedAt)
        val storedOwners = documents.findAll().sortedBy { it.sourceDocumentId }
        assertThat(storedOwners.map { it.primaryOwners }).containsExactly(
            listOf("file-owner@example.com"),
            listOf("remote-owner@example.com"),
        )
        assertThat(storedOwners.map { it.secondaryOwners }).containsExactly(
            listOf("file-reviewer@example.com"),
            listOf("remote-reviewer@example.com"),
        )
        val indexedMetadata = mockingDetails(indexer).invocations
            .filter { it.method.name == "upsert" }
            .map { listOf(it.arguments.getOrNull(9), it.arguments.getOrNull(10), it.arguments.getOrNull(11)) }
        assertThat(indexedMetadata).containsExactly(
            listOf(fileUpdatedAt, listOf("file-owner@example.com"), listOf("file-reviewer@example.com")),
            listOf(remoteUpdatedAt, listOf("remote-owner@example.com"), listOf("remote-reviewer@example.com")),
        )
    }

    @Test
    fun enqueuesDurablePermissionWorkBeforeIngestionBecomesSuccessful() {
        val run = createRun()
        load(sequenceOf(batch(1, false, document("one"))))
        doAnswer {
            assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.IN_PROGRESS)
            null
        }.`when`(permissionSync).enqueue(run.pairId)

        processor.process(run.jobId)

        verify(permissionSync).enqueue(run.pairId)
        verify(permissionSync, never()).process(run.pairId)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.SUCCESS)
    }

    @Test
    fun concurrentClaimsReturnOneJobId() {
        val run = createRun()
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val results = List(2) {
                executor.submit<IngestionClaim?> {
                    start.await()
                    claims.claimNext()
                }
            }
            start.countDown()

            assertThat(results.mapNotNull { it.get()?.jobId }).containsExactly(run.jobId)
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun crashedIngestionJobIsReclaimedWithFreshPairToken() {
        val run = createRun()
        val now = jobs.findById(run.jobId).orElseThrow().runAfter.plusSeconds(1)
        val first = requireNotNull(claims.claimNext(now))
        jdbc.update(
            "UPDATE ingestion_jobs SET lease_expires_at = ? WHERE id = ?",
            Timestamp.from(now.minusSeconds(1)),
            first.jobId,
        )
        jdbc.update(
            "UPDATE connector_credential_pairs SET ingestion_lease_expires_at = ? WHERE id = ?",
            Timestamp.from(now.minusSeconds(1)),
            run.pairId,
        )

        val reclaimed = requireNotNull(claims.claimNext(now.plusSeconds(1)))

        assertThat(reclaimed.jobId).isEqualTo(first.jobId)
        assertThat(reclaimed.pairId).isEqualTo(run.pairId)
        assertThat(reclaimed.token).isNotEqualTo(first.token)
    }

    @Test
    fun reclaimedJobStopsStaleWorkerBeforeOpenSearchWriteOrCompletion() {
        val run = createRun()
        load(sequenceOf(batch(1, false, document("one"))))
        val oldClaim = requireNotNull(claims.claimNext())
        val embeddingStarted = CountDownLatch(1)
        val releaseEmbedding = CountDownLatch(1)
        doAnswer {
            embeddingStarted.countDown()
            check(releaseEmbedding.await(10, TimeUnit.SECONDS))
            listOf(listOf(0.1))
        }.`when`(embedder).embed(anyList<String>())
        val executor = Executors.newSingleThreadExecutor()
        lateinit var reclaimed: IngestionClaim
        try {
            val staleWorker = executor.submit { processor.process(oldClaim) }
            assertThat(embeddingStarted.await(10, TimeUnit.SECONDS)).isTrue()
            jdbc.update(
                "UPDATE ingestion_jobs SET lease_expires_at = ? WHERE id = ?",
                Instant.now().minusSeconds(1),
                run.jobId,
            )
            jdbc.update(
                "UPDATE connector_credential_pairs SET ingestion_lease_expires_at = ? WHERE id = ?",
                Instant.now().minusSeconds(1),
                run.pairId,
            )
            reclaimed = requireNotNull(claims.claimNext())
            assertThat(reclaimed.token).isNotEqualTo(oldClaim.token)
            releaseEmbedding.countDown()
            staleWorker.get(10, TimeUnit.SECONDS)
        } finally {
            releaseEmbedding.countDown()
            executor.shutdownNow()
        }

        assertThat(mockingDetails(indexer).invocations.none { it.method.name == "upsert" }).isTrue()
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.RUNNING)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.IN_PROGRESS)

        processor.process(reclaimed)

        assertThat(documents.findByCcPairIdAndSourceDocumentId(run.pairId, "one")).isNotNull()
        assertThat(jobs.findById(run.jobId).orElseThrow().state).isEqualTo(JobState.SUCCEEDED)
        assertThat(attempts.findById(run.attemptId).orElseThrow().status).isEqualTo(AttemptStatus.SUCCESS)
    }

    @Test
    fun reclaimedAttemptPreservesPreviouslyCommittedCounts() {
        val run = createRun()
        saveDocument(run.pairId, "one")
        attempts.saveAndFlush(
            attempts.findById(run.attemptId).orElseThrow().apply {
                newDocsIndexed = 1
                totalDocsIndexed = 1
            },
        )
        jdbc.update(
            "INSERT INTO ingestion_enumerated_documents(attempt_id, source_document_id, processed) VALUES (?, ?, TRUE)",
            run.attemptId,
            "one",
        )
        load(sequenceOf(batch(1, false, document("one"), document("two"))))

        processor.process(run.jobId)

        val completed = attempts.findById(run.attemptId).orElseThrow()
        assertThat(completed.newDocsIndexed).isEqualTo(2)
        assertThat(completed.totalDocsIndexed).isEqualTo(2)
    }

    @Test
    fun deletionFenceStopsClaimedWorkerBeforeLateWrite() {
        val run = createRun(status = PairStatus.ACTIVE)
        val pair = pairs.findById(run.pairId).orElseThrow()
        load(sequenceOf(batch(1, false, document("one"))))
        val claim = requireNotNull(claims.claimNext())
        val embeddingStarted = CountDownLatch(1)
        val releaseEmbedding = CountDownLatch(1)
        doAnswer {
            embeddingStarted.countDown()
            check(releaseEmbedding.await(10, TimeUnit.SECONDS))
            listOf(listOf(0.1))
        }.`when`(embedder).embed(anyList<String>())
        val executor = Executors.newSingleThreadExecutor()
        try {
            val staleWorker = executor.submit { processor.process(claim) }
            assertThat(embeddingStarted.await(10, TimeUnit.SECONDS)).isTrue()

            admin.deletePair(DeletionAttemptRequest(pair.connectorId, pair.credentialId))
            releaseEmbedding.countDown()
            staleWorker.get(10, TimeUnit.SECONDS)
        } finally {
            releaseEmbedding.countDown()
            executor.shutdownNow()
        }

        assertThat(mockingDetails(indexer).invocations.none { it.method.name == "upsert" }).isTrue()
        assertThat(pairs.findById(run.pairId)).isEmpty()
        assertThat(jobs.findById(run.jobId)).isEmpty()
        assertThat(attempts.findById(run.attemptId)).isEmpty()
    }

    @Test
    fun deletionWaitsForAnAuthorizedInFlightUpsertBeforeItsFinalIndexDelete() {
        val run = createRun(status = PairStatus.ACTIVE)
        val pair = pairs.findById(run.pairId).orElseThrow()
        load(sequenceOf(batch(1, false, document("one"))))
        val claim = requireNotNull(claims.claimNext())
        val upsertStarted = CountDownLatch(1)
        val releaseUpsert = CountDownLatch(1)
        val deletionStarted = CountDownLatch(1)
        val indexDeleteStarted = CountDownLatch(1)
        val orphanedChunk = AtomicBoolean(false)
        val indexDeleteHadDatabaseTransaction = AtomicBoolean(false)
        doAnswer {
            upsertStarted.countDown()
            check(releaseUpsert.await(10, TimeUnit.SECONDS))
            orphanedChunk.set(true)
            Unit
        }.`when`(indexer).upsert(
            run.pairId,
            "one",
            0,
            "one",
            "one content",
            null,
            emptyMap(),
            listOf(0.1),
        )
        doAnswer {
            indexDeleteStarted.countDown()
            indexDeleteHadDatabaseTransaction.set(TransactionSynchronizationManager.isActualTransactionActive())
            orphanedChunk.set(false)
            Unit
        }.`when`(indexer).deletePair(run.pairId)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val worker = executor.submit { processor.process(claim) }
            assertThat(upsertStarted.await(10, TimeUnit.SECONDS)).isTrue()
            val deletion = executor.submit {
                deletionStarted.countDown()
                admin.deletePair(DeletionAttemptRequest(pair.connectorId, pair.credentialId))
            }
            assertThat(deletionStarted.await(10, TimeUnit.SECONDS)).isTrue()
            assertThat(indexDeleteStarted.await(2, TimeUnit.SECONDS)).isFalse()
            releaseUpsert.countDown()
            worker.get(10, TimeUnit.SECONDS)
            deletion.get(10, TimeUnit.SECONDS)
        } finally {
            releaseUpsert.countDown()
            executor.shutdownNow()
        }

        assertThat(orphanedChunk).isFalse()
        assertThat(indexDeleteHadDatabaseTransaction).isTrue()
        assertThat(pairs.findById(run.pairId)).isEmpty()
    }

    @Test
    fun leaseRenewsWhileEmbeddingRequestIsInFlight() {
        val run = createRun()
        load(sequenceOf(batch(1, false, document("one"))))
        val claim = requireNotNull(claims.claimJob(run.jobId))
        val embeddingStarted = CountDownLatch(1)
        val releaseEmbedding = CountDownLatch(1)
        val leaseAtEmbeddingStart = AtomicReference<Instant>()
        doAnswer { invocation ->
            leaseAtEmbeddingStart.set(jobs.findById(run.jobId).orElseThrow().leaseExpiresAt)
            embeddingStarted.countDown()
            check(releaseEmbedding.await(2, TimeUnit.SECONDS))
            invocation.getArgument<List<String>>(0).map { listOf(0.1) }
        }.`when`(embedder).embed(anyList<String>())

        val executor = Executors.newSingleThreadExecutor()
        try {
            val worker = executor.submit { processor.process(claim) }
            assertThat(embeddingStarted.await(2, TimeUnit.SECONDS)).isTrue()
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            var renewed = false
            while (!renewed && System.nanoTime() < deadline) {
                renewed = jobs.findById(run.jobId).orElseThrow().leaseExpiresAt
                    ?.isAfter(leaseAtEmbeddingStart.get()) == true
                if (!renewed) Thread.sleep(10)
            }
            assertThat(renewed).isTrue()
            releaseEmbedding.countDown()
            worker.get(2, TimeUnit.SECONDS)
        } finally {
            releaseEmbedding.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun manualDuplicateEnqueueReturnsExistingActiveJob() {
        val pairId = createPair(status = PairStatus.ACTIVE)

        val first = admin.enqueuePair(pairId, fromBeginning = false)
        val duplicate = admin.enqueuePair(pairId, fromBeginning = true)

        assertThat(duplicate).isEqualTo(first)
        assertThat(attempts.count()).isEqualTo(1)
        assertThat(jobs.count()).isEqualTo(1)
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
        verifyNoInteractions(permissionSync)
        assertThat(jobs.count()).isEqualTo(1)
    }

    @Test
    fun concurrentSchedulerCallsQueueOneJob() {
        val now = Instant.parse("2026-09-01T00:00:00Z")
        createPair(refreshFreq = 1)
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
        }
    }

    private fun createRun(
        refreshFreq: Long? = null,
        fromBeginning: Boolean = false,
        inRepeatedErrorState: Boolean = false,
        source: ConnectorSource = ConnectorSource.FILE,
        status: PairStatus = PairStatus.SCHEDULED,
        pollRangeStart: Instant? = null,
        pollRangeEnd: Instant? = null,
    ): Run {
        val pairId = createPair(
            refreshFreq = refreshFreq,
            inRepeatedErrorState = inRepeatedErrorState,
            source = source,
            status = status,
        )
        val attempt = attempts.save(
            IngestionAttemptEntity(
                ccPairId = pairId,
                fromBeginning = fromBeginning,
                pollRangeStart = pollRangeStart,
                pollRangeEnd = pollRangeEnd,
            ),
        )
        val job = jobs.save(IngestionJobEntity(attemptId = requireNotNull(attempt.id), ccPairId = pairId))
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
