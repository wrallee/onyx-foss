package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.IngestionAttemptEntity
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionCheckpointEntity
import com.onyx.foss.kotlin.domain.IngestionCheckpointRepository
import com.onyx.foss.kotlin.domain.IngestionErrorEntity
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.IngestionEnumerationRepository
import com.onyx.foss.kotlin.domain.IngestionJobEntity
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.service.AdminService
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

@Component
class IngestionWorker(
    private val properties: OnyxProperties,
    private val claims: JobClaimService,
    private val processor: IngestionProcessor,
) {
    @Scheduled(fixedDelay = 1000)
    fun work() {
        if (!properties.worker.enabled) return
        claims.claimNext()?.let(processor::process)
    }
}

@Service
class JobClaimService(
    private val jobs: IngestionJobRepository,
    private val pairs: ConnectorCredentialPairRepository,
    private val attempts: IngestionAttemptRepository,
    private val errors: IngestionErrorRepository,
) {
    @Transactional
    fun claimNext(now: Instant = Instant.now()): IngestionClaim? =
        jobs.lockNextClaimable(now)?.let { claim(it, now) }

    @Transactional
    fun claimJob(jobId: Long, now: Instant = Instant.now()): IngestionClaim? =
        jobs.lockClaimableById(jobId, now)?.let { claim(it, now) }

    private fun claim(job: IngestionJobEntity, now: Instant): IngestionClaim? {
        val pair = pairs.lockById(job.ccPairId) ?: return null
        if (pair.status == PairStatus.DELETING || pair.ingestionLeaseExpiresAt?.isAfter(now) == true) return null
        val token = UUID.randomUUID()
        val leaseExpiresAt = now.plus(INGESTION_LEASE)
        job.state = JobState.RUNNING
        job.lockedAt = now
        job.lockedBy = "spring-worker"
        job.attempts += 1
        job.claimToken = token
        job.leaseExpiresAt = leaseExpiresAt
        pair.ingestionClaimToken = token
        pair.ingestionLeaseExpiresAt = leaseExpiresAt
        jobs.saveAndFlush(job)
        pairs.saveAndFlush(pair)
        return IngestionClaim(requireNotNull(job.id), job.ccPairId, job.attemptId, token)
    }

    @Transactional
    fun start(claim: IngestionClaim, now: Instant = Instant.now()): Boolean {
        val ownership = ownership(claim) ?: return false
        val attempt = attempts.findById(claim.attemptId).orElse(null) ?: return false
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = attempt.timeStarted ?: now
        attempts.save(attempt)
        if (ownership.pair.status == PairStatus.SCHEDULED) {
            ownership.pair.status = PairStatus.INITIAL_INDEXING
            pairs.save(ownership.pair)
        }
        return true
    }

    @Transactional
    fun renew(claim: IngestionClaim, now: Instant = Instant.now()): Boolean {
        val ownership = ownership(claim) ?: return false
        val leaseExpiresAt = now.plus(INGESTION_LEASE)
        ownership.job.leaseExpiresAt = leaseExpiresAt
        ownership.pair.ingestionLeaseExpiresAt = leaseExpiresAt
        jobs.save(ownership.job)
        pairs.save(ownership.pair)
        return true
    }

    @Transactional
    fun complete(
        claim: IngestionClaim,
        status: AttemptStatus,
        newDocuments: Int,
        totalDocuments: Int,
        removedDocuments: Int,
        updateLastPrunedAt: Boolean,
    ): Boolean {
        val ownership = ownership(claim) ?: return false
        val attempt = attempts.findById(claim.attemptId).orElse(null) ?: return false
        attempt.status = status
        attempt.newDocsIndexed = newDocuments
        attempt.totalDocsIndexed = totalDocuments
        attempt.docsRemovedFromIndex = removedDocuments
        attempts.save(attempt)
        ownership.pair.inRepeatedErrorState = false
        ownership.pair.status = PairStatus.ACTIVE
        if (updateLastPrunedAt) ownership.pair.lastPrunedAt = Instant.now()
        releasePair(ownership.pair)
        pairs.save(ownership.pair)
        ownership.job.state = JobState.SUCCEEDED
        releaseJob(ownership.job)
        jobs.save(ownership.job)
        return true
    }

    @Transactional
    fun cancel(claim: IngestionClaim): Boolean {
        val ownership = ownership(claim) ?: return false
        val attempt = attempts.findById(claim.attemptId).orElse(null) ?: return false
        attempt.status = AttemptStatus.CANCELED
        attempts.save(attempt)
        releasePair(ownership.pair)
        pairs.save(ownership.pair)
        ownership.job.state = JobState.SUCCEEDED
        releaseJob(ownership.job)
        jobs.save(ownership.job)
        return true
    }

    @Transactional
    fun fail(claim: IngestionClaim, error: Exception, refreshFreq: Long?): Boolean {
        val ownership = ownership(claim) ?: return false
        val attempt = attempts.findById(claim.attemptId).orElse(null) ?: return false
        attempt.status = AttemptStatus.FAILED
        attempt.errorMessage = error.message?.take(1000) ?: "Ingestion failed"
        attempt.fullExceptionTrace = error.stackTraceToString().take(16000)
        attempts.saveAndFlush(attempt)
        errors.save(
            IngestionErrorEntity(
                attemptId = claim.attemptId,
                failureMessage = attempt.errorMessage ?: "Ingestion failed",
                errorType = error::class.simpleName,
            ),
        )
        ownership.pair.inRepeatedErrorState = isRepeatedError(
            refreshFreq,
            attempts.findAllByCcPairIdOrderByIdDesc(claim.pairId),
        )
        releasePair(ownership.pair)
        pairs.save(ownership.pair)
        ownership.job.state = JobState.FAILED
        ownership.job.lastError = attempt.errorMessage
        releaseJob(ownership.job)
        jobs.save(ownership.job)
        return true
    }

    private fun ownership(claim: IngestionClaim): IngestionOwnership? {
        val job = jobs.lockById(claim.jobId) ?: return null
        if (job.state != JobState.RUNNING || job.claimToken != claim.token || job.ccPairId != claim.pairId) return null
        val pair = pairs.lockById(claim.pairId) ?: return null
        if (pair.status == PairStatus.DELETING || pair.ingestionClaimToken != claim.token) return null
        return IngestionOwnership(job, pair)
    }

    private fun releaseJob(job: IngestionJobEntity) {
        job.claimToken = null
        job.leaseExpiresAt = null
    }

    private fun releasePair(pair: com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity) {
        pair.ingestionClaimToken = null
        pair.ingestionLeaseExpiresAt = null
    }
}

data class IngestionClaim(
    val jobId: Long,
    val pairId: Long,
    val attemptId: Long,
    val token: UUID,
)

private data class IngestionOwnership(
    val job: IngestionJobEntity,
    val pair: com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity,
)

@Service
class IngestionProcessor(
    private val admin: AdminService,
    private val pairs: ConnectorCredentialPairRepository,
    private val attempts: IngestionAttemptRepository,
    private val checkpoints: IngestionCheckpointRepository,
    private val errors: IngestionErrorRepository,
    private val enumeration: IngestionEnumerationRepository,
    private val documents: IndexedDocumentRepository,
    private val documentSets: DocumentSetRepository,
    private val fileLoader: FileConnectorLoader,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val embedder: ModelServerClient,
    private val indexer: OpenSearchIndexer,
    private val pruning: PruningService,
    private val permissionSync: PermissionSyncWorker,
    private val mapper: ObjectMapper,
    private val claims: JobClaimService,
    private val externalWrites: PairExternalWriteFence,
) {
    fun process(jobId: Long) {
        claims.claimJob(jobId)?.let(::process)
    }

    fun process(claim: IngestionClaim) {
        if (!claims.start(claim)) return
        val attempt = attempts.findById(claim.attemptId).orElse(null) ?: return
        val pair = pairs.findById(claim.pairId).orElse(null) ?: return
        var refreshFreq: Long? = null
        try {
            val connector = admin.connector(pair.connectorId)
            refreshFreq = connector.refreshFreq
            if (!attempt.pruneOnly) setPollRange(attempt, connector.indexingStart)
            attempts.save(attempt)
            val checkpoint = if (attempt.fromBeginning || attempt.pruneOnly) {
                null
            } else {
                checkpoints.findById(requireNotNull(pair.id)).orElse(null)?.checkpointJson
            }
            val credentials = admin.credentialSecret(pair.credentialId)
            val batches = if (attempt.pruneOnly) {
                when (connector.source) {
                    ConnectorSource.FILE -> fileLoader.load(connector.connectorSpecificConfig)
                    else -> remoteLoaders.loadSlim(
                        connector.source,
                        connector.connectorSpecificConfig,
                        credentials,
                    )
                }
            } else {
                when (connector.source) {
                    ConnectorSource.FILE -> fileLoader.load(connector.connectorSpecificConfig)
                    else -> remoteLoaders.load(
                        connector.source,
                        connector.connectorSpecificConfig,
                        credentials,
                        checkpoint,
                        attempt.pollRangeStart,
                        attempt.pollRangeEnd,
                    )
                }
            }
            var newDocuments = attempt.newDocsIndexed
            var totalDocuments = attempt.totalDocsIndexed
            var hasFailures = false
            var completeEnumeration = false
            var enumerationSafe = true
            val attemptId = requireNotNull(attempt.id)
            val batchIterator = batches.iterator()
            while (true) {
                stopIfStopped(claim)
                if (!batchIterator.hasNext()) break
                val batch = batchIterator.next()
                stopIfStopped(claim)
                val batchFailedDocumentIds = batch.failures.mapNotNull { failure ->
                    (failure.target as? FailureTarget.Document)?.id
                }.toSet()
                enumeration.protectFailures(attemptId, batchFailedDocumentIds)
                if (!batch.enumerationComplete || batch.failures.any { it.target !is FailureTarget.Document }) {
                    enumerationSafe = false
                }
                val newDocumentIds = enumeration.registerDocuments(attemptId, batch.documents.map(SourceDocument::id))
                val processedInBatch = mutableSetOf<String>()
                batch.documents.forEach { document ->
                    if (document.id !in newDocumentIds || !processedInBatch.add(document.id)) return@forEach
                    if (attempt.pruneOnly) return@forEach
                    if (document.title.isBlank() && document.content.isBlank()) {
                        enumerationSafe = false
                        return@forEach
                    }
                    val indexableContent = document.content.ifBlank { document.title }
                    val chunks = indexableContent.chunked(1500).filter { it.isNotBlank() }
                    if (chunks.isEmpty()) {
                        enumerationSafe = false
                        return@forEach
                    }
                    renew(claim)
                    val vectors = embedder.embed(chunks)
                    check(vectors.size == chunks.size) {
                        "Model server returned ${vectors.size} embeddings for ${chunks.size} chunks"
                    }
                    renew(claim)
                    val documentSetNames = documentSets.findNamesByCcPairId(requireNotNull(pair.id))
                    chunks.zip(vectors).forEachIndexed { index, item ->
                        externalWrites.withPair(requireNotNull(pair.id)) {
                            renew(claim)
                            indexer.upsert(
                                pairId = requireNotNull(pair.id),
                                sourceDocumentId = document.id,
                                chunkId = index,
                                title = document.title,
                                content = item.first,
                                link = document.link,
                                metadata = document.metadata,
                                embedding = item.second,
                                documentSets = documentSetNames,
                                updatedAt = document.updatedAt,
                                primaryOwners = document.primaryOwners,
                                secondaryOwners = document.secondaryOwners,
                            )
                        }
                        renew(claim)
                    }
                    renew(claim)
                    indexer.deleteStaleChunks(requireNotNull(pair.id), document.id, chunks.size)
                    renew(claim)
                    val existing = documents.findByCcPairIdAndSourceDocumentId(requireNotNull(pair.id), document.id)
                    if (existing == null) newDocuments += 1
                    val indexedDocument = existing ?: IndexedDocumentEntity(
                        ccPairId = requireNotNull(pair.id),
                        sourceDocumentId = document.id,
                    )
                    indexedDocument.apply {
                        title = document.title
                        link = document.link
                        contentHash = hash(document.content)
                        metadata = mapper.valueToTree(document.metadata)
                        externalAccess = document.externalAccess?.let(mapper::valueToTree)
                        lastModified = document.updatedAt
                        primaryOwners = document.primaryOwners
                        secondaryOwners = document.secondaryOwners
                        lastSynced = Instant.now()
                    }
                    documents.save(indexedDocument)
                    totalDocuments += 1
                    attempt.newDocsIndexed = newDocuments
                    attempt.totalDocsIndexed = totalDocuments
                    attempts.save(attempt)
                    if (document.id !in batchFailedDocumentIds) {
                        val resolvedErrors = errors
                            .findUnresolvedByCcPairIdAndSourceDocumentId(requireNotNull(pair.id), document.id)
                            .onEach { it.isResolved = true }
                        errors.saveAll(resolvedErrors)
                    }
                    enumeration.markProcessed(attemptId, document.id)
                }
                if (batch.failures.isNotEmpty()) {
                    hasFailures = true
                    errors.saveAll(batch.failures.map { failure -> failure.toEntity(requireNotNull(attempt.id)) })
                }
                if (!attempt.pruneOnly) {
                    checkpoints.save(
                        IngestionCheckpointEntity(
                            ccPairId = requireNotNull(pair.id),
                            checkpointJson = batch.checkpoint.value,
                        ),
                    )
                }
                completeEnumeration = enumerationSafe && !batch.checkpoint.hasMore
                attempt.enumerationComplete = completeEnumeration
                attempts.save(attempt)
            }
            stopIfStopped(claim)
            attempt.docsRemovedFromIndex = pruning.prune(
                requireNotNull(pair.id),
                attemptId,
                attempt.fromBeginning || attempt.pruneOnly,
                completeEnumeration,
                beforeDelete = { renew(claim) },
            )
            if (!hasFailures) {
                val resolvedEntityErrors = errors.findUnresolvedEntityErrorsByCcPairId(requireNotNull(pair.id))
                    .onEach { it.isResolved = true }
                errors.saveAll(resolvedEntityErrors)
            }
            renew(claim)
            if (!attempt.pruneOnly) permissionSync.enqueue(requireNotNull(pair.id))
            claims.complete(
                claim = claim,
                status = if (hasFailures) AttemptStatus.COMPLETED_WITH_ERRORS else AttemptStatus.SUCCESS,
                newDocuments = newDocuments,
                totalDocuments = totalDocuments,
                removedDocuments = attempt.docsRemovedFromIndex,
                updateLastPrunedAt = (attempt.fromBeginning || attempt.pruneOnly) && completeEnumeration,
            )
        } catch (_: ConnectorPausedException) {
            claims.cancel(claim)
        } catch (_: StaleIngestionClaimException) {
            return
        } catch (error: Exception) {
            claims.fail(claim, error, refreshFreq)
        }
    }

    private fun stopIfStopped(claim: IngestionClaim) {
        if (pairs.findById(claim.pairId).orElse(null)?.status == PairStatus.PAUSED) throw ConnectorPausedException()
        renew(claim)
    }

    private fun renew(claim: IngestionClaim) {
        if (!claims.renew(claim)) throw StaleIngestionClaimException()
    }

    private fun hash(value: String): String =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray(StandardCharsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    private fun setPollRange(attempt: IngestionAttemptEntity, indexingStart: Instant?) {
        if (attempt.pollRangeStart != null && attempt.pollRangeEnd != null) return
        val priorAttempts = attempts.findAllByCcPairIdOrderByIdDesc(attempt.ccPairId)
            .filterNot { it.id == attempt.id }
        val resumable = priorAttempts.firstOrNull()?.takeIf { it.status == AttemptStatus.FAILED }
        if (resumable?.pollRangeStart != null && resumable.pollRangeEnd != null) {
            attempt.pollRangeStart = resumable.pollRangeStart
            attempt.pollRangeEnd = resumable.pollRangeEnd
            return
        }
        val previousEnd = priorAttempts.firstOrNull {
            it.status in setOf(AttemptStatus.SUCCESS, AttemptStatus.COMPLETED_WITH_ERRORS) && it.pollRangeEnd != null
        }?.pollRangeEnd
        val earliest = indexingStart ?: Instant.EPOCH
        attempt.pollRangeStart = if (attempt.fromBeginning || previousEnd == null) {
            earliest
        } else {
            previousEnd.minusSeconds(30 * 60).coerceAtLeast(Instant.EPOCH)
        }
        attempt.pollRangeEnd = Instant.now()
    }
}

private class ConnectorPausedException : RuntimeException()
private class StaleIngestionClaimException : RuntimeException()

internal fun isRepeatedError(refreshFreq: Long?, recent: List<IngestionAttemptEntity>): Boolean {
    val required = if (refreshFreq == null) 1 else 5
    return recent.take(required).size == required && recent.take(required).all { it.status == AttemptStatus.FAILED }
}

private fun ConnectorFailure.toEntity(attemptId: Long): IngestionErrorEntity = when (val failureTarget = target) {
    is FailureTarget.Document -> IngestionErrorEntity(
        attemptId = attemptId,
        sourceDocumentId = failureTarget.id,
        documentLink = failureTarget.link,
        failureMessage = message,
        errorType = errorType,
    )
    is FailureTarget.Entity -> IngestionErrorEntity(
        attemptId = attemptId,
        entityId = failureTarget.id,
        failedTimeRangeStart = failureTarget.missedStart,
        failedTimeRangeEnd = failureTarget.missedEnd,
        failureMessage = message,
        errorType = errorType,
    )
}

@Service
class ModelServerClient(
    private val properties: OnyxProperties,
    private val clientBuilder: WebClient.Builder,
) {
    private companion object {
        const val MAX_RESPONSE_BYTES = 16 * 1024 * 1024
    }

    fun embed(texts: List<String>): List<List<Double>> {
        require(properties.modelServer.modelName.isNotBlank()) {
            "ONYX_EMBEDDING_MODEL_NAME must be configured before file ingestion"
        }
        val response = clientBuilder.clone().codecs { codecs ->
            codecs.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BYTES)
        }.build()
            .post()
            .uri(properties.modelServer.baseUrl.trimEnd('/') + "/encoder/bi-encoder-embed")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(
                mapOf(
                    "texts" to texts,
                    "model_name" to properties.modelServer.modelName,
                    "max_context_length" to properties.modelServer.maxContextLength,
                    "normalize_embeddings" to properties.modelServer.normalizeEmbeddings,
                    "text_type" to "passage",
                ),
            )
            .retrieve()
            .bodyToMono(JsonNode::class.java)
            .block(MODEL_SERVER_TIMEOUT) ?: error("Model server returned no embedding response")
        return response.path("embeddings").map { vector -> vector.map { it.asDouble() } }
    }
}

@Service
class OpenSearchIndexer(
    private val properties: OnyxProperties,
    clientBuilder: WebClient.Builder,
    private val mapper: ObjectMapper,
    private val externalWrites: PairExternalWriteFence,
) {
    private val client = if (properties.opensearch.verifyCerts) {
        clientBuilder.build()
    } else {
        val sslContext = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build()
        clientBuilder.clone()
            .clientConnector(ReactorClientHttpConnector(HttpClient.create().secure { it.sslContext(sslContext) }))
            .build()
    }
    private val indexReady = AtomicBoolean(false)

    fun deletePair(pairId: Long) {
        deleteByQuery(mapOf("term" to mapOf("cc_pair_id" to pairId)), "pair deletion")
    }

    fun deleteDocuments(pairId: Long, sourceDocumentIds: Set<String>) {
        deleteByQuery(
            mapOf(
                "bool" to mapOf(
                    "filter" to listOf(
                        mapOf("term" to mapOf("cc_pair_id" to pairId)),
                        mapOf("terms" to mapOf("source_document_id" to sourceDocumentIds)),
                    ),
                ),
            ),
            "document deletion",
        )
    }

    fun deleteStaleChunks(pairId: Long, sourceDocumentId: String, newChunkCount: Int) {
        deleteByQuery(
            mapOf(
                "bool" to mapOf(
                    "filter" to listOf(
                        mapOf("term" to mapOf("cc_pair_id" to pairId)),
                        mapOf("term" to mapOf(EXACT_DOCUMENT_ID_FIELD to sourceDocumentId)),
                        mapOf("range" to mapOf("chunk_id" to mapOf("gte" to newChunkCount))),
                    ),
                ),
            ),
            "stale chunk deletion",
        )
    }

    fun updateAccess(pairId: Long, accessByDocument: Map<String, ExternalAccess>) {
        if (accessByDocument.isEmpty()) return
        val access = accessByDocument.mapValues { (_, value) ->
            mapOf(
                "external_user_emails" to value.externalUserEmails,
                "external_user_group_ids" to value.externalUserGroupIds,
                "is_public" to value.isPublic,
            )
        }
        val body = mapOf(
            "script" to mapOf(
                "source" to """
                    def access = params.access_by_document[ctx._source.source_document_id];
                    ctx._source.external_user_emails = access.external_user_emails;
                    ctx._source.external_user_group_ids = access.external_user_group_ids;
                    ctx._source.is_public = access.is_public;
                """.trimIndent(),
                "params" to mapOf("access_by_document" to access),
            ),
            "query" to mapOf(
                "bool" to mapOf(
                    "filter" to listOf(
                        mapOf("term" to mapOf("cc_pair_id" to pairId)),
                        mapOf("terms" to mapOf("source_document_id" to accessByDocument.keys)),
                    ),
                ),
            ),
        )
        updateByQuery(body, "ACL update", accessByDocument.size)
    }

    fun updateDocumentSets(pairId: Long, sourceDocumentIds: Set<String>, documentSetNames: List<String>) {
        if (sourceDocumentIds.isEmpty()) return
        val body = mapOf(
            "script" to mapOf(
                "source" to "ctx._source.document_sets = params.document_sets",
                "params" to mapOf("document_sets" to documentSetNames),
            ),
            "query" to mapOf(
                "bool" to mapOf(
                    "filter" to listOf(
                        mapOf("term" to mapOf("cc_pair_id" to pairId)),
                        mapOf("terms" to mapOf("source_document_id" to sourceDocumentIds)),
                    ),
                ),
            ),
        )
        updateByQuery(body, "document set update", sourceDocumentIds.size)
    }

    private fun updateByQuery(body: Map<String, Any>, operation: String, minimumTotal: Int) {
        val response = withMigrationRetry {
            client
                .post()
                .uri(
                    properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index +
                        "/_update_by_query?refresh=true&conflicts=proceed",
                )
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(mapper.valueToTree<JsonNode>(body))
                .exchangeToMono { result ->
                    if (result.statusCode().is2xxSuccessful) {
                        result.bodyToMono(JsonNode::class.java).defaultIfEmpty(mapper.createObjectNode())
                    } else {
                        result.bodyToMono(String::class.java).flatMap {
                            Mono.error(openSearchWriteError(operation, result.statusCode().value(), it))
                        }
                    }
                }.block(DOCUMENT_SET_UPDATE_TIMEOUT)
        }
        val total = response?.path("total")?.asInt(-1) ?: -1
        val updated = response?.path("updated")?.asInt(-1) ?: -1
        val noops = response?.path("noops")?.asInt(-1) ?: -1
        check(
            response != null && !response.path("timed_out").asBoolean(true) &&
                response.path("failures").isArray && response.path("failures").isEmpty &&
                response.path("version_conflicts").asInt(-1) == 0 &&
                total >= minimumTotal && updated + noops == total,
        ) { "OpenSearch did not fully apply the $operation" }
    }

    private fun deleteByQuery(query: Map<String, Any>, operation: String) {
        val response = withMigrationRetry {
            client
                .post()
                .uri(
                    properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index +
                        "/_delete_by_query?refresh=true",
                )
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(mapOf("query" to query))
                .exchangeToMono { result ->
                    if (result.statusCode().is2xxSuccessful) {
                        result.bodyToMono(JsonNode::class.java).defaultIfEmpty(mapper.createObjectNode())
                    } else result.bodyToMono(String::class.java).flatMap {
                        Mono.error(openSearchWriteError(operation, result.statusCode().value(), it))
                    }
                }.block(OPENSEARCH_TIMEOUT)
        }
        val total = response?.path("total")?.asInt(-1) ?: -1
        val deleted = response?.path("deleted")?.asInt(-1) ?: -1
        check(
            response != null && !response.path("timed_out").asBoolean(true) &&
                response.path("failures").isArray && response.path("failures").isEmpty &&
                response.path("version_conflicts").asInt(-1) == 0 && total >= 0 && deleted == total,
        ) { "OpenSearch did not fully apply the $operation" }
    }

    fun upsert(
        pairId: Long,
        sourceDocumentId: String,
        chunkId: Int,
        title: String,
        content: String,
        link: String?,
        metadata: Map<String, Any?>,
        embedding: List<Double>,
        documentSets: List<String> = emptyList(),
        updatedAt: Instant? = null,
        primaryOwners: List<String> = emptyList(),
        secondaryOwners: List<String> = emptyList(),
    ) {
        val documentId = Base64.getUrlEncoder().withoutPadding().encodeToString(
            (pairId.toString() + ":" + sourceDocumentId + ":" + chunkId).toByteArray(StandardCharsets.UTF_8),
        )
        val body = mapOf(
            "cc_pair_id" to pairId,
            "source_document_id" to sourceDocumentId,
            "chunk_id" to chunkId,
            "title" to title,
            "content" to content,
            "link" to link,
            "metadata" to metadata,
            "embedding" to embedding,
            "document_sets" to documentSets,
            "doc_updated_at" to updatedAt,
            "primary_owners" to primaryOwners,
            "secondary_owners" to secondaryOwners,
            "external_user_emails" to emptyList<String>(),
            "external_user_group_ids" to emptyList<String>(),
            "is_public" to false,
        )
        val response = withMigrationRetry {
            client
                .put()
                .uri(properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index + "/_doc/" + documentId + "?refresh=true")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(mapper.valueToTree<JsonNode>(body))
                .exchangeToMono { result ->
                    if (result.statusCode().is2xxSuccessful) result.releaseBody().thenReturn(true)
                    else result.bodyToMono(String::class.java).flatMap {
                        Mono.error(openSearchWriteError("index write", result.statusCode().value(), it))
                    }
                }.block(OPENSEARCH_TIMEOUT)
        }
        check(response == true) { "OpenSearch did not confirm the index write" }
    }

    private fun <T> withMigrationRetry(action: () -> T): T {
        var lastBlock: OpenSearchWriteBlockedException? = null
        repeat(2) {
            ensureIndex()
            try {
                return action()
            } catch (error: OpenSearchWriteBlockedException) {
                indexReady.set(false)
                lastBlock = error
            }
        }
        throw requireNotNull(lastBlock)
    }

    private fun openSearchWriteError(operation: String, status: Int, body: String): RuntimeException {
        val message = "OpenSearch $operation failed: $body"
        return if (status == 403 && body.contains("cluster_block_exception")) {
            OpenSearchWriteBlockedException(message)
        } else {
            IllegalStateException(message)
        }
    }

    private fun ensureIndex() {
        if (indexReady.get()) return
        synchronized(indexReady) {
            if (indexReady.get()) return
            externalWrites.withOpenSearchIndex(properties.opensearch.index) {
                val indexUrl = properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index
                val exists = indexExists(indexUrl, "index check")
                if (!exists) {
                    putJson(indexUrl, INDEX_DEFINITION, "index creation")
                } else {
                    val mapping = mapping(indexUrl, "mapping check")
                    val missingFields = linkedMapOf<String, Any>()
                    var incompatibleMapping = false
                    EXACT_FIELDS.forEach { (field, definition) ->
                        val expectedType = (definition as Map<*, *>)["type"].toString()
                        val actualType = mapping.properties.path(field).path("type").asText()
                        if (actualType.isBlank()) {
                            missingFields[field] = definition
                        } else if (actualType != expectedType) {
                            incompatibleMapping = true
                        }
                    }
                    if (incompatibleMapping) {
                        reindexWithExactMappings(mapping.concreteIndex)
                    } else if (missingFields.isNotEmpty()) {
                        putJson("$indexUrl/_mapping", mapOf("properties" to missingFields), "exact mapping creation")
                    }
                }
            }
            indexReady.set(true)
        }
    }

    private fun reindexWithExactMappings(sourceIndex: String) {
        val logicalIndex = properties.opensearch.index
        val replacementIndex = "$logicalIndex-exact-v1"
        check(sourceIndex != replacementIndex) { "OpenSearch exact mapping replacement cannot replace itself" }
        val baseUrl = properties.opensearch.baseUrl.trimEnd('/')
        val indexUrl = "$baseUrl/$logicalIndex"
        val replacementUrl = "$baseUrl/$replacementIndex"
        try {
            addWriteBlock(sourceIndex)
        } catch (error: Exception) {
            if (exactLogicalMapping() != null) return
            if (!isWriteBlocked(sourceIndex)) throw error
        }
        try {
            ensureReplacementIndex(replacementUrl)
            val sourceCount = getJson("$baseUrl/$sourceIndex/_count", "source count").path("count").asLong(-1)
            val result = postJson(
                "$baseUrl/_reindex?refresh=true&wait_for_completion=true",
                mapOf(
                    "source" to mapOf("index" to sourceIndex),
                    "dest" to mapOf("index" to replacementIndex),
                ),
                "exact mapping reindex",
            )
            check(
                sourceCount >= 0 && !result.path("timed_out").asBoolean(true) &&
                    result.path("failures").isArray && result.path("failures").isEmpty &&
                    result.path("version_conflicts").asLong(-1) == 0L &&
                    result.path("total").asLong(-1) == sourceCount &&
                    result.path("created").asLong(-1) + result.path("updated").asLong(-1) == sourceCount &&
                    getJson("$replacementUrl/_count", "replacement count").path("count").asLong(-1) == sourceCount,
            ) { "OpenSearch did not fully reindex the existing index" }

            val current = mapping(indexUrl, "pre-swap mapping check")
            if (hasExactMappings(current.properties)) return
            check(current.concreteIndex == sourceIndex) {
                "OpenSearch logical index changed to an incompatible index during migration"
            }
            val aliasResult = postJson(
                "$baseUrl/_aliases",
                mapOf(
                    "actions" to listOf(
                        mapOf("remove_index" to mapOf("index" to sourceIndex)),
                        mapOf("add" to mapOf("index" to replacementIndex, "alias" to logicalIndex, "is_write_index" to true)),
                    ),
                ),
                "exact mapping alias swap",
            )
            check(aliasResult.path("acknowledged").asBoolean(false)) {
                "OpenSearch did not confirm the exact mapping alias swap"
            }
        } catch (error: Exception) {
            if (exactLogicalMapping() != null) return
            throw error
        }
    }

    private fun addWriteBlock(sourceIndex: String) {
        val result = putWithoutBodyJson(
            properties.opensearch.baseUrl.trimEnd('/') + "/$sourceIndex/_block/write",
            "migration write block",
        )
        check(
            result.path("acknowledged").asBoolean(false) &&
                result.path("shards_acknowledged").asBoolean(false),
        ) { "OpenSearch did not confirm the migration write block" }
    }

    private fun isWriteBlocked(sourceIndex: String): Boolean = runCatching {
        val settings = getJson(
            properties.opensearch.baseUrl.trimEnd('/') + "/$sourceIndex/_settings/index.blocks.write?flat_settings=true",
            "migration write block check",
        ).path(sourceIndex).path("settings")
        settings.path("index.blocks.write").asText().equals("true", ignoreCase = true) ||
            settings.path("index").path("blocks").path("write").asBoolean(false)
    }.getOrDefault(false)

    private fun ensureReplacementIndex(replacementUrl: String) {
        if (!indexExists(replacementUrl, "replacement index check")) {
            try {
                putJson(replacementUrl, INDEX_DEFINITION, "replacement index creation")
            } catch (error: Exception) {
                if (!indexExists(replacementUrl, "replacement index race check")) throw error
            }
        }
        check(hasExactMappings(mapping(replacementUrl, "replacement mapping check").properties)) {
            "OpenSearch replacement index does not have exact mappings"
        }
    }

    private fun exactLogicalMapping(): IndexMapping? = runCatching {
        mapping(
            properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index,
            "migration recovery mapping check",
        )
    }.getOrNull()?.takeIf { hasExactMappings(it.properties) }

    private fun hasExactMappings(propertiesNode: JsonNode): Boolean = EXACT_FIELDS.all { (field, definition) ->
        propertiesNode.path(field).path("type").asText() == (definition as Map<*, *>)["type"].toString()
    }

    private fun mapping(indexUrl: String, operation: String): IndexMapping {
        val response = getJson("$indexUrl/_mapping", operation)
        check(response.size() == 1) { "OpenSearch index must resolve to one concrete index" }
        val entry = response.fields().next()
        return IndexMapping(entry.key, entry.value.path("mappings").path("properties"))
    }

    private fun indexExists(uri: String, operation: String): Boolean =
        client.head().uri(uri).exchangeToMono { response ->
            when {
                response.statusCode().is2xxSuccessful -> response.releaseBody().thenReturn(true)
                response.statusCode().value() == 404 -> response.releaseBody().thenReturn(false)
                else -> response.bodyToMono(String::class.java).flatMap {
                    Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
                }
            }
        }.block(OPENSEARCH_TIMEOUT) == true

    private fun getJson(uri: String, operation: String): JsonNode = requireNotNull(
        client.get().uri(uri).exchangeToMono { response ->
            if (response.statusCode().is2xxSuccessful) response.bodyToMono(JsonNode::class.java)
            else response.bodyToMono(String::class.java).flatMap {
                Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
            }
        }.block(OPENSEARCH_TIMEOUT),
    )

    private fun putJson(uri: String, body: Map<String, Any>, operation: String) {
        val confirmed = client.put().uri(uri)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(mapper.valueToTree<JsonNode>(body))
            .exchangeToMono { response ->
                if (response.statusCode().is2xxSuccessful) response.releaseBody().thenReturn(true)
                else response.bodyToMono(String::class.java).flatMap {
                    Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
                }
            }.block(OPENSEARCH_TIMEOUT)
        check(confirmed == true) { "OpenSearch did not confirm $operation" }
    }

    private fun putWithoutBodyJson(uri: String, operation: String): JsonNode = requireNotNull(
        client.put().uri(uri).exchangeToMono { response ->
            if (response.statusCode().is2xxSuccessful) response.bodyToMono(JsonNode::class.java)
            else response.bodyToMono(String::class.java).flatMap {
                Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
            }
        }.block(OPENSEARCH_TIMEOUT),
    )

    private fun postJson(uri: String, body: Map<String, Any>, operation: String): JsonNode = requireNotNull(
        client.post().uri(uri)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(mapper.valueToTree<JsonNode>(body))
            .exchangeToMono { response ->
                if (response.statusCode().is2xxSuccessful) response.bodyToMono(JsonNode::class.java)
                else response.bodyToMono(String::class.java).flatMap {
                    Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
                }
        }.block(OPENSEARCH_MIGRATION_TIMEOUT),
    )

    private data class IndexMapping(
        val concreteIndex: String,
        val properties: JsonNode,
    )

    private companion object {
        const val EXACT_DOCUMENT_ID_FIELD = "source_document_id"
        val EXACT_FIELDS: Map<String, Any> = mapOf(
            "cc_pair_id" to mapOf("type" to "long"),
            EXACT_DOCUMENT_ID_FIELD to mapOf("type" to "keyword"),
            "chunk_id" to mapOf("type" to "integer"),
            "external_user_emails" to mapOf("type" to "keyword"),
            "external_user_group_ids" to mapOf("type" to "keyword"),
            "is_public" to mapOf("type" to "boolean"),
            "document_sets" to mapOf("type" to "keyword"),
            "doc_updated_at" to mapOf("type" to "date"),
            "primary_owners" to mapOf("type" to "keyword"),
            "secondary_owners" to mapOf("type" to "keyword"),
        )
        val INDEX_DEFINITION: Map<String, Any> = mapOf("mappings" to mapOf("properties" to EXACT_FIELDS))
    }
}

private class OpenSearchWriteBlockedException(message: String) : RuntimeException(message)

internal val DOCUMENT_SET_UPDATE_TIMEOUT: Duration = Duration.ofSeconds(30)
internal val OPENSEARCH_TIMEOUT: Duration = Duration.ofSeconds(30)
internal val OPENSEARCH_MIGRATION_TIMEOUT: Duration = Duration.ofMinutes(10)
internal val MODEL_SERVER_TIMEOUT: Duration = Duration.ofSeconds(30)
internal val INGESTION_LEASE: Duration = Duration.ofMinutes(1)
