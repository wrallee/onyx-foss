package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.IngestionAttemptEntity
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionCheckpointEntity
import com.onyx.foss.kotlin.domain.IngestionCheckpointRepository
import com.onyx.foss.kotlin.domain.IngestionErrorEntity
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.IngestionJobEntity
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.http.MediaType
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64

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
) {
    @Transactional
    fun claimNext(): Long? {
        val job = jobs.lockNextQueued(Instant.now()) ?: return null
        job.state = JobState.RUNNING
        job.lockedAt = Instant.now()
        job.lockedBy = "spring-worker"
        job.attempts += 1
        jobs.save(job)
        return requireNotNull(job.id)
    }
}

@Service
class IngestionProcessor(
    private val admin: AdminService,
    private val pairs: ConnectorCredentialPairRepository,
    private val attempts: IngestionAttemptRepository,
    private val jobs: IngestionJobRepository,
    private val checkpoints: IngestionCheckpointRepository,
    private val errors: IngestionErrorRepository,
    private val documents: IndexedDocumentRepository,
    private val fileLoader: FileConnectorLoader,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val embedder: ModelServerClient,
    private val indexer: OpenSearchIndexer,
    private val pruning: PruningService,
    private val permissionSync: PermissionSyncWorker,
    private val mapper: ObjectMapper,
) {
    fun process(jobId: Long) {
        val job = jobs.findById(jobId).orElse(null) ?: return
        val attempt = attempts.findById(job.attemptId).orElse(null) ?: return
        val pair = pairs.findById(attempt.ccPairId).orElse(null) ?: return
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = Instant.now()
        attempts.save(attempt)
        if (pair.status == PairStatus.SCHEDULED) {
            pair.status = PairStatus.INITIAL_INDEXING
            pairs.save(pair)
        }
        var refreshFreq: Long? = null
        var runPermissionSync = false
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
            var newDocuments = 0
            var totalDocuments = 0
            var hasFailures = false
            var completeEnumeration = false
            val seenDocumentIds = mutableSetOf<String>()
            val failedDocumentIds = mutableSetOf<String>()
            batches.forEach { batch ->
                val batchFailedDocumentIds = batch.failures.mapNotNull { failure ->
                    (failure.target as? FailureTarget.Document)?.id
                }.toSet()
                failedDocumentIds += batchFailedDocumentIds
                batch.documents.forEach { document ->
                    if (!seenDocumentIds.add(document.id)) {
                        return@forEach
                    }
                    if (attempt.pruneOnly) return@forEach
                    if (document.title.isBlank() && document.content.isBlank()) {
                        return@forEach
                    }
                    val indexableContent = document.content.ifBlank { document.title }
                    val chunks = indexableContent.chunked(1500).filter { it.isNotBlank() }
                    if (chunks.isEmpty()) return@forEach
                    val vectors = embedder.embed(chunks)
                    chunks.zip(vectors).forEachIndexed { index, item ->
                        indexer.upsert(
                            pairId = requireNotNull(pair.id),
                            sourceDocumentId = document.id,
                            chunkId = index,
                            title = document.title,
                            content = item.first,
                            link = document.link,
                            metadata = document.metadata,
                            embedding = item.second,
                        )
                    }
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
                completeEnumeration = !batch.checkpoint.hasMore
            }
            attempt.docsRemovedFromIndex = pruning.prune(
                requireNotNull(pair.id),
                seenDocumentIds,
                failedDocumentIds,
                attempt.fromBeginning || attempt.pruneOnly,
                completeEnumeration,
            )
            if ((attempt.fromBeginning || attempt.pruneOnly) && completeEnumeration) pair.lastPrunedAt = Instant.now()
            if (!hasFailures) {
                val resolvedEntityErrors = errors.findUnresolvedEntityErrorsByCcPairId(requireNotNull(pair.id))
                    .onEach { it.isResolved = true }
                errors.saveAll(resolvedEntityErrors)
            }
            attempt.status = if (hasFailures) AttemptStatus.COMPLETED_WITH_ERRORS else AttemptStatus.SUCCESS
            attempt.newDocsIndexed = newDocuments
            attempt.totalDocsIndexed = totalDocuments
            attempts.save(attempt)
            pair.inRepeatedErrorState = false
            pair.status = PairStatus.ACTIVE
            pairs.save(pair)
            job.state = JobState.SUCCEEDED
            jobs.save(job)
            runPermissionSync = !attempt.pruneOnly
        } catch (error: Exception) {
            attempt.status = AttemptStatus.FAILED
            attempt.errorMessage = error.message?.take(1000) ?: "Ingestion failed"
            attempt.fullExceptionTrace = error.stackTraceToString().take(16000)
            attempts.save(attempt)
            errors.save(
                IngestionErrorEntity(
                    attemptId = requireNotNull(attempt.id),
                    failureMessage = attempt.errorMessage ?: "Ingestion failed",
                    errorType = error::class.simpleName,
                ),
            )
            pair.inRepeatedErrorState = isRepeatedError(
                refreshFreq,
                attempts.findAllByCcPairIdOrderByIdDesc(requireNotNull(pair.id)),
            )
            pairs.save(pair)
            job.state = JobState.FAILED
            job.lastError = attempt.errorMessage
            jobs.save(job)
        }
        if (runPermissionSync) permissionSync.process(requireNotNull(pair.id))
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
            .block() ?: error("Model server returned no embedding response")
        return response.path("embeddings").map { vector -> vector.map { it.asDouble() } }
    }
}

@Service
class OpenSearchIndexer(
    private val properties: OnyxProperties,
    private val clientBuilder: WebClient.Builder,
    private val mapper: ObjectMapper,
) {
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
        val response = clientBuilder.build()
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
                        Mono.error(IllegalStateException("OpenSearch ACL update failed: $it"))
                    }
                }
            }
            .block()
        val total = response?.path("total")?.asInt(-1) ?: -1
        val updated = response?.path("updated")?.asInt(-1) ?: -1
        val noops = response?.path("noops")?.asInt(-1) ?: -1
        check(
            response != null && !response.path("timed_out").asBoolean(true) &&
                response.path("failures").isArray && response.path("failures").isEmpty &&
                response.path("version_conflicts").asInt(-1) == 0 &&
                total >= accessByDocument.size && updated + noops == total,
        ) { "OpenSearch did not fully apply the ACL update" }
    }

    private fun deleteByQuery(query: Map<String, Any>, operation: String) {
        val response = clientBuilder.build()
            .post()
            .uri(
                properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index +
                    "/_delete_by_query?refresh=true",
            )
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(mapOf("query" to query))
            .exchangeToMono { result ->
                if (result.statusCode().is2xxSuccessful) result.releaseBody().thenReturn(true)
                else result.bodyToMono(String::class.java).flatMap {
                    Mono.error(IllegalStateException("OpenSearch $operation failed: $it"))
                }
            }
            .block()
        check(response == true) { "OpenSearch did not confirm $operation" }
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
        )
        val response = clientBuilder.build()
            .put()
            .uri(properties.opensearch.baseUrl.trimEnd('/') + "/" + properties.opensearch.index + "/_doc/" + documentId + "?refresh=true")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(mapper.valueToTree<JsonNode>(body))
            .exchangeToMono { result ->
                if (result.statusCode().is2xxSuccessful) result.releaseBody().thenReturn(true)
                else result.bodyToMono(String::class.java).flatMap { Mono.error(IllegalStateException("OpenSearch index write failed: " + it)) }
            }
            .block()
        check(response == true) { "OpenSearch did not confirm the index write" }
    }
}
