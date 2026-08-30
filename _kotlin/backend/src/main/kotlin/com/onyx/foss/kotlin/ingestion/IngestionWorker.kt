package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionCheckpointEntity
import com.onyx.foss.kotlin.domain.IngestionCheckpointRepository
import com.onyx.foss.kotlin.domain.IngestionErrorEntity
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.IngestionJobEntity
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.service.FileStorageService
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.BodyContentHandler
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64

data class SourceDocument(
    val id: String,
    val title: String,
    val content: String,
    val link: String? = null,
    val metadata: Map<String, Any?> = emptyMap(),
)

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
    private val fileLoader: FileDocumentLoader,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val embedder: ModelServerClient,
    private val indexer: OpenSearchIndexer,
    private val mapper: ObjectMapper,
) {
    fun process(jobId: Long) {
        val job = jobs.findById(jobId).orElse(null) ?: return
        val attempt = attempts.findById(job.attemptId).orElse(null) ?: return
        val pair = pairs.findById(attempt.ccPairId).orElse(null) ?: return
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = Instant.now()
        attempts.save(attempt)
        try {
            val connector = admin.connector(pair.connectorId)
            val checkpoint = checkpoints.findById(requireNotNull(pair.id)).orElse(null)?.checkpointJson
            val sourceDocuments = when (connector.source) {
                ConnectorSource.FILE -> fileLoader.load(connector.connectorSpecificConfig)
                else -> remoteLoaders.load(connector.source.value, connector.connectorSpecificConfig, admin.credentialSecret(pair.credentialId), checkpoint)
            }
            var newDocuments = 0
            sourceDocuments.forEach { document ->
                val chunks = document.content.chunked(1500).filter { it.isNotBlank() }
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
                    lastSynced = Instant.now()
                }
                documents.save(indexedDocument)
            }
            attempt.status = AttemptStatus.SUCCESS
            attempt.newDocsIndexed = newDocuments
            attempt.totalDocsIndexed = sourceDocuments.size
            attempt.pollRangeEnd = Instant.now()
            attempts.save(attempt)
            checkpoints.save(
                IngestionCheckpointEntity(
                    ccPairId = requireNotNull(pair.id),
                    checkpointJson = mapper.valueToTree(
                        mapOf("last_success_at" to Instant.now().toString(), "documents" to sourceDocuments.size),
                    ),
                ),
            )
            job.state = JobState.SUCCEEDED
            jobs.save(job)
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
            pair.inRepeatedErrorState = true
            pairs.save(pair)
            job.state = JobState.FAILED
            job.lastError = attempt.errorMessage
            jobs.save(job)
        }
    }

    private fun hash(value: String): String =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray(StandardCharsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
}

@Service
class FileDocumentLoader(
    private val mapper: ObjectMapper,
    private val files: FileStorageService,
) {
    fun load(config: JsonNode?): List<SourceDocument> {
        val locations = config?.path("file_locations") ?: return emptyList()
        return locations.map { location ->
            val assetId = location.asText()
            val path = files.filePath(assetId)
            val handler = BodyContentHandler(-1)
            Files.newInputStream(path).use { input ->
                AutoDetectParser().parse(input, handler, Metadata())
            }
            SourceDocument(
                id = "FILE_CONNECTOR__" + assetId,
                title = path.fileName.toString(),
                content = handler.toString().trim(),
                metadata = mapOf("source" to "file", "file_id" to assetId),
            )
        }
    }
}

@Service
class ModelServerClient(
    private val properties: OnyxProperties,
    private val clientBuilder: WebClient.Builder,
) {
    fun embed(texts: List<String>): List<List<Double>> {
        require(properties.modelServer.modelName.isNotBlank()) {
            "ONYX_EMBEDDING_MODEL_NAME must be configured before file ingestion"
        }
        val response = clientBuilder.build()
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
