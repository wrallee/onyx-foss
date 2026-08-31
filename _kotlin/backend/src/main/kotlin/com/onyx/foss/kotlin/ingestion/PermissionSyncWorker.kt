package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import com.onyx.foss.kotlin.domain.PermissionSyncStageRepository
import com.onyx.foss.kotlin.domain.PermissionSyncStageRow
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class PermissionSyncWorker(
    private val admin: AdminService,
    private val pairs: ConnectorCredentialPairRepository,
    private val attempts: PermissionSyncAttemptRepository,
    private val documents: IndexedDocumentRepository,
    private val staging: PermissionSyncStageRepository,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val indexer: OpenSearchIndexer,
    private val mapper: ObjectMapper,
) {
    fun process(pairId: Long) {
        val pair = pairs.findById(pairId).orElse(null) ?: return
        recoverStaleAttempt(pairId)
        staging.deleteTerminalForPair(pairId)
        if (attempts.createIfNoActive(pairId) == 0) return
        val attempt = requireNotNull(attempts.findFirstByCcPairIdOrderByIdDesc(pairId))
        val attemptId = requireNotNull(attempt.id)
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = Instant.now()
        attempts.save(attempt)
        try {
            val connector = admin.connector(pair.connectorId)
            val unboundErrors = if (connector.source == ConnectorSource.FILE) {
                stageFile(attemptId, pairId, pair.accessType.equals("public", ignoreCase = true))
                0
            } else {
                stageRemote(
                    attemptId,
                    remoteLoaders.loadSlim(
                        connector.source,
                        connector.connectorSpecificConfig,
                        admin.credentialSecret(pair.credentialId),
                        start = connector.indexingStart,
                        includePermissions = true,
                    ),
                )
            }
            attempt.totalDocsSynced = Math.toIntExact(staging.countForAttempt(attemptId))
            attempt.docsWithPermissionErrors = Math.toIntExact(staging.countErrorsForAttempt(attemptId)) + unboundErrors
            attempts.save(attempt)
            publish(attemptId, pairId)
            staging.deleteAllForAttempt(attemptId)
            attempt.status = if (attempt.docsWithPermissionErrors > 0) {
                AttemptStatus.COMPLETED_WITH_ERRORS
            } else {
                AttemptStatus.SUCCESS
            }
            attempt.timeFinished = Instant.now()
            attempts.save(attempt)
        } catch (error: Exception) {
            staging.deleteAllForAttempt(attemptId)
            attempt.status = AttemptStatus.FAILED
            attempt.errorMessage = error.message?.take(1000) ?: "Permission sync failed"
            attempt.fullExceptionTrace = error.stackTraceToString().take(16000)
            attempt.timeFinished = Instant.now()
            attempts.save(attempt)
        }
    }

    private fun recoverStaleAttempt(pairId: Long) {
        attempts.failStaleActive(
            ccPairId = pairId,
            cutoff = Instant.now().minusSeconds(STALE_AFTER_SECONDS),
            message = STALE_MESSAGE,
            trace = STALE_MESSAGE,
        )
    }

    private fun stageFile(attemptId: Long, pairId: Long, isPublic: Boolean) {
        var afterSourceDocumentId = ""
        val access = mapper.valueToTree<JsonNode>(ExternalAccess(isPublic = isPublic))
        while (true) {
            val page = documents.findPageByCcPairId(pairId, afterSourceDocumentId, PAGE_SIZE)
            if (page.isEmpty()) return
            staging.upsert(
                attemptId,
                page.map { PermissionSyncStageRow(it.sourceDocumentId, access, hasError = false) },
            )
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private fun stageRemote(attemptId: Long, batches: Sequence<ConnectorBatch>): Int {
        var unboundErrors = 0
        batches.forEach { batch ->
            batch.documents.asSequence().chunked(PAGE_SIZE).forEach { page ->
                staging.upsert(
                    attemptId,
                    page.map { document ->
                        val access = document.externalAccess
                        PermissionSyncStageRow(
                            document.id,
                            mapper.valueToTree(access ?: PRIVATE_ACCESS),
                            hasError = access == null,
                        )
                    },
                )
            }
            batch.failures.asSequence().chunked(PAGE_SIZE).forEach { page ->
                val documentFailures = page.mapNotNull { failure ->
                    val target = failure.target as? FailureTarget.Document
                    if (target == null) {
                        unboundErrors += 1
                        null
                    } else {
                        PermissionSyncStageRow(
                            target.link ?: target.id,
                            mapper.valueToTree(PRIVATE_ACCESS),
                            hasError = true,
                        )
                    }
                }
                staging.upsert(attemptId, documentFailures)
            }
        }
        return unboundErrors
    }

    private fun publish(attemptId: Long, pairId: Long) {
        var afterSourceDocumentId = ""
        while (true) {
            val page = staging.findPage(attemptId, afterSourceDocumentId, PAGE_SIZE)
            if (page.isEmpty()) return
            val rowsById = page.associateBy(PermissionSyncStageRow::sourceDocumentId)
            val storedDocuments = documents.findAllByCcPairIdAndSourceDocumentIdIn(pairId, rowsById.keys)
            if (storedDocuments.isNotEmpty()) publishPage(pairId, storedDocuments, rowsById)
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private fun publishPage(
        pairId: Long,
        storedDocuments: List<IndexedDocumentEntity>,
        rowsById: Map<String, PermissionSyncStageRow>,
    ) {
        val previousJson = storedDocuments.associate { it.sourceDocumentId to it.externalAccess?.deepCopy<JsonNode>() }
        val previousAccess = previousJson.mapNotNull { (sourceDocumentId, value) ->
            value?.let { sourceDocumentId to mapper.treeToValue(it, ExternalAccess::class.java) }
        }.toMap()
        val targetAccess = storedDocuments.associate { document ->
            document.sourceDocumentId to mapper.treeToValue(
                requireNotNull(rowsById[document.sourceDocumentId]).externalAccess,
                ExternalAccess::class.java,
            )
        }
        val privateFence = targetAccess.keys.associateWith { PRIVATE_ACCESS }
        try {
            indexer.updateAccess(pairId, privateFence)
            storedDocuments.forEach { document ->
                document.externalAccess = requireNotNull(rowsById[document.sourceDocumentId]).externalAccess
            }
            documents.saveAllAndFlush(storedDocuments)
            indexer.updateAccess(pairId, targetAccess)
        } catch (error: Exception) {
            restoreOrFence(pairId, storedDocuments, previousJson, previousAccess, privateFence)
            throw error
        }
    }

    private fun restoreOrFence(
        pairId: Long,
        storedDocuments: List<IndexedDocumentEntity>,
        previousJson: Map<String, JsonNode?>,
        previousAccess: Map<String, ExternalAccess>,
        privateFence: Map<String, ExternalAccess>,
    ) {
        runCatching { indexer.updateAccess(pairId, privateFence) }
        val databaseRestored = runCatching {
            storedDocuments.forEach { document -> document.externalAccess = previousJson[document.sourceDocumentId] }
            documents.saveAllAndFlush(storedDocuments)
        }.isSuccess
        if (!databaseRestored || previousAccess.isEmpty()) return
        if (runCatching { indexer.updateAccess(pairId, previousAccess) }.isFailure) {
            runCatching { indexer.updateAccess(pairId, privateFence) }
        }
    }

    private companion object {
        const val PAGE_SIZE = 500
        const val STALE_AFTER_SECONDS = 60 * 60L
        const val STALE_MESSAGE = "Recovered stale permission sync attempt older than one hour"
        val PRIVATE_ACCESS = ExternalAccess(isPublic = false)
    }
}
