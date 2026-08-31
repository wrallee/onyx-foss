package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class PermissionSyncWorker(
    private val admin: AdminService,
    private val pairs: ConnectorCredentialPairRepository,
    private val attempts: PermissionSyncAttemptRepository,
    private val documents: IndexedDocumentRepository,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val indexer: OpenSearchIndexer,
    private val mapper: ObjectMapper,
) {
    fun process(pairId: Long) {
        val pair = pairs.findById(pairId).orElse(null) ?: return
        if (attempts.createIfNoActive(pairId) == 0) return
        val attempt = requireNotNull(attempts.findFirstByCcPairIdOrderByIdDesc(pairId))
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = Instant.now()
        attempts.save(attempt)
        try {
            val connector = admin.connector(pair.connectorId)
            val result = if (connector.source == ConnectorSource.FILE) {
                val access = ExternalAccess(isPublic = pair.accessType.equals("public", ignoreCase = true))
                PermissionUpdates(
                    documents.findAllByCcPairId(pairId).associate { it.sourceDocumentId to access },
                    hasPermissionErrors = false,
                )
            } else {
                collectRemoteAccess(
                    pairId,
                    remoteLoaders.loadSlim(
                        connector.source,
                        connector.connectorSpecificConfig,
                        admin.credentialSecret(pair.credentialId),
                        start = connector.indexingStart,
                        includePermissions = true,
                    ),
                )
            }
            val updates = result.accessByDocument
            val persisted = updates.mapNotNull { (sourceDocumentId, access) ->
                documents.findByCcPairIdAndSourceDocumentId(pairId, sourceDocumentId)?.apply {
                    externalAccess = mapper.valueToTree(access)
                }
            }
            val persistedAccess = persisted.associate { document ->
                document.sourceDocumentId to requireNotNull(updates[document.sourceDocumentId])
            }
            indexer.updateAccess(pairId, persistedAccess)
            documents.saveAll(persisted)
            attempt.status = if (result.hasPermissionErrors) {
                AttemptStatus.COMPLETED_WITH_ERRORS
            } else {
                AttemptStatus.SUCCESS
            }
            attempt.timeFinished = Instant.now()
            attempts.save(attempt)
        } catch (error: Exception) {
            attempt.status = AttemptStatus.FAILED
            attempt.errorMessage = error.message?.take(1000) ?: "Permission sync failed"
            attempt.timeFinished = Instant.now()
            attempts.save(attempt)
        }
    }

    private fun collectRemoteAccess(pairId: Long, batches: Sequence<ConnectorBatch>): PermissionUpdates {
        val updates = linkedMapOf<String, ExternalAccess>()
        var hasErrors = false
        batches.forEach { batch ->
            batch.documents.forEach { document ->
                val access = document.externalAccess
                if (access == null) hasErrors = true
                updates[document.id] = access ?: PRIVATE_ACCESS
            }
            batch.failures.forEach { failure ->
                hasErrors = true
                val target = failure.target as? FailureTarget.Document ?: return@forEach
                findDocument(pairId, target)?.let { updates[it.sourceDocumentId] = PRIVATE_ACCESS }
            }
        }
        return PermissionUpdates(updates, hasErrors)
    }

    private fun findDocument(pairId: Long, target: FailureTarget.Document): IndexedDocumentEntity? =
        documents.findByCcPairIdAndSourceDocumentId(pairId, target.id)
            ?: target.link?.let { documents.findByCcPairIdAndSourceDocumentId(pairId, it) }

    private data class PermissionUpdates(
        val accessByDocument: Map<String, ExternalAccess>,
        val hasPermissionErrors: Boolean,
    )

    private companion object {
        val PRIVATE_ACCESS = ExternalAccess(isPublic = false)
    }
}
