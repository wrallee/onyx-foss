package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import com.onyx.foss.kotlin.domain.PermissionSyncStageRepository
import com.onyx.foss.kotlin.domain.PermissionSyncStageRow
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Instant
import java.util.UUID

data class PermissionSyncClaim(
    val attemptId: Long,
    val pairId: Long,
    val token: UUID,
)

@Service
class PermissionSyncClaimService(
    private val attempts: PermissionSyncAttemptRepository,
) {
    @Transactional
    fun enqueue(pairId: Long): Boolean = attempts.createOrCoalesce(pairId) == 1

    @Transactional
    fun claimNext(now: Instant = Instant.now()): PermissionSyncClaim? =
        attempts.lockNextClaimable(now)?.let { claim(it, now) }

    @Transactional
    fun claimForPair(pairId: Long, now: Instant = Instant.now()): PermissionSyncClaim? =
        attempts.lockClaimableForPair(pairId, now)?.let { claim(it, now) }

    private fun claim(attempt: com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity, now: Instant): PermissionSyncClaim {
        val token = UUID.randomUUID()
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = attempt.timeStarted ?: now
        attempt.timeFinished = null
        attempt.claimToken = token
        attempt.leaseExpiresAt = now.plus(PERMISSION_SYNC_LEASE)
        attempts.saveAndFlush(attempt)
        return PermissionSyncClaim(requireNotNull(attempt.id), attempt.ccPairId, token)
    }

    fun renew(claim: PermissionSyncClaim, now: Instant = Instant.now()): Boolean =
        attempts.renewOwned(claim.attemptId, claim.token, now.plus(PERMISSION_SYNC_LEASE)) == 1

    fun updateCounts(claim: PermissionSyncClaim, total: Int, errors: Int): Boolean =
        attempts.updateCountsOwned(claim.attemptId, claim.token, total, errors) == 1

    @Transactional
    fun complete(claim: PermissionSyncClaim, status: AttemptStatus): Boolean = finish(claim) { attempt ->
        attempt.status = status
    }

    @Transactional
    fun fail(claim: PermissionSyncClaim, error: Exception): Boolean = finish(claim) { attempt ->
        attempt.status = AttemptStatus.FAILED
        attempt.errorMessage = (error.message ?: "Permission sync failed").take(1000)
        attempt.fullExceptionTrace = error.stackTraceToString().take(16000)
    }

    private fun finish(
        claim: PermissionSyncClaim,
        update: (com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity) -> Unit,
    ): Boolean {
        val attempt = attempts.lockOwned(claim.attemptId, claim.token) ?: return false
        val followUpRequested = attempt.followUpRequested
        update(attempt)
        attempt.claimToken = null
        attempt.leaseExpiresAt = null
        attempt.followUpRequested = false
        attempt.timeFinished = Instant.now()
        attempts.saveAndFlush(attempt)
        if (followUpRequested) {
            attempts.save(
                com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity(ccPairId = claim.pairId),
            )
        }
        return true
    }
}

@Component
class PermissionSyncScheduledWorker(
    private val properties: OnyxProperties,
    private val worker: PermissionSyncWorker,
) {
    @Scheduled(fixedDelayString = "\${onyx.worker.poll-delay-ms:1000}")
    fun work() {
        if (properties.worker.enabled) worker.processNext()
    }
}

@Service
class PermissionSyncWorker(
    private val admin: AdminService,
    private val pairs: ConnectorCredentialPairRepository,
    private val claims: PermissionSyncClaimService,
    private val documents: IndexedDocumentRepository,
    private val staging: PermissionSyncStageRepository,
    private val remoteLoaders: RemoteConnectorLoaders,
    private val indexer: OpenSearchIndexer,
    private val mapper: ObjectMapper,
) {
    fun enqueue(pairId: Long) {
        claims.enqueue(pairId)
    }

    fun processNext(): Boolean {
        val claim = claims.claimNext() ?: return false
        process(claim)
        return true
    }

    fun process(pairId: Long) {
        enqueue(pairId)
        claims.claimForPair(pairId)?.let(::process)
    }

    fun process(claim: PermissionSyncClaim) {
        val pairId = claim.pairId
        val pair = pairs.findById(pairId).orElse(null) ?: return
        staging.deleteTerminalForPair(pairId)
        renew(claim)
        try {
            val connector = admin.connector(pair.connectorId)
            val unboundErrors = if (connector.source == ConnectorSource.FILE) {
                stageFile(claim, pairId, pair.accessType.equals("public", ignoreCase = true))
                0
            } else {
                stageRemote(
                    claim,
                    remoteLoaders.loadSlim(
                        connector.source,
                        connector.connectorSpecificConfig,
                        admin.credentialSecret(pair.credentialId),
                        start = connector.indexingStart,
                        includePermissions = true,
                        heartbeat = { renew(claim) },
                    ),
                )
            }
            val total = Math.toIntExact(staging.countForAttempt(claim.attemptId))
            val errorCount = Math.toIntExact(staging.countErrorsForAttempt(claim.attemptId)) + unboundErrors
            if (!claims.updateCounts(claim, total, errorCount)) throw StalePermissionClaimException()
            publish(claim, pairId)
            val status = if (errorCount > 0) {
                AttemptStatus.COMPLETED_WITH_ERRORS
            } else {
                AttemptStatus.SUCCESS
            }
            if (claims.complete(claim, status)) staging.deleteAllForAttempt(claim.attemptId)
        } catch (_: StalePermissionClaimException) {
            return
        } catch (error: Exception) {
            if (claims.fail(claim, error)) staging.deleteAllForAttempt(claim.attemptId)
        }
    }

    private fun stageFile(claim: PermissionSyncClaim, pairId: Long, isPublic: Boolean) {
        var afterSourceDocumentId = ""
        val access = mapper.valueToTree<JsonNode>(ExternalAccess(isPublic = isPublic))
        while (true) {
            renew(claim)
            val page = documents.findPageByCcPairId(pairId, afterSourceDocumentId, PAGE_SIZE)
            if (page.isEmpty()) return
            staging.upsert(
                claim.attemptId,
                page.map { PermissionSyncStageRow(it.sourceDocumentId, access, hasError = false) },
            )
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private fun stageRemote(claim: PermissionSyncClaim, batches: Sequence<ConnectorBatch>): Int {
        var unboundErrors = 0
        val iterator = batches.iterator()
        while (true) {
            renew(claim)
            if (!iterator.hasNext()) return unboundErrors
            val batch = iterator.next()
            renew(claim)
            batch.documents.asSequence().chunked(PAGE_SIZE).forEach { page ->
                renew(claim)
                staging.upsert(
                    claim.attemptId,
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
                renew(claim)
                val documentFailures = page.mapNotNull { failure ->
                    val target = failure.target as? FailureTarget.Document
                    if (target == null) {
                        unboundErrors += 1
                        null
                    } else {
                        PermissionSyncStageRow(
                            target.id,
                            mapper.valueToTree(PRIVATE_ACCESS),
                            hasError = true,
                        )
                    }
                }
                staging.upsert(claim.attemptId, documentFailures)
            }
        }
    }

    private fun publish(claim: PermissionSyncClaim, pairId: Long) {
        var afterSourceDocumentId = ""
        while (true) {
            renew(claim)
            val page = staging.findPage(claim.attemptId, afterSourceDocumentId, PAGE_SIZE)
            if (page.isEmpty()) return
            val rowsById = page.associateBy(PermissionSyncStageRow::sourceDocumentId)
            val storedDocuments = documents.findAllByCcPairIdAndSourceDocumentIdIn(pairId, rowsById.keys)
            if (storedDocuments.isNotEmpty()) publishPage(claim, pairId, storedDocuments, rowsById)
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private fun publishPage(
        claim: PermissionSyncClaim,
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
        renew(claim)
        indexer.updateAccess(pairId, privateFence)
        renew(claim)
        storedDocuments.forEach { document ->
            document.externalAccess = requireNotNull(rowsById[document.sourceDocumentId]).externalAccess
        }
        try {
            documents.saveAllAndFlush(storedDocuments)
        } catch (error: Exception) {
            restoreAfterDatabaseFailure(claim, pairId, storedDocuments, previousJson, previousAccess, privateFence)
            throw error
        }
        try {
            renew(claim)
            indexer.updateAccess(pairId, targetAccess)
        } catch (error: Exception) {
            if (claims.renew(claim)) runCatching { indexer.updateAccess(pairId, privateFence) }
            throw error
        }
        renew(claim)
    }

    private fun restoreAfterDatabaseFailure(
        claim: PermissionSyncClaim,
        pairId: Long,
        storedDocuments: List<IndexedDocumentEntity>,
        previousJson: Map<String, JsonNode?>,
        previousAccess: Map<String, ExternalAccess>,
        privateFence: Map<String, ExternalAccess>,
    ) {
        if (!claims.renew(claim)) return
        runCatching { indexer.updateAccess(pairId, privateFence) }
        if (!claims.renew(claim)) return
        val databaseRestored = runCatching {
            storedDocuments.forEach { document -> document.externalAccess = previousJson[document.sourceDocumentId] }
            documents.saveAllAndFlush(storedDocuments)
        }.isSuccess
        if (!databaseRestored || previousAccess.isEmpty()) return
        if (!claims.renew(claim)) return
        if (runCatching { indexer.updateAccess(pairId, previousAccess) }.isFailure) {
            if (claims.renew(claim)) runCatching { indexer.updateAccess(pairId, privateFence) }
        }
    }

    private fun renew(claim: PermissionSyncClaim) {
        if (!claims.renew(claim)) throw StalePermissionClaimException()
    }

    private companion object {
        const val PAGE_SIZE = 500
        val PRIVATE_ACCESS = ExternalAccess(isPublic = false)
    }
}

private class StalePermissionClaimException : RuntimeException()

internal val PERMISSION_SYNC_LEASE: Duration = Duration.ofMinutes(1)
