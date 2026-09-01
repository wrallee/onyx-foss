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
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Clock
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
    private val pairs: ConnectorCredentialPairRepository,
    private val documents: IndexedDocumentRepository,
    private val clock: Clock,
) {
    @Transactional
    fun enqueue(pairId: Long): Boolean {
        checkNotNull(pairs.lockById(pairId)) { "CC pair $pairId does not exist" }
        val active = attempts.findFirstByCcPairIdAndActiveMarkerOrderById(pairId, ACTIVE_MARKER)
        if (active == null) {
            attempts.save(com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity(ccPairId = pairId))
        } else if (active.status == AttemptStatus.IN_PROGRESS) {
            active.followUpRequested = true
            attempts.save(active)
        }
        return true
    }

    @Transactional
    fun claimNext(): PermissionSyncClaim? {
        val now = clock.instant()
        return attempts.findClaimableIds(now, PageRequest.of(0, CLAIM_CANDIDATE_LIMIT))
            .firstNotNullOfOrNull { claim(it, now) }
    }

    @Transactional
    fun claimForPair(pairId: Long): PermissionSyncClaim? =
        attempts.findFirstByCcPairIdAndActiveMarkerOrderById(pairId, ACTIVE_MARKER)?.id?.let {
            claim(it, clock.instant())
        }

    private fun claim(attemptId: Long, now: Instant): PermissionSyncClaim? {
        val snapshot = attempts.findById(attemptId).orElse(null) ?: return null
        pairs.lockById(snapshot.ccPairId) ?: return null
        val attempt = attempts.lockById(attemptId) ?: return null
        if (attempt.status != AttemptStatus.NOT_STARTED &&
            (attempt.status != AttemptStatus.IN_PROGRESS || attempt.leaseExpiresAt?.isAfter(now) == true)
        ) return null
        val token = UUID.randomUUID()
        val reclaimed = attempt.status == AttemptStatus.IN_PROGRESS
        attempt.status = AttemptStatus.IN_PROGRESS
        attempt.timeStarted = attempt.timeStarted ?: now
        attempt.timeFinished = null
        attempt.claimToken = token
        attempt.leaseExpiresAt = now.plus(PERMISSION_SYNC_LEASE)
        if (reclaimed) attempt.followUpRequested = false
        attempts.saveAndFlush(attempt)
        return PermissionSyncClaim(requireNotNull(attempt.id), attempt.ccPairId, token)
    }

    @Transactional
    fun renew(claim: PermissionSyncClaim): Boolean {
        val owner = lockLiveOwner(claim) ?: return false
        owner.attempt.leaseExpiresAt = owner.now.plus(PERMISSION_SYNC_LEASE)
        attempts.saveAndFlush(owner.attempt)
        return true
    }

    @Transactional
    fun updateCounts(claim: PermissionSyncClaim, total: Int, errors: Int): Boolean {
        val owner = lockLiveOwner(claim) ?: return false
        owner.attempt.totalDocsSynced = total
        owner.attempt.docsWithPermissionErrors = errors
        attempts.saveAndFlush(owner.attempt)
        return true
    }

    @Transactional
    fun saveDocumentAccess(
        claim: PermissionSyncClaim,
        storedDocuments: List<IndexedDocumentEntity>,
        accessByDocument: Map<String, JsonNode?>,
    ): Boolean {
        val owner = lockLiveOwner(claim) ?: return false
        storedDocuments.forEach { document ->
            document.externalAccess = accessByDocument[document.sourceDocumentId]
        }
        documents.saveAllAndFlush(storedDocuments)
        if (!isLive(owner.attempt, clock.instant())) throw StalePermissionClaimException()
        return true
    }

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
        val owner = lockLiveOwner(claim) ?: return false
        val attempt = owner.attempt
        val followUpRequested = attempt.followUpRequested
        update(attempt)
        attempt.claimToken = null
        attempt.leaseExpiresAt = null
        attempt.followUpRequested = false
        attempt.timeFinished = owner.now
        attempt.activeMarker = null
        attempts.saveAndFlush(attempt)
        if (followUpRequested) {
            attempts.save(
                com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity(ccPairId = claim.pairId),
            )
        }
        return true
    }

    private fun lockLiveOwner(
        claim: PermissionSyncClaim,
    ): LivePermissionOwner? {
        val attempt = attempts.lockOwned(claim.attemptId, claim.token) ?: return null
        val now = clock.instant()
        return attempt.takeIf { isLive(it, now) }?.let { LivePermissionOwner(it, now) }
    }

    private fun isLive(
        attempt: com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity,
        now: Instant,
    ): Boolean = attempt.status == AttemptStatus.IN_PROGRESS &&
        attempt.leaseExpiresAt?.isAfter(now) == true

    private data class LivePermissionOwner(
        val attempt: com.onyx.foss.kotlin.domain.PermissionSyncAttemptEntity,
        val now: Instant,
    )

    private companion object {
        const val ACTIVE_MARKER: Short = 1
        const val CLAIM_CANDIDATE_LIMIT = 10
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
            val page = documents.findAllByCcPairIdAndSourceDocumentIdGreaterThanOrderBySourceDocumentId(
                pairId,
                afterSourceDocumentId,
                PageRequest.of(0, PAGE_SIZE),
            )
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
        val targetJson = storedDocuments.associate { document ->
            document.sourceDocumentId to requireNotNull(rowsById[document.sourceDocumentId]).externalAccess
        }
        val targetAccess = targetJson.mapValues { (_, access) ->
            mapper.treeToValue(access, ExternalAccess::class.java)
        }
        val privateFence = targetAccess.keys.associateWith { PRIVATE_ACCESS }
        renew(claim)
        indexer.updateAccess(pairId, privateFence)
        renew(claim)
        try {
            if (!claims.saveDocumentAccess(claim, storedDocuments, targetJson)) throw StalePermissionClaimException()
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
            claims.saveDocumentAccess(claim, storedDocuments, previousJson)
        }.getOrDefault(false)
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
