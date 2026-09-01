package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxEntity
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncStatus
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

data class DocumentSetSyncClaim(
    val id: Long,
    val token: UUID,
    val ccPairIds: List<Long>,
)

@Service
class DocumentSetSyncClaimService(
    private val outbox: DocumentSetSyncOutboxRepository,
    private val jdbc: JdbcTemplate,
) {
    @Transactional
    fun claimNext(now: Instant = Instant.now()): DocumentSetSyncClaim? {
        jdbc.queryForObject(
            "SELECT id FROM document_set_sync_claim_lock WHERE id = 1 FOR UPDATE",
            Short::class.java,
        )
        val active = outbox.lockOldestInProgress()
        if (active != null) {
            val lockedAt = active.lockedAt
            if (lockedAt != null && !lockedAt.isBefore(now.minusSeconds(LEASE_SECONDS))) return null
            return claim(active, now)
        }
        return outbox.lockNextPending()?.let { claim(it, now) }
    }

    private fun claim(row: DocumentSetSyncOutboxEntity, now: Instant): DocumentSetSyncClaim {
        val token = UUID.randomUUID()
        row.status = DocumentSetSyncStatus.IN_PROGRESS
        row.attemptCount += 1
        row.lockedAt = now
        row.claimToken = token
        outbox.save(row)
        return DocumentSetSyncClaim(
            id = requireNotNull(row.id),
            token = token,
            ccPairIds = row.ccPairIds.orEmpty().map { it.asLong() }.distinct(),
        )
    }

    @Transactional
    fun renew(id: Long, token: UUID, now: Instant = Instant.now()): Boolean =
        outbox.renewOwned(id, token, now) == 1

    @Transactional
    fun complete(id: Long, token: UUID): Boolean = outbox.completeOwned(id, token) == 1

    @Transactional
    fun retry(id: Long, token: UUID, error: Exception): Boolean = outbox.retryOwned(
        id,
        token,
        (error.message ?: error::class.simpleName ?: "Document Set sync failed").take(2000),
    ) == 1

    private fun JsonNode?.orEmpty(): List<JsonNode> = if (this?.isArray == true) toList() else emptyList()

    private companion object {
        const val LEASE_SECONDS = 60 * 60L
    }
}

@Component
class DocumentSetSyncWorker(
    private val properties: OnyxProperties,
    private val claims: DocumentSetSyncClaimService,
    private val documents: IndexedDocumentRepository,
    private val documentSets: DocumentSetRepository,
    private val indexer: OpenSearchIndexer,
) {
    @Scheduled(fixedDelayString = "\${onyx.worker.poll-delay-ms:1000}")
    fun work() {
        if (properties.worker.enabled) processNext()
    }

    fun processNext(): Boolean {
        val claim = claims.claimNext() ?: return false
        return process(claim)
    }

    fun process(claim: DocumentSetSyncClaim): Boolean {
        try {
            claim.ccPairIds.forEach { pairId ->
                if (!claims.renew(claim.id, claim.token)) return true
                if (!syncPair(claim, pairId)) return true
            }
            claims.complete(claim.id, claim.token)
        } catch (error: Exception) {
            claims.retry(claim.id, claim.token, error)
        }
        return true
    }

    private fun syncPair(claim: DocumentSetSyncClaim, pairId: Long): Boolean {
        val names = documentSets.findNamesByCcPairId(pairId)
        var afterSourceDocumentId = ""
        while (true) {
            val page = documents.findPageByCcPairId(pairId, afterSourceDocumentId, DOCUMENT_PAGE_SIZE)
            if (page.isEmpty()) return true
            if (!claims.renew(claim.id, claim.token)) return false
            indexer.updateDocumentSets(pairId, page.map { it.sourceDocumentId }.toSet(), names)
            if (page.size < DOCUMENT_PAGE_SIZE) return true
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private companion object {
        const val DOCUMENT_PAGE_SIZE = 500
    }
}
