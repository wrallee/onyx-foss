package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxEntity
import com.onyx.foss.kotlin.domain.DocumentSetSyncOutboxRepository
import com.onyx.foss.kotlin.domain.DocumentSetSyncStatus
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class DocumentSetSyncClaimService(
    private val outbox: DocumentSetSyncOutboxRepository,
) {
    @Transactional
    fun claimNext(): DocumentSetSyncOutboxEntity? {
        val row = outbox.lockNextPending() ?: return null
        row.status = DocumentSetSyncStatus.IN_PROGRESS
        row.attemptCount += 1
        return outbox.save(row)
    }

    @Transactional
    fun complete(id: Long) {
        val row = outbox.findById(id).orElseThrow()
        row.status = DocumentSetSyncStatus.DONE
        row.lastError = null
        outbox.save(row)
    }

    @Transactional
    fun retry(id: Long, error: Exception) {
        val row = outbox.findById(id).orElseThrow()
        row.status = DocumentSetSyncStatus.PENDING
        row.lastError = (error.message ?: error::class.simpleName ?: "Document Set sync failed").take(2000)
        outbox.save(row)
    }
}

@Component
class DocumentSetSyncWorker(
    private val properties: OnyxProperties,
    private val claims: DocumentSetSyncClaimService,
    private val documents: IndexedDocumentRepository,
    private val indexer: OpenSearchIndexer,
    private val jdbc: JdbcTemplate,
) {
    @Scheduled(fixedDelayString = "\${onyx.worker.poll-delay-ms:1000}")
    fun work() {
        if (properties.worker.enabled) processNext()
    }

    fun processNext(): Boolean {
        val row = claims.claimNext() ?: return false
        val rowId = requireNotNull(row.id)
        try {
            row.ccPairIds.orEmpty().map { it.asLong() }.distinct().forEach(::syncPair)
            claims.complete(rowId)
        } catch (error: Exception) {
            claims.retry(rowId, error)
        }
        return true
    }

    private fun syncPair(pairId: Long) {
        val names = jdbc.queryForList(
            """
                SELECT document_set.name
                FROM document_sets document_set
                JOIN document_set_cc_pairs membership ON membership.document_set_id = document_set.id
                WHERE membership.cc_pair_id = ?
                ORDER BY document_set.name
            """.trimIndent(),
            String::class.java,
            pairId,
        )
        var afterSourceDocumentId = ""
        while (true) {
            val page = documents.findPageByCcPairId(pairId, afterSourceDocumentId, DOCUMENT_PAGE_SIZE)
            if (page.isEmpty()) return
            indexer.updateDocumentSets(pairId, page.map { it.sourceDocumentId }.toSet(), names)
            if (page.size < DOCUMENT_PAGE_SIZE) return
            afterSourceDocumentId = page.last().sourceDocumentId
        }
    }

    private fun JsonNode?.orEmpty(): List<JsonNode> =
        if (this?.isArray == true) toList() else emptyList()

    private companion object {
        const val DOCUMENT_PAGE_SIZE = 500
    }
}
