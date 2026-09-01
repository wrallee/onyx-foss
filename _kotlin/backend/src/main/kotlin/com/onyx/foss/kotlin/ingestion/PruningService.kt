package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.IngestionEnumerationRepository
import org.springframework.stereotype.Service

@Service
class PruningService(
    private val documents: IndexedDocumentRepository,
    private val enumeration: IngestionEnumerationRepository,
    private val indexer: OpenSearchIndexer,
) {
    fun prune(
        pairId: Long,
        attemptId: Long,
        fromBeginning: Boolean,
        completeEnumeration: Boolean,
        beforeDelete: () -> Unit = {},
    ): Int {
        if (!fromBeginning || !completeEnumeration) return 0
        var afterSourceDocumentId = ""
        var removed = 0
        while (true) {
            val removedIds = enumeration.findMissingPage(pairId, attemptId, afterSourceDocumentId, PRUNE_PAGE_SIZE)
            if (removedIds.isEmpty()) return removed
            beforeDelete()
            indexer.deleteDocuments(pairId, removedIds.toSet())
            documents.deleteByCcPairIdAndSourceDocumentIdIn(pairId, removedIds)
            removed += removedIds.size
            if (removedIds.size < PRUNE_PAGE_SIZE) return removed
            afterSourceDocumentId = removedIds.last()
        }
    }

    private companion object {
        const val PRUNE_PAGE_SIZE = 500
    }
}
