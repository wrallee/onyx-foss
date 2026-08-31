package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class PruningService(
    private val documents: IndexedDocumentRepository,
    private val indexer: OpenSearchIndexer,
) {
    @Transactional
    fun prune(
        pairId: Long,
        seenDocumentIds: Set<String>,
        failedDocumentIds: Set<String>,
        fromBeginning: Boolean,
        completeEnumeration: Boolean,
    ): Int {
        if (!fromBeginning || !completeEnumeration) return 0
        val removedIds = documents.findSourceIdsByCcPairId(pairId).toSet() - (seenDocumentIds + failedDocumentIds)
        if (removedIds.isNotEmpty()) {
            indexer.deleteDocuments(pairId, removedIds)
            documents.deleteByCcPairIdAndSourceDocumentIdIn(pairId, removedIds)
        }
        return removedIds.size
    }
}
