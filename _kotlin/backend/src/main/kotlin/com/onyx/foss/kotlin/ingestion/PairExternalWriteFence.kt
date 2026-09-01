package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.OpenSearchIndexMigrationLockRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class PairExternalWriteFence(
    private val pairs: ConnectorCredentialPairRepository,
    private val indexMigrationLock: OpenSearchIndexMigrationLockRepository,
) {
    @Transactional
    fun <T> withPair(pairId: Long, action: () -> T): T = withPairs(listOf(pairId), action)

    @Transactional
    fun <T> withPairs(pairIds: Collection<Long>, action: () -> T): T {
        val orderedPairIds = pairIds.distinct().sorted()
        require(orderedPairIds.all { it > 0 }) { "CC pair IDs must be positive" }
        orderedPairIds.forEach { pairId -> checkNotNull(pairs.lockById(pairId)) { "CC pair $pairId does not exist" } }
        return action()
    }

    @Transactional
    fun withOpenSearchIndex(indexName: String, action: () -> Unit) {
        require(indexName.isNotBlank()) { "OpenSearch index name must not be blank" }
        indexMigrationLock.lock()
        action()
    }
}
