package com.onyx.foss.kotlin.ingestion

import org.springframework.stereotype.Service
import java.sql.Connection
import javax.sql.DataSource

@Service
class PairExternalWriteFence(
    private val dataSource: DataSource,
) {
    fun <T> withPair(pairId: Long, action: () -> T): T = withPairs(listOf(pairId), action)

    fun <T> withPairs(pairIds: Collection<Long>, action: () -> T): T {
        val orderedPairIds = pairIds.distinct().sorted()
        require(orderedPairIds.all { it > 0 }) { "CC pair IDs must be positive" }
        return withLocks(orderedPairIds.map(::pairLockName), action)
    }

    fun withOpenSearchIndex(indexName: String, action: () -> Unit) {
        require(indexName.isNotBlank()) { "OpenSearch index name must not be blank" }
        withLocks(listOf("onyx:opensearch-index-migration:$indexName"), action)
    }

    private fun <T> withLocks(lockNames: List<String>, action: () -> T): T {
        if (lockNames.isEmpty()) return action()
        dataSource.connection.use { connection ->
            check(connection.autoCommit) { "Pair external-write fences require an auto-commit connection" }
            val acquired = mutableListOf<String>()
            var primaryFailure: Throwable? = null
            try {
                lockNames.forEach { lockName ->
                    lock(connection, lockName)
                    acquired += lockName
                }
                return action()
            } catch (error: Throwable) {
                primaryFailure = error
                throw error
            } finally {
                releaseAll(connection, acquired, primaryFailure)
            }
        }
    }

    private fun releaseAll(connection: Connection, acquired: List<String>, primaryFailure: Throwable?) {
        var releaseFailure: Throwable? = null
        acquired.asReversed().forEach { lockName ->
            try {
                unlock(connection, lockName)
            } catch (error: Throwable) {
                if (releaseFailure == null) releaseFailure = error else releaseFailure.addSuppressed(error)
            }
        }
        releaseFailure?.let { failure ->
            if (primaryFailure == null) throw failure
            primaryFailure.addSuppressed(failure)
        }
    }

    private fun lock(connection: Connection, lockName: String) {
        connection.prepareStatement("SELECT pg_advisory_lock(hashtextextended(?, 0))").use { statement ->
            statement.setString(1, lockName)
            statement.execute()
        }
    }

    private fun unlock(connection: Connection, lockName: String) {
        connection.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))").use { statement ->
            statement.setString(1, lockName)
            statement.executeQuery().use { result ->
                check(result.next() && result.getBoolean(1)) { "Pair external-write fence was not held" }
            }
        }
    }

    private fun pairLockName(pairId: Long): String = "onyx:pair-external-write:$pairId"
}
