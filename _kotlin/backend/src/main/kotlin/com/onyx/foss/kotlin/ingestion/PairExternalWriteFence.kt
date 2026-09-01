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
        if (orderedPairIds.isEmpty()) return action()
        dataSource.connection.use { connection ->
            check(connection.autoCommit) { "Pair external-write fences require an auto-commit connection" }
            orderedPairIds.forEach { lock(connection, it) }
            try {
                return action()
            } finally {
                orderedPairIds.asReversed().forEach { unlock(connection, it) }
            }
        }
    }

    private fun lock(connection: Connection, pairId: Long) {
        connection.prepareStatement("SELECT pg_advisory_lock(hashtextextended(?, 0))").use { statement ->
            statement.setString(1, lockName(pairId))
            statement.execute()
        }
    }

    private fun unlock(connection: Connection, pairId: Long) {
        connection.prepareStatement("SELECT pg_advisory_unlock(hashtextextended(?, 0))").use { statement ->
            statement.setString(1, lockName(pairId))
            statement.executeQuery().use { result ->
                check(result.next() && result.getBoolean(1)) { "Pair external-write fence was not held" }
            }
        }
    }

    private fun lockName(pairId: Long): String = "onyx:pair-external-write:$pairId"
}
