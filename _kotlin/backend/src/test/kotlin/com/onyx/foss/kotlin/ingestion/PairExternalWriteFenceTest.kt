package com.onyx.foss.kotlin.ingestion

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.jdbc.datasource.AbstractDataSource
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicBoolean

class PairExternalWriteFenceTest {
    @Test
    fun partialAcquisitionFailureExplicitlyReleasesEveryAcquiredLock() {
        val dataSource = RecordingAdvisoryDataSource(
            acquireFailures = setOf("onyx:pair-external-write:2"),
        )
        val actionRan = AtomicBoolean(false)

        assertThrows<SQLException> {
            PairExternalWriteFence(dataSource).withPairs(listOf(2, 1)) { actionRan.set(true) }
        }

        assertThat(actionRan).isFalse()
        assertThat(dataSource.events).containsExactly(
            "lock:onyx:pair-external-write:1",
            "lock:onyx:pair-external-write:2",
            "unlock:onyx:pair-external-write:1",
            "close",
        )
    }

    @Test
    fun unlockFailureStillAttemptsEveryReleaseAndCombinesFailures() {
        val dataSource = RecordingAdvisoryDataSource(
            unlockFailures = setOf("onyx:pair-external-write:1", "onyx:pair-external-write:2"),
        )

        val failure = assertThrows<SQLException> {
            PairExternalWriteFence(dataSource).withPairs(listOf(1, 2)) {}
        }

        assertThat(dataSource.events).containsExactly(
            "lock:onyx:pair-external-write:1",
            "lock:onyx:pair-external-write:2",
            "unlock:onyx:pair-external-write:2",
            "unlock:onyx:pair-external-write:1",
            "close",
        )
        assertThat(failure.message).contains("onyx:pair-external-write:2")
        assertThat(failure.suppressed.map { it.message })
            .containsExactly("Unlock failed for onyx:pair-external-write:1")
    }

    @Test
    fun actionFailureReleasesTheLockBeforeClosingTheConnection() {
        val dataSource = RecordingAdvisoryDataSource()
        val actionFailure = IllegalStateException("migration failed")

        val failure = assertThrows<IllegalStateException> {
            PairExternalWriteFence(dataSource).withOpenSearchIndex("documents") { throw actionFailure }
        }

        assertThat(failure).isSameAs(actionFailure)
        assertThat(dataSource.events).containsExactly(
            "lock:onyx:opensearch-index-migration:documents",
            "unlock:onyx:opensearch-index-migration:documents",
            "close",
        )
    }
}

private class RecordingAdvisoryDataSource(
    private val acquireFailures: Set<String> = emptySet(),
    private val unlockFailures: Set<String> = emptySet(),
) : AbstractDataSource() {
    val events = mutableListOf<String>()

    override fun getConnection(): Connection = proxy(Connection::class.java) { method, arguments ->
        when (method.name) {
            "getAutoCommit" -> true
            "prepareStatement" -> statement(arguments?.get(0) as String)
            "close" -> {
                events += "close"
                Unit
            }
            "isClosed" -> false
            else -> unsupported(method)
        }
    }

    override fun getConnection(username: String?, password: String?): Connection = connection

    private fun statement(sql: String): java.sql.PreparedStatement {
        var lockName: String? = null
        return proxy(java.sql.PreparedStatement::class.java) { method, arguments ->
            when (method.name) {
                "setString" -> {
                    lockName = arguments?.get(1) as String
                    Unit
                }
                "execute" -> {
                    check(sql.contains("pg_advisory_lock"))
                    val name = requireNotNull(lockName)
                    events += "lock:$name"
                    if (name in acquireFailures) throw SQLException("Lock failed for $name")
                    true
                }
                "executeQuery" -> {
                    check(sql.contains("pg_advisory_unlock"))
                    val name = requireNotNull(lockName)
                    events += "unlock:$name"
                    if (name in unlockFailures) throw SQLException("Unlock failed for $name")
                    successfulUnlock()
                }
                "close" -> Unit
                else -> unsupported(method)
            }
        }
    }

    private fun successfulUnlock(): java.sql.ResultSet {
        var first = true
        return proxy(java.sql.ResultSet::class.java) { method, _ ->
            when (method.name) {
                "next" -> first.also { first = false }
                "getBoolean" -> true
                "close" -> Unit
                else -> unsupported(method)
            }
        }
    }
}

private fun unsupported(method: Method): Nothing =
    throw UnsupportedOperationException("Unexpected JDBC call: ${method.name}")

@Suppress("UNCHECKED_CAST")
private fun <T> proxy(
    type: Class<T>,
    handler: (Method, Array<out Any?>?) -> Any?,
): T = Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { instance, method, arguments ->
    when (method.name) {
        "toString" -> "Recording ${type.simpleName}"
        "hashCode" -> System.identityHashCode(instance)
        "equals" -> instance === arguments?.firstOrNull()
        else -> handler(method, arguments)
    }
} as T
