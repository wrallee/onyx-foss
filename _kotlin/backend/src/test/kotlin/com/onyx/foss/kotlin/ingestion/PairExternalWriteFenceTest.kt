package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.OpenSearchIndexMigrationLockEntity
import com.onyx.foss.kotlin.domain.OpenSearchIndexMigrationLockRepository
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`

class PairExternalWriteFenceTest {
    private val pairs = mock(ConnectorCredentialPairRepository::class.java)
    private val indexLock = mock(OpenSearchIndexMigrationLockRepository::class.java)
    private val fence = PairExternalWriteFence(pairs, indexLock)

    @Test
    fun pairLocksUseStableOrderBeforeTheAction() {
        `when`(pairs.lockById(1)).thenReturn(ConnectorCredentialPairEntity(id = 1))
        `when`(pairs.lockById(2)).thenReturn(ConnectorCredentialPairEntity(id = 2))
        var actionRan = false

        fence.withPairs(listOf(2, 1, 2)) { actionRan = true }

        assertThat(actionRan).isTrue()
        inOrder(pairs).apply {
            verify(pairs).lockById(1)
            verify(pairs).lockById(2)
        }
    }

    @Test
    fun indexLockIsTakenAndActionFailuresPropagate() {
        `when`(indexLock.lock()).thenReturn(OpenSearchIndexMigrationLockEntity())
        val failure = IllegalStateException("migration failed")

        assertThat(assertThrows<IllegalStateException> {
            fence.withOpenSearchIndex("documents") { throw failure }
        }).isSameAs(failure)
        verify(indexLock).lock()
    }
}
