package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.api.ApiException
import com.onyx.foss.kotlin.api.CCPropertyUpdateRequest
import com.onyx.foss.kotlin.api.ConnectorRequest
import com.onyx.foss.kotlin.api.DeletionAttemptRequest
import com.onyx.foss.kotlin.api.PairMetadataRequest
import com.onyx.foss.kotlin.api.RunConnectorRequest
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.support.H2IntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

class AdminDeletionIntegrationTest : H2IntegrationTest() {
    @Autowired private lateinit var admin: AdminService
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @MockitoSpyBean private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @Autowired private lateinit var externalWrites: PairExternalWriteFence
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        truncateTables(
            "document_set_sync_outbox", "document_set_cc_pairs", "document_sets", "permission_sync_staging",
            "permission_sync_attempts", "ingestion_errors", "ingestion_jobs", "ingestion_attempts",
            "ingestion_checkpoints", "indexed_documents", "connector_credential_pairs", "connectors", "credentials",
        )
    }

    @Test
    fun deletingPairRejectsAssociationStatusChangesAndEnqueueWhileExternalDeleteIsPaused() {
        val fixture = createPair()
        val indexDeleted = CountDownLatch(1)
        val releaseDelete = CountDownLatch(1)
        pauseIndexDelete(fixture.pairId, indexDeleted, releaseDelete)
        val executor = Executors.newSingleThreadExecutor()
        try {
            val deletion = executor.submit {
                admin.deletePair(DeletionAttemptRequest(fixture.connectorId, fixture.credentialId))
            }
            assertThat(indexDeleted.await(10, TimeUnit.SECONDS)).isTrue()
            assertThat(pairStatus(fixture.pairId)).isEqualTo(PairStatus.DELETING)

            assertConflict {
                admin.associate(
                    fixture.connectorId,
                    fixture.credentialId,
                    PairMetadataRequest(name = "reactivated"),
                )
            }
            assertConflict { admin.setPairStatus(fixture.pairId, PairStatus.ACTIVE) }
            assertConflict { admin.enqueuePair(fixture.pairId, fromBeginning = false) }
            assertConflict { admin.enqueue(RunConnectorRequest(connectorId = fixture.connectorId)) }
            assertConflict { admin.updateConnector(fixture.connectorId, connectorRequest("late update")) }
            assertConflict { admin.renamePair(fixture.pairId, "late rename") }
            assertConflict {
                admin.updatePairProperty(
                    fixture.pairId,
                    CCPropertyUpdateRequest(name = "refresh_frequency", value = "60"),
                )
            }

            releaseDelete.countDown()
            deletion.get(10, TimeUnit.SECONDS)
        } finally {
            releaseDelete.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun deletingConnectorRejectsAnewPairAfterItsPairSnapshot() {
        val fixture = createPair()
        val secondCredential = createCredential()
        val indexDeleted = CountDownLatch(1)
        val releaseDelete = CountDownLatch(1)
        pauseIndexDelete(fixture.pairId, indexDeleted, releaseDelete)
        val executor = Executors.newSingleThreadExecutor()
        try {
            val deletion = executor.submit { admin.deleteConnector(fixture.connectorId) }
            assertThat(indexDeleted.await(10, TimeUnit.SECONDS)).isTrue()

            assertConflict {
                admin.associate(
                    fixture.connectorId,
                    secondCredential,
                    PairMetadataRequest(name = "late pair"),
                )
            }

            releaseDelete.countDown()
            deletion.get(10, TimeUnit.SECONDS)
        } finally {
            releaseDelete.countDown()
            executor.shutdownNow()
        }
    }

    @Test
    fun pairFenceRemainsHeldAfterIndexDeleteUntilDatabaseRemovalCommits() {
        val fixture = createPair()
        var deletion: Future<*>? = null
        var competingWrite: Future<*>? = null
        val executor = Executors.newFixedThreadPool(2)
        val indexDeleted = CountDownLatch(1)
        val databaseRemovalReached = CountDownLatch(1)
        val releaseDatabaseRemoval = CountDownLatch(1)
        val competingWriteEntered = CountDownLatch(1)
        try {
            pausePairDelete(fixture.pairId, databaseRemovalReached, releaseDatabaseRemoval)
            doAnswer {
                indexDeleted.countDown()
                Unit
            }.`when`(indexer).deletePair(fixture.pairId)

            deletion = executor.submit {
                admin.deletePair(DeletionAttemptRequest(fixture.connectorId, fixture.credentialId))
            }
            assertThat(indexDeleted.await(10, TimeUnit.SECONDS)).isTrue()
            assertThat(databaseRemovalReached.await(10, TimeUnit.SECONDS)).isTrue()

            competingWrite = executor.submit {
                externalWrites.withPair(fixture.pairId) { competingWriteEntered.countDown() }
            }
            assertThat(competingWriteEntered.await(300, TimeUnit.MILLISECONDS)).isFalse()

            releaseDatabaseRemoval.countDown()
            deletion.get(10, TimeUnit.SECONDS)
            val competingError = catchThrowable { competingWrite.get(10, TimeUnit.SECONDS) }
            assertThat(competingWriteEntered.count).isEqualTo(1)
            assertThat(competingError).hasCauseInstanceOf(IllegalStateException::class.java)
        } finally {
            releaseDatabaseRemoval.countDown()
            runCatching { deletion?.get(10, TimeUnit.SECONDS) }
            runCatching { competingWrite?.get(10, TimeUnit.SECONDS) }
            executor.shutdownNow()
        }
    }

    @Test
    fun connectorDeleteFailureLeavesTombstonesAndReleasesEveryPairFence() {
        val fixture = createPair()
        val secondCredential = createCredential()
        val secondPair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = fixture.connectorId,
                credentialId = secondCredential,
                name = "second",
                status = PairStatus.ACTIVE,
            ),
        )
        doAnswer { throw IllegalStateException("index unavailable") }
            .`when`(indexer).deletePair(fixture.pairId)

        val error = catchThrowable { admin.deleteConnector(fixture.connectorId) }

        assertThat(error).isInstanceOf(IllegalStateException::class.java)
        assertThat(connectors.findById(fixture.connectorId).orElseThrow().deleting).isTrue()
        assertThat(pairs.findAllByConnectorId(fixture.connectorId).map { it.status })
            .containsOnly(PairStatus.DELETING)
        var entered = false
        externalWrites.withPairs(listOf(fixture.pairId, requireNotNull(secondPair.id))) { entered = true }
        assertThat(entered).isTrue()
    }

    @Test
    fun staleConnectorUpdateCannotEraseATombstoneAfterDeletionStarts() {
        val fixture = createPair()
        val updateEntered = CountDownLatch(1)
        val releaseUpdate = CountDownLatch(1)
        val indexDeleted = CountDownLatch(1)
        val releaseDelete = CountDownLatch(1)
        val request = blockingConnectorRequest(updateEntered, releaseUpdate)
        pauseIndexDelete(fixture.pairId, indexDeleted, releaseDelete)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val update = executor.submit { admin.updateConnector(fixture.connectorId, request) }
            assertThat(updateEntered.await(10, TimeUnit.SECONDS)).isTrue()
            val deletion = executor.submit { admin.deleteConnector(fixture.connectorId) }
            val deletionStartedBeforeUpdateReleased = indexDeleted.await(300, TimeUnit.MILLISECONDS)

            releaseUpdate.countDown()
            update.get(10, TimeUnit.SECONDS)
            assertThat(indexDeleted.await(10, TimeUnit.SECONDS)).isTrue()
            val tombstone = connectors.findById(fixture.connectorId).orElseThrow().deleting

            releaseDelete.countDown()
            deletion.get(10, TimeUnit.SECONDS)
            assertThat(deletionStartedBeforeUpdateReleased).isFalse()
            assertThat(tombstone).isTrue()
        } finally {
            releaseUpdate.countDown()
            releaseDelete.countDown()
            executor.shutdownNow()
        }
    }

    private fun createPair(): Fixture {
        val connector = connectors.save(
            ConnectorEntity(
                name = "file",
                source = ConnectorSource.FILE,
                connectorSpecificConfig = mapper.createObjectNode(),
            ),
        )
        val credentialId = createCredential()
        val pair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = requireNotNull(connector.id),
                credentialId = credentialId,
                name = "pair",
                status = PairStatus.ACTIVE,
            ),
        )
        return Fixture(requireNotNull(connector.id), credentialId, requireNotNull(pair.id))
    }

    private fun createCredential(): Long = requireNotNull(
        credentials.save(
            CredentialEntity(
                source = ConnectorSource.FILE,
                secretJson = cipher.encrypt(mapper.createObjectNode()),
            ),
        ).id,
    )

    private fun pauseIndexDelete(pairId: Long, started: CountDownLatch, release: CountDownLatch) {
        doAnswer {
            started.countDown()
            check(release.await(10, TimeUnit.SECONDS))
            Unit
        }.`when`(indexer).deletePair(pairId)
    }

    private fun connectorRequest(name: String): ConnectorRequest = ConnectorRequest(
        name = name,
        source = ConnectorSource.FILE,
        connectorSpecificConfig = mapper.createObjectNode(),
    )

    private fun blockingConnectorRequest(entered: CountDownLatch, release: CountDownLatch): ConnectorRequest {
        val request = mock(ConnectorRequest::class.java)
        doAnswer {
            entered.countDown()
            check(release.await(10, TimeUnit.SECONDS))
            "updated"
        }.`when`(request).name
        doReturn(ConnectorSource.FILE).`when`(request).source
        doReturn("load_state").`when`(request).inputType
        doReturn(mapper.createObjectNode()).`when`(request).connectorSpecificConfig
        return request
    }

    private fun pairStatus(pairId: Long): PairStatus = PairStatus.valueOf(
        requireNotNull(
            jdbc.queryForObject(
                "SELECT status FROM connector_credential_pairs WHERE id = ?",
                String::class.java,
                pairId,
            ),
        ),
    )

    private fun assertConflict(action: () -> Unit) {
        val error = catchThrowable(action)
        assertThat(error).isInstanceOf(ApiException::class.java)
        assertThat((error as ApiException).status).isEqualTo(HttpStatus.CONFLICT)
    }

    private fun pausePairDelete(pairId: Long, reached: CountDownLatch, release: CountDownLatch) {
        doAnswer {
            reached.countDown()
            check(release.await(10, TimeUnit.SECONDS))
            jdbc.update("DELETE FROM connector_credential_pairs WHERE id = ?", pairId)
            Unit
        }.`when`(pairs).delete(any(ConnectorCredentialPairEntity::class.java))
    }

    private data class Fixture(val connectorId: Long, val credentialId: Long, val pairId: Long)

}
