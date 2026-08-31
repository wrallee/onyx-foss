package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentEntity
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.security.CredentialCipher
import com.onyx.foss.kotlin.support.PostgresIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean

class PruningServiceTest : PostgresIntegrationTest() {
    @Autowired private lateinit var pruning: PruningService
    @Autowired private lateinit var mapper: ObjectMapper
    @Autowired private lateinit var cipher: CredentialCipher
    @Autowired private lateinit var connectors: ConnectorRepository
    @Autowired private lateinit var credentials: CredentialRepository
    @Autowired private lateinit var pairs: ConnectorCredentialPairRepository
    @Autowired private lateinit var documents: IndexedDocumentRepository
    @Autowired private lateinit var jdbc: JdbcTemplate
    @MockitoBean private lateinit var indexer: OpenSearchIndexer

    @BeforeEach
    fun resetDatabase() {
        jdbc.execute(
            "TRUNCATE ingestion_errors, ingestion_jobs, ingestion_attempts, ingestion_checkpoints, " +
                "indexed_documents, connector_credential_pairs, connectors, credentials RESTART IDENTITY CASCADE",
        )
    }

    @Test
    fun deletesDocumentsMissingFromCompleteEnumeration() {
        val pairId = createPairWithDocuments("seen", "removed")

        val removed = pruning.prune(pairId, setOf("seen"), emptySet(), fromBeginning = true, completeEnumeration = true)

        assertThat(removed).isEqualTo(1)
        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("seen")
        verify(indexer).deleteDocuments(pairId, setOf("removed"))
    }

    @Test
    fun keepsDocumentWhoseRetrievalReturnedFailure() {
        val pairId = createPairWithDocuments("seen", "failed")

        val removed = pruning.prune(
            pairId,
            seenDocumentIds = setOf("seen"),
            failedDocumentIds = setOf("failed"),
            fromBeginning = true,
            completeEnumeration = true,
        )

        assertThat(removed).isZero()
        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactlyInAnyOrder("seen", "failed")
        verifyNoInteractions(indexer)
    }

    @Test
    fun leavesDatabaseRowsWhenOpenSearchDeletionFails() {
        val pairId = createPairWithDocuments("removed")
        doThrow(IllegalStateException("OpenSearch failed")).`when`(indexer).deleteDocuments(pairId, setOf("removed"))

        assertThrows<IllegalStateException> {
            pruning.prune(pairId, emptySet(), emptySet(), fromBeginning = true, completeEnumeration = true)
        }

        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("removed")
    }

    @Test
    fun doesNotPruneAfterIncrementalCheckpointRun() {
        val pairId = createPairWithDocuments("existing")

        val removed = pruning.prune(
            pairId,
            emptySet(),
            emptySet(),
            fromBeginning = false,
            completeEnumeration = true,
        )

        assertThat(removed).isZero()
        assertThat(documents.findAll().map { it.sourceDocumentId }).containsExactly("existing")
        verifyNoInteractions(indexer)
    }

    private fun createPairWithDocuments(vararg sourceDocumentIds: String): Long {
        val connector = connectors.save(
            ConnectorEntity(
                name = "file",
                source = ConnectorSource.FILE,
                connectorSpecificConfig = mapper.createObjectNode(),
            ),
        )
        val credential = credentials.save(
            CredentialEntity(
                source = ConnectorSource.FILE,
                secretJson = cipher.encrypt(mapper.createObjectNode()),
            ),
        )
        val pair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = requireNotNull(connector.id),
                credentialId = requireNotNull(credential.id),
                name = "file pair",
            ),
        )
        sourceDocumentIds.forEach { sourceDocumentId ->
            documents.save(
                IndexedDocumentEntity(
                    ccPairId = requireNotNull(pair.id),
                    sourceDocumentId = sourceDocumentId,
                    title = sourceDocumentId,
                    contentHash = sourceDocumentId,
                    metadata = mapper.createObjectNode(),
                ),
            )
        }
        return requireNotNull(pair.id)
    }
}
