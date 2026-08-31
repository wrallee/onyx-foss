package com.onyx.foss.kotlin.domain

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import java.time.Instant

interface CredentialRepository : JpaRepository<CredentialEntity, Long> {
    fun findAllBySource(source: ConnectorSource): List<CredentialEntity>
}

interface ConnectorRepository : JpaRepository<ConnectorEntity, Long>

interface ConnectorCredentialPairRepository : JpaRepository<ConnectorCredentialPairEntity, Long> {
    fun findAllByConnectorId(connectorId: Long): List<ConnectorCredentialPairEntity>
    fun findAllByCredentialId(credentialId: Long): List<ConnectorCredentialPairEntity>
    fun findByConnectorIdAndCredentialId(connectorId: Long, credentialId: Long): ConnectorCredentialPairEntity?
}

interface DocumentSetRepository : JpaRepository<DocumentSetEntity, Long> {
    fun existsByName(name: String): Boolean
    fun existsByNameAndIdNot(name: String, id: Long): Boolean
}

interface FileAssetRepository : JpaRepository<FileAssetEntity, String>

interface IngestionAttemptRepository : JpaRepository<IngestionAttemptEntity, Long> {
    fun findAllByCcPairIdOrderByIdDesc(ccPairId: Long): List<IngestionAttemptEntity>
    fun findFirstByCcPairIdOrderByIdDesc(ccPairId: Long): IngestionAttemptEntity?
}

interface IngestionCheckpointRepository : JpaRepository<IngestionCheckpointEntity, Long>

interface IngestionJobRepository : JpaRepository<IngestionJobEntity, Long> {
    @Query(
        value = """
            SELECT * FROM ingestion_jobs
            WHERE state = 'QUEUED' AND run_after <= :now
            ORDER BY run_after, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockNextQueued(@Param("now") now: Instant): IngestionJobEntity?
}

interface IndexedDocumentRepository : JpaRepository<IndexedDocumentEntity, Long> {
    fun findByCcPairIdAndSourceDocumentId(ccPairId: Long, sourceDocumentId: String): IndexedDocumentEntity?
    fun countByCcPairId(ccPairId: Long): Long
    fun deleteAllByCcPairId(ccPairId: Long)
}

interface IngestionErrorRepository : JpaRepository<IngestionErrorEntity, Long> {
    fun findAllByAttemptIdOrderByIdDesc(attemptId: Long): List<IngestionErrorEntity>
    fun findAllByAttemptIdInOrderByIdDesc(attemptIds: Collection<Long>): List<IngestionErrorEntity>
}
