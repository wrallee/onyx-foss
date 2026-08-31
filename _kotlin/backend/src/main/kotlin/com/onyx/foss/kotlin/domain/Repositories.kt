package com.onyx.foss.kotlin.domain

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

interface CredentialRepository : JpaRepository<CredentialEntity, Long> {
    fun findAllBySource(source: ConnectorSource): List<CredentialEntity>
}

interface ConnectorRepository : JpaRepository<ConnectorEntity, Long>

interface ConnectorCredentialPairRepository : JpaRepository<ConnectorCredentialPairEntity, Long> {
    fun findAllByConnectorId(connectorId: Long): List<ConnectorCredentialPairEntity>
    fun findAllByCredentialId(credentialId: Long): List<ConnectorCredentialPairEntity>
    fun findByConnectorIdAndCredentialId(connectorId: Long, credentialId: Long): ConnectorCredentialPairEntity?

    @Query(
        value = """
            SELECT pair.id AS "pairId",
                   connector.refresh_freq AS "refreshFreq",
                   connector.prune_freq AS "pruneFreq",
                   pair.last_pruned_at AS "lastPrunedAt",
                   (
                       SELECT MAX(attempt.time_updated)
                       FROM ingestion_attempts attempt
                       WHERE attempt.cc_pair_id = pair.id
                         AND attempt.prune_only = FALSE
                   ) AS "lastAttemptAt"
            FROM connector_credential_pairs pair
            JOIN connectors connector ON connector.id = pair.connector_id
            WHERE pair.status IN ('SCHEDULED', 'INITIAL_INDEXING', 'ACTIVE')
              AND NOT EXISTS (
                  SELECT 1
                  FROM ingestion_attempts active_attempt
                  JOIN ingestion_jobs active_job ON active_job.attempt_id = active_attempt.id
                  WHERE active_attempt.cc_pair_id = pair.id
                    AND active_job.state IN ('QUEUED', 'RUNNING')
              )
            FOR UPDATE OF pair SKIP LOCKED
        """,
        nativeQuery = true,
    )
    fun findSchedulable(): List<IngestionScheduleCandidate>
}

interface IngestionScheduleCandidate {
    val pairId: Long
    val refreshFreq: Long?
    val pruneFreq: Long?
    val lastPrunedAt: Instant?
    val lastAttemptAt: Instant?
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
    fun findAllByCcPairId(ccPairId: Long): List<IndexedDocumentEntity>
    @Query("SELECT document.sourceDocumentId FROM IndexedDocumentEntity document WHERE document.ccPairId = :ccPairId")
    fun findSourceIdsByCcPairId(@Param("ccPairId") ccPairId: Long): List<String>
    fun countByCcPairId(ccPairId: Long): Long
    fun deleteAllByCcPairId(ccPairId: Long)
    fun deleteByCcPairIdAndSourceDocumentIdIn(ccPairId: Long, sourceDocumentIds: Collection<String>): Long
}

interface IngestionErrorRepository : JpaRepository<IngestionErrorEntity, Long> {
    fun findAllByAttemptIdOrderByIdDesc(attemptId: Long): List<IngestionErrorEntity>
    fun findAllByAttemptIdInOrderByIdDesc(attemptIds: Collection<Long>): List<IngestionErrorEntity>
    @Query(
        """
            SELECT error FROM IngestionErrorEntity error, IngestionAttemptEntity attempt
            WHERE error.attemptId = attempt.id
              AND attempt.ccPairId = :ccPairId
              AND error.sourceDocumentId = :sourceDocumentId
              AND error.isResolved = false
            ORDER BY error.id DESC
        """,
    )
    fun findUnresolvedByCcPairIdAndSourceDocumentId(
        @Param("ccPairId") ccPairId: Long,
        @Param("sourceDocumentId") sourceDocumentId: String,
    ): List<IngestionErrorEntity>

    @Query(
        """
            SELECT error FROM IngestionErrorEntity error, IngestionAttemptEntity attempt
            WHERE error.attemptId = attempt.id
              AND attempt.ccPairId = :ccPairId
              AND error.entityId IS NOT NULL
              AND error.isResolved = false
            ORDER BY error.id DESC
        """,
    )
    fun findUnresolvedEntityErrorsByCcPairId(@Param("ccPairId") ccPairId: Long): List<IngestionErrorEntity>
}

interface PermissionSyncAttemptRepository : JpaRepository<PermissionSyncAttemptEntity, Long> {
    fun findAllByCcPairIdOrderByIdDesc(ccPairId: Long): List<PermissionSyncAttemptEntity>
    fun findFirstByCcPairIdOrderByIdDesc(ccPairId: Long): PermissionSyncAttemptEntity?

    @Transactional
    @Modifying
    @Query(
        value = """
            INSERT INTO permission_sync_attempts (cc_pair_id, status)
            VALUES (:ccPairId, 'NOT_STARTED')
            ON CONFLICT DO NOTHING
        """,
        nativeQuery = true,
    )
    fun createIfNoActive(@Param("ccPairId") ccPairId: Long): Int
}
