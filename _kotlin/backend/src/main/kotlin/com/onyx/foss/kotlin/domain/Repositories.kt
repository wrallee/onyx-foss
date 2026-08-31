package com.onyx.foss.kotlin.domain

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.sql.Types
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

interface DocumentSetSyncOutboxRepository : JpaRepository<DocumentSetSyncOutboxEntity, Long> {
    @Query(
        value = """
            SELECT * FROM document_set_sync_outbox
            WHERE status = 'IN_PROGRESS'
            ORDER BY id
            FOR UPDATE
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockOldestInProgress(): DocumentSetSyncOutboxEntity?

    @Query(
        value = """
            SELECT * FROM document_set_sync_outbox
            WHERE status = 'PENDING'
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockNextPending(): DocumentSetSyncOutboxEntity?
}

interface FileAssetRepository : JpaRepository<FileAssetEntity, String>

interface IngestionAttemptRepository : JpaRepository<IngestionAttemptEntity, Long> {
    fun findAllByCcPairIdOrderByIdDesc(ccPairId: Long): List<IngestionAttemptEntity>
    fun findFirstByCcPairIdOrderByIdDesc(ccPairId: Long): IngestionAttemptEntity?
    fun findFirstByCcPairIdAndStatusInOrderByTimeStartedDescIdDesc(
        ccPairId: Long,
        statuses: Collection<AttemptStatus>,
    ): IngestionAttemptEntity?
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
    fun findAllByCcPairIdAndSourceDocumentIdIn(
        ccPairId: Long,
        sourceDocumentIds: Collection<String>,
    ): List<IndexedDocumentEntity>
    @Query(
        value = """
            SELECT * FROM indexed_documents
            WHERE cc_pair_id = :ccPairId
              AND source_document_id > :afterSourceDocumentId
            ORDER BY source_document_id
            LIMIT :limit
        """,
        nativeQuery = true,
    )
    fun findPageByCcPairId(
        @Param("ccPairId") ccPairId: Long,
        @Param("afterSourceDocumentId") afterSourceDocumentId: String,
        @Param("limit") limit: Int,
    ): List<IndexedDocumentEntity>
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
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        value = """
            UPDATE permission_sync_attempts
            SET status = 'FAILED',
                error_msg = :message,
                full_exception_trace = :trace,
                time_finished = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE cc_pair_id = :ccPairId
              AND status IN ('NOT_STARTED', 'IN_PROGRESS')
              AND COALESCE(time_started, created_at) < :cutoff
        """,
        nativeQuery = true,
    )
    fun failStaleActive(
        @Param("ccPairId") ccPairId: Long,
        @Param("cutoff") cutoff: Instant,
        @Param("message") message: String,
        @Param("trace") trace: String,
    ): Int

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

data class PermissionSyncStageRow(
    val sourceDocumentId: String,
    val externalAccess: JsonNode,
    val hasError: Boolean,
)

@Repository
class PermissionSyncStageRepository(
    private val jdbc: JdbcTemplate,
    private val mapper: ObjectMapper,
) {
    @Transactional
    fun upsert(attemptId: Long, rows: Collection<PermissionSyncStageRow>) {
        if (rows.isEmpty()) return
        jdbc.batchUpdate(
            """
                INSERT INTO permission_sync_staging
                    (attempt_id, source_document_id, external_access, has_error)
                VALUES (?, ?, ?::jsonb, ?)
                ON CONFLICT (attempt_id, source_document_id) DO UPDATE
                SET external_access = CASE
                        WHEN permission_sync_staging.has_error THEN permission_sync_staging.external_access
                        ELSE EXCLUDED.external_access
                    END,
                    has_error = permission_sync_staging.has_error OR EXCLUDED.has_error
            """.trimIndent(),
            rows,
            rows.size,
        ) { statement, row ->
            statement.setLong(1, attemptId)
            statement.setString(2, row.sourceDocumentId)
            statement.setObject(3, mapper.writeValueAsString(row.externalAccess), Types.OTHER)
            statement.setBoolean(4, row.hasError)
        }
    }

    fun findPage(attemptId: Long, afterSourceDocumentId: String, limit: Int): List<PermissionSyncStageRow> =
        jdbc.query(
            """
                SELECT source_document_id, external_access, has_error
                FROM permission_sync_staging
                WHERE attempt_id = ? AND source_document_id > ?
                ORDER BY source_document_id
                LIMIT ?
            """.trimIndent(),
            { result, _ ->
                PermissionSyncStageRow(
                    sourceDocumentId = result.getString("source_document_id"),
                    externalAccess = mapper.readTree(result.getString("external_access")),
                    hasError = result.getBoolean("has_error"),
                )
            },
            attemptId,
            afterSourceDocumentId,
            limit,
        )

    fun countForAttempt(attemptId: Long): Long = jdbc.queryForObject(
        "SELECT COUNT(*) FROM permission_sync_staging WHERE attempt_id = ?",
        Long::class.java,
        attemptId,
    ) ?: 0

    fun countErrorsForAttempt(attemptId: Long): Long = jdbc.queryForObject(
        "SELECT COUNT(*) FROM permission_sync_staging WHERE attempt_id = ? AND has_error = TRUE",
        Long::class.java,
        attemptId,
    ) ?: 0

    @Transactional
    fun deleteAllForAttempt(attemptId: Long) {
        jdbc.update("DELETE FROM permission_sync_staging WHERE attempt_id = ?", attemptId)
    }

    @Transactional
    fun deleteTerminalForPair(pairId: Long) {
        jdbc.update(
            """
                DELETE FROM permission_sync_staging staging
                USING permission_sync_attempts attempt
                WHERE staging.attempt_id = attempt.id
                  AND attempt.cc_pair_id = ?
                  AND attempt.status IN ('SUCCESS', 'FAILED', 'COMPLETED_WITH_ERRORS')
            """.trimIndent(),
            pairId,
        )
    }
}
