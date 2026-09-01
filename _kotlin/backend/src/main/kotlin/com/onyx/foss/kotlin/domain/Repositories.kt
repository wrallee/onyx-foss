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
import java.util.UUID

interface CredentialRepository : JpaRepository<CredentialEntity, Long> {
    fun findAllBySource(source: ConnectorSource): List<CredentialEntity>
}

interface ConnectorRepository : JpaRepository<ConnectorEntity, Long> {
    @Query(value = "SELECT * FROM connectors WHERE id = :id FOR UPDATE", nativeQuery = true)
    fun lockById(@Param("id") id: Long): ConnectorEntity?
}

interface ConnectorCredentialPairRepository : JpaRepository<ConnectorCredentialPairEntity, Long> {
    fun findAllByConnectorId(connectorId: Long): List<ConnectorCredentialPairEntity>
    fun findAllByCredentialId(credentialId: Long): List<ConnectorCredentialPairEntity>
    fun findByConnectorIdAndCredentialId(connectorId: Long, credentialId: Long): ConnectorCredentialPairEntity?

    @Query(value = "SELECT * FROM connector_credential_pairs WHERE id = :id FOR UPDATE", nativeQuery = true)
    fun lockById(@Param("id") id: Long): ConnectorCredentialPairEntity?

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

    @Query(
        value = """
            SELECT document_set.name
            FROM document_sets document_set
            JOIN document_set_cc_pairs membership ON membership.document_set_id = document_set.id
            WHERE membership.cc_pair_id = :ccPairId
            ORDER BY document_set.name
        """,
        nativeQuery = true,
    )
    fun findNamesByCcPairId(@Param("ccPairId") ccPairId: Long): List<String>
}

interface DocumentSetSyncOutboxRepository : JpaRepository<DocumentSetSyncOutboxEntity, Long> {
    @Query(
        value = """
            SELECT EXISTS (
                SELECT 1
                FROM document_set_sync_outbox
                WHERE status IN ('PENDING', 'IN_PROGRESS')
                  AND (
                      document_set_ids IS NULL
                      OR document_set_ids @> jsonb_build_array(CAST(:documentSetId AS BIGINT))
                  )
            )
        """,
        nativeQuery = true,
    )
    fun existsActiveForDocumentSet(@Param("documentSetId") documentSetId: Long): Boolean

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

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        value = """
            UPDATE document_set_sync_outbox
            SET locked_at = :now,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
              AND claim_token = :token
              AND status = 'IN_PROGRESS'
        """,
        nativeQuery = true,
    )
    fun renewOwned(
        @Param("id") id: Long,
        @Param("token") token: UUID,
        @Param("now") now: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        value = """
            UPDATE document_set_sync_outbox
            SET status = 'DONE',
                claim_token = NULL,
                locked_at = NULL,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
              AND claim_token = :token
              AND status = 'IN_PROGRESS'
        """,
        nativeQuery = true,
    )
    fun completeOwned(@Param("id") id: Long, @Param("token") token: UUID): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        value = """
            UPDATE document_set_sync_outbox
            SET status = 'PENDING',
                claim_token = NULL,
                locked_at = NULL,
                last_error = :message,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
              AND claim_token = :token
              AND status = 'IN_PROGRESS'
        """,
        nativeQuery = true,
    )
    fun retryOwned(
        @Param("id") id: Long,
        @Param("token") token: UUID,
        @Param("message") message: String,
    ): Int
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
    fun findFirstByCcPairIdAndStateInOrderById(
        ccPairId: Long,
        states: Collection<JobState>,
    ): IngestionJobEntity?

    @Query(
        value = """
            SELECT job.*
            FROM ingestion_jobs job
            JOIN connector_credential_pairs pair ON pair.id = job.cc_pair_id
            WHERE pair.status <> 'DELETING'
              AND (pair.ingestion_lease_expires_at IS NULL OR pair.ingestion_lease_expires_at < :now)
              AND (
                  (job.state = 'QUEUED' AND job.run_after <= :now)
                  OR (job.state = 'RUNNING' AND (job.lease_expires_at IS NULL OR job.lease_expires_at < :now))
              )
            ORDER BY job.run_after, job.id
            FOR UPDATE OF job SKIP LOCKED
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockNextClaimable(@Param("now") now: Instant): IngestionJobEntity?

    @Query(
        value = """
            SELECT * FROM ingestion_jobs
            WHERE id = :id
              AND (
                  (state = 'QUEUED' AND run_after <= :now)
                  OR (state = 'RUNNING' AND (lease_expires_at IS NULL OR lease_expires_at < :now))
              )
            FOR UPDATE
        """,
        nativeQuery = true,
    )
    fun lockClaimableById(@Param("id") id: Long, @Param("now") now: Instant): IngestionJobEntity?

    @Query(value = "SELECT * FROM ingestion_jobs WHERE id = :id FOR UPDATE", nativeQuery = true)
    fun lockById(@Param("id") id: Long): IngestionJobEntity?
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
    fun countByCcPairId(ccPairId: Long): Long
    fun deleteAllByCcPairId(ccPairId: Long)
    @Transactional
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
    fun findFirstByCcPairIdAndStatusInOrderByTimeFinishedDescIdDesc(
        ccPairId: Long,
        statuses: Collection<AttemptStatus>,
    ): PermissionSyncAttemptEntity?

    @Query(
        value = """
            SELECT * FROM permission_sync_attempts
            WHERE status = 'NOT_STARTED'
               OR (status = 'IN_PROGRESS' AND (lease_expires_at IS NULL OR lease_expires_at <= :now))
            ORDER BY created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockNextClaimable(@Param("now") now: Instant): PermissionSyncAttemptEntity?

    @Query(
        value = """
            SELECT * FROM permission_sync_attempts
            WHERE cc_pair_id = :ccPairId
              AND (
                  status = 'NOT_STARTED'
                  OR (status = 'IN_PROGRESS' AND (lease_expires_at IS NULL OR lease_expires_at <= :now))
              )
            ORDER BY created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """,
        nativeQuery = true,
    )
    fun lockClaimableForPair(
        @Param("ccPairId") ccPairId: Long,
        @Param("now") now: Instant,
    ): PermissionSyncAttemptEntity?

    @Query(
        value = """
            SELECT * FROM permission_sync_attempts
            WHERE id = :id
              AND claim_token = :token
              AND status = 'IN_PROGRESS'
            FOR UPDATE
        """,
        nativeQuery = true,
    )
    fun lockOwned(@Param("id") id: Long, @Param("token") token: UUID): PermissionSyncAttemptEntity?

    @Transactional
    @Modifying
    @Query(
        value = """
            INSERT INTO permission_sync_attempts (cc_pair_id, status)
            VALUES (:ccPairId, 'NOT_STARTED')
            ON CONFLICT (cc_pair_id) WHERE status IN ('NOT_STARTED', 'IN_PROGRESS') DO UPDATE
            SET follow_up_requested = permission_sync_attempts.follow_up_requested
                    OR permission_sync_attempts.status = 'IN_PROGRESS',
                updated_at = CURRENT_TIMESTAMP
        """,
        nativeQuery = true,
    )
    fun createOrCoalesce(@Param("ccPairId") ccPairId: Long): Int
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

@Repository
class IngestionEnumerationRepository(
    private val jdbc: JdbcTemplate,
) {
    @Transactional
    fun registerDocuments(attemptId: Long, sourceDocumentIds: Collection<String>): Set<String> {
        val ids = sourceDocumentIds.filter(String::isNotBlank).distinct()
        jdbc.batchUpdate(
            """
                INSERT INTO ingestion_enumerated_documents(attempt_id, source_document_id)
                VALUES (?, ?)
                ON CONFLICT DO NOTHING
            """.trimIndent(),
            ids,
            ENUMERATION_PAGE_SIZE,
        ) { statement, sourceDocumentId ->
            statement.setLong(1, attemptId)
            statement.setString(2, sourceDocumentId)
        }
        return ids.chunked(ENUMERATION_PAGE_SIZE).flatMapTo(mutableSetOf()) { page ->
            val placeholders = List(page.size) { "?" }.joinToString()
            val parameters: Array<Any> = arrayOf(attemptId, *page.toTypedArray())
            jdbc.queryForList(
                """
                    SELECT source_document_id
                    FROM ingestion_enumerated_documents
                    WHERE attempt_id = ?
                      AND processed = FALSE
                      AND source_document_id IN ($placeholders)
                """.trimIndent(),
                String::class.java,
                *parameters,
            )
        }
    }

    @Transactional
    fun markProcessed(attemptId: Long, sourceDocumentId: String) {
        check(
            jdbc.update(
                """
                    UPDATE ingestion_enumerated_documents
                    SET processed = TRUE
                    WHERE attempt_id = ? AND source_document_id = ?
                """.trimIndent(),
                attemptId,
                sourceDocumentId,
            ) == 1,
        ) { "Enumerated document disappeared before processing completed" }
    }

    @Transactional
    fun protectFailures(attemptId: Long, sourceDocumentIds: Collection<String>) {
        val ids = sourceDocumentIds.filter(String::isNotBlank).distinct()
        jdbc.batchUpdate(
            """
                INSERT INTO ingestion_enumerated_documents(attempt_id, source_document_id)
                VALUES (?, ?)
                ON CONFLICT DO NOTHING
            """.trimIndent(),
            ids,
            ENUMERATION_PAGE_SIZE,
        ) { statement, sourceDocumentId ->
            statement.setLong(1, attemptId)
            statement.setString(2, sourceDocumentId)
        }
    }

    fun findMissingPage(pairId: Long, attemptId: Long, afterSourceDocumentId: String, limit: Int): List<String> =
        jdbc.queryForList(
            """
                SELECT document.source_document_id
                FROM indexed_documents document
                WHERE document.cc_pair_id = ?
                  AND document.source_document_id > ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ingestion_enumerated_documents enumerated
                      WHERE enumerated.attempt_id = ?
                        AND enumerated.source_document_id = document.source_document_id
                  )
                ORDER BY document.source_document_id
                LIMIT ?
            """.trimIndent(),
            String::class.java,
            pairId,
            afterSourceDocumentId,
            attemptId,
            limit,
        )

    private companion object {
        const val ENUMERATION_PAGE_SIZE = 500
    }
}
