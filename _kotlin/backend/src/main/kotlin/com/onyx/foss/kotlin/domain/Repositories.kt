package com.onyx.foss.kotlin.domain

import com.fasterxml.jackson.databind.JsonNode
import jakarta.persistence.LockModeType
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

interface CredentialRepository : JpaRepository<CredentialEntity, Long> {
    fun findAllBySource(source: ConnectorSource): List<CredentialEntity>
}

interface ConnectorRepository : JpaRepository<ConnectorEntity, Long> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT connector FROM ConnectorEntity connector WHERE connector.id = :id")
    fun lockById(@Param("id") id: Long): ConnectorEntity?
}

interface ConnectorCredentialPairRepository : JpaRepository<ConnectorCredentialPairEntity, Long> {
    fun findAllByConnectorId(connectorId: Long): List<ConnectorCredentialPairEntity>
    fun findAllByCredentialId(credentialId: Long): List<ConnectorCredentialPairEntity>
    fun findByConnectorIdAndCredentialId(connectorId: Long, credentialId: Long): ConnectorCredentialPairEntity?

    @Query("SELECT pair.connectorId FROM ConnectorCredentialPairEntity pair WHERE pair.id = :id")
    fun findConnectorIdById(@Param("id") id: Long): Long?

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT pair FROM ConnectorCredentialPairEntity pair WHERE pair.id = :id")
    fun lockById(@Param("id") id: Long): ConnectorCredentialPairEntity?

    @Query(
        """
            SELECT pair FROM ConnectorCredentialPairEntity pair
            WHERE pair.status IN :statuses
              AND NOT EXISTS (
                  SELECT job.id FROM IngestionJobEntity job
                  WHERE job.ccPairId = pair.id AND job.state IN :activeStates
              )
        """,
    )
    fun findSchedulable(
        @Param("statuses") statuses: Collection<PairStatus>,
        @Param("activeStates") activeStates: Collection<JobState>,
    ): List<ConnectorCredentialPairEntity>
}

interface DocumentSetRepository : JpaRepository<DocumentSetEntity, Long> {
    fun existsByName(name: String): Boolean
    fun existsByNameAndIdNot(name: String, id: Long): Boolean
    fun findAllByNameIn(names: Collection<String>): List<DocumentSetEntity>

    @Query(
        """
            SELECT documentSet.name
            FROM DocumentSetEntity documentSet, DocumentSetPairEntity membership
            WHERE membership.documentSetId = documentSet.id
              AND membership.ccPairId = :ccPairId
            ORDER BY documentSet.name
        """,
    )
    fun findNamesByCcPairId(@Param("ccPairId") ccPairId: Long): List<String>
}

interface DocumentSetPairRepository : JpaRepository<DocumentSetPairEntity, DocumentSetPairId> {
    fun findAllByDocumentSetIdOrderByCcPairId(documentSetId: Long): List<DocumentSetPairEntity>
    fun deleteAllByDocumentSetId(documentSetId: Long)
    fun deleteAllByCcPairId(ccPairId: Long)
}

interface DocumentSetSyncOutboxRepository : JpaRepository<DocumentSetSyncOutboxEntity, Long> {
    fun findAllByStatusIn(statuses: Collection<DocumentSetSyncStatus>): List<DocumentSetSyncOutboxEntity>

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    fun findFirstByStatusOrderById(status: DocumentSetSyncStatus): DocumentSetSyncOutboxEntity?

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
            UPDATE DocumentSetSyncOutboxEntity row
            SET row.lockedAt = :now
            WHERE row.id = :id
              AND row.claimToken = :token
              AND row.status = com.onyx.foss.kotlin.domain.DocumentSetSyncStatus.IN_PROGRESS
        """,
    )
    fun renewOwned(
        @Param("id") id: Long,
        @Param("token") token: UUID,
        @Param("now") now: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
            UPDATE DocumentSetSyncOutboxEntity row
            SET row.status = com.onyx.foss.kotlin.domain.DocumentSetSyncStatus.DONE,
                row.claimToken = NULL,
                row.lockedAt = NULL,
                row.lastError = NULL
            WHERE row.id = :id
              AND row.claimToken = :token
              AND row.status = com.onyx.foss.kotlin.domain.DocumentSetSyncStatus.IN_PROGRESS
        """,
    )
    fun completeOwned(@Param("id") id: Long, @Param("token") token: UUID): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
            UPDATE DocumentSetSyncOutboxEntity row
            SET row.status = com.onyx.foss.kotlin.domain.DocumentSetSyncStatus.PENDING,
                row.claimToken = NULL,
                row.lockedAt = NULL,
                row.lastError = :message
            WHERE row.id = :id
              AND row.claimToken = :token
              AND row.status = com.onyx.foss.kotlin.domain.DocumentSetSyncStatus.IN_PROGRESS
        """,
    )
    fun retryOwned(
        @Param("id") id: Long,
        @Param("token") token: UUID,
        @Param("message") message: String,
    ): Int
}

interface DocumentSetSyncClaimLockRepository : JpaRepository<DocumentSetSyncClaimLockEntity, Short> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT lock FROM DocumentSetSyncClaimLockEntity lock WHERE lock.id = 1")
    fun lock(): DocumentSetSyncClaimLockEntity
}

interface OpenSearchIndexMigrationLockRepository : JpaRepository<OpenSearchIndexMigrationLockEntity, Short> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT lock FROM OpenSearchIndexMigrationLockEntity lock WHERE lock.id = 1")
    fun lock(): OpenSearchIndexMigrationLockEntity
}

interface FileAssetRepository : JpaRepository<FileAssetEntity, String>

interface IngestionAttemptRepository : JpaRepository<IngestionAttemptEntity, Long> {
    fun findAllByCcPairIdOrderByIdDesc(ccPairId: Long): List<IngestionAttemptEntity>
    fun findFirstByCcPairIdOrderByIdDesc(ccPairId: Long): IngestionAttemptEntity?
    fun findFirstByCcPairIdAndStatusInOrderByTimeStartedDescIdDesc(
        ccPairId: Long,
        statuses: Collection<AttemptStatus>,
    ): IngestionAttemptEntity?
    fun findFirstByCcPairIdAndPruneOnlyFalseOrderByTimeUpdatedDescIdDesc(ccPairId: Long): IngestionAttemptEntity?
}

interface IngestionCheckpointRepository : JpaRepository<IngestionCheckpointEntity, Long>

interface IngestionJobRepository : JpaRepository<IngestionJobEntity, Long> {
    fun findFirstByCcPairIdAndStateInOrderById(
        ccPairId: Long,
        states: Collection<JobState>,
    ): IngestionJobEntity?

    @Query(
        """
            SELECT job.id
            FROM IngestionJobEntity job, ConnectorCredentialPairEntity pair
            WHERE pair.id = job.ccPairId
              AND pair.status <> com.onyx.foss.kotlin.domain.PairStatus.DELETING
              AND (pair.ingestionLeaseExpiresAt IS NULL OR pair.ingestionLeaseExpiresAt < :now)
              AND (
                  (job.state = com.onyx.foss.kotlin.domain.JobState.QUEUED AND job.runAfter <= :now)
                  OR (job.state = com.onyx.foss.kotlin.domain.JobState.RUNNING
                      AND (job.leaseExpiresAt IS NULL OR job.leaseExpiresAt < :now))
              )
            ORDER BY job.runAfter, job.id
        """,
    )
    fun findClaimableIds(@Param("now") now: Instant, pageable: Pageable): List<Long>

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
            UPDATE IngestionJobEntity job
            SET job.state = com.onyx.foss.kotlin.domain.JobState.RUNNING,
                job.lockedAt = :now,
                job.lockedBy = :worker,
                job.attempts = job.attempts + 1,
                job.claimToken = :token,
                job.leaseExpiresAt = :leaseExpiresAt
            WHERE job.id = :id
              AND (
                  (job.state = com.onyx.foss.kotlin.domain.JobState.QUEUED AND job.runAfter <= :now)
                  OR (job.state = com.onyx.foss.kotlin.domain.JobState.RUNNING
                      AND (job.leaseExpiresAt IS NULL OR job.leaseExpiresAt < :now))
              )
        """,
    )
    fun claim(
        @Param("id") id: Long,
        @Param("now") now: Instant,
        @Param("worker") worker: String,
        @Param("token") token: UUID,
        @Param("leaseExpiresAt") leaseExpiresAt: Instant,
    ): Int

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT job FROM IngestionJobEntity job WHERE job.id = :id")
    fun lockById(@Param("id") id: Long): IngestionJobEntity?
}

interface IndexedDocumentRepository : JpaRepository<IndexedDocumentEntity, Long> {
    fun findByCcPairIdAndSourceDocumentId(ccPairId: Long, sourceDocumentId: String): IndexedDocumentEntity?
    fun findAllByCcPairId(ccPairId: Long): List<IndexedDocumentEntity>
    fun findAllByCcPairIdAndSourceDocumentIdIn(
        ccPairId: Long,
        sourceDocumentIds: Collection<String>,
    ): List<IndexedDocumentEntity>
    fun findAllByCcPairIdAndSourceDocumentIdGreaterThanOrderBySourceDocumentId(
        ccPairId: Long,
        afterSourceDocumentId: String,
        pageable: Pageable,
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
    fun findFirstByCcPairIdAndActiveMarkerOrderById(ccPairId: Long, activeMarker: Short): PermissionSyncAttemptEntity?

    @Query(
        """
            SELECT attempt.id FROM PermissionSyncAttemptEntity attempt
            WHERE attempt.status = com.onyx.foss.kotlin.domain.AttemptStatus.NOT_STARTED
               OR (attempt.status = com.onyx.foss.kotlin.domain.AttemptStatus.IN_PROGRESS
                   AND (attempt.leaseExpiresAt IS NULL OR attempt.leaseExpiresAt <= :now))
            ORDER BY attempt.createdAt, attempt.id
        """,
    )
    fun findClaimableIds(@Param("now") now: Instant, pageable: Pageable): List<Long>

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT attempt FROM PermissionSyncAttemptEntity attempt WHERE attempt.id = :id")
    fun lockById(@Param("id") id: Long): PermissionSyncAttemptEntity?

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query(
        """
            SELECT attempt FROM PermissionSyncAttemptEntity attempt
            WHERE attempt.id = :id
              AND attempt.claimToken = :token
              AND attempt.status = com.onyx.foss.kotlin.domain.AttemptStatus.IN_PROGRESS
        """,
    )
    fun lockOwned(@Param("id") id: Long, @Param("token") token: UUID): PermissionSyncAttemptEntity?
}

data class PermissionSyncStageRow(
    val sourceDocumentId: String,
    val externalAccess: JsonNode,
    val hasError: Boolean,
)

@Repository
class PermissionSyncStageRepository(
    private val rows: PermissionSyncStageJpaRepository,
    private val attempts: PermissionSyncAttemptRepository,
) {
    @Transactional
    fun upsert(attemptId: Long, rows: Collection<PermissionSyncStageRow>) {
        if (rows.isEmpty()) return
        val ids = rows.map { it.sourceDocumentId }.distinct()
        val merged = this.rows.findAllByAttemptIdAndSourceDocumentIdIn(attemptId, ids)
            .associateByTo(linkedMapOf()) { it.sourceDocumentId }
        rows.forEach { row ->
            val current = merged[row.sourceDocumentId]
            merged[row.sourceDocumentId] = PermissionSyncStageEntity(
                attemptId = attemptId,
                sourceDocumentId = row.sourceDocumentId,
                externalAccess = if (current?.hasError == true) current.externalAccess else row.externalAccess,
                hasError = current?.hasError == true || row.hasError,
            )
        }
        this.rows.saveAll(merged.values)
    }

    fun findPage(attemptId: Long, afterSourceDocumentId: String, limit: Int): List<PermissionSyncStageRow> =
        rows.findAllByAttemptIdAndSourceDocumentIdGreaterThanOrderBySourceDocumentId(
            attemptId,
            afterSourceDocumentId,
            org.springframework.data.domain.PageRequest.of(0, limit),
        ).map { PermissionSyncStageRow(it.sourceDocumentId, requireNotNull(it.externalAccess), it.hasError) }

    fun countForAttempt(attemptId: Long): Long = rows.countByAttemptId(attemptId)

    fun countErrorsForAttempt(attemptId: Long): Long = rows.countByAttemptIdAndHasErrorTrue(attemptId)

    @Transactional
    fun deleteAllForAttempt(attemptId: Long) {
        rows.deleteAllByAttemptId(attemptId)
    }

    @Transactional
    fun deleteTerminalForPair(pairId: Long) {
        rows.deleteAllByAttemptIdIn(
            attempts.findAllByCcPairIdOrderByIdDesc(pairId)
                .filter { it.status in TERMINAL_PERMISSION_STATUSES }
                .mapNotNull { it.id },
        )
    }

    private companion object {
        val TERMINAL_PERMISSION_STATUSES = setOf(
            AttemptStatus.SUCCESS,
            AttemptStatus.FAILED,
            AttemptStatus.COMPLETED_WITH_ERRORS,
        )
    }
}

interface PermissionSyncStageJpaRepository : JpaRepository<PermissionSyncStageEntity, PermissionSyncStageId> {
    fun findAllByAttemptIdAndSourceDocumentIdIn(
        attemptId: Long,
        sourceDocumentIds: Collection<String>,
    ): List<PermissionSyncStageEntity>
    fun findAllByAttemptIdAndSourceDocumentIdGreaterThanOrderBySourceDocumentId(
        attemptId: Long,
        sourceDocumentId: String,
        pageable: Pageable,
    ): List<PermissionSyncStageEntity>
    fun countByAttemptId(attemptId: Long): Long
    fun countByAttemptIdAndHasErrorTrue(attemptId: Long): Long
    fun deleteAllByAttemptId(attemptId: Long)
    fun deleteAllByAttemptIdIn(attemptIds: Collection<Long>)
}

@Repository
class IngestionEnumerationRepository(
    private val rows: IngestionEnumerationJpaRepository,
) {
    @Transactional
    fun registerDocuments(attemptId: Long, sourceDocumentIds: Collection<String>): Set<String> {
        val ids = sourceDocumentIds.filter(String::isNotBlank).distinct()
        val existing = rows.findAllByAttemptIdAndSourceDocumentIdIn(attemptId, ids)
            .associateBy { it.sourceDocumentId }
        rows.saveAll(ids.filterNot(existing::containsKey).map { IngestionEnumeratedDocumentEntity(attemptId, it) })
        return ids.filterTo(mutableSetOf()) { existing[it]?.processed != true }
    }

    @Transactional
    fun markProcessed(attemptId: Long, sourceDocumentId: String) {
        val row = rows.findById(IngestionEnumeratedDocumentId(attemptId, sourceDocumentId)).orElseThrow {
            IllegalStateException("Enumerated document disappeared before processing completed")
        }
        row.processed = true
        rows.save(row)
    }

    @Transactional
    fun protectFailures(attemptId: Long, sourceDocumentIds: Collection<String>) {
        val ids = sourceDocumentIds.filter(String::isNotBlank).distinct()
        val existing = rows.findAllByAttemptIdAndSourceDocumentIdIn(attemptId, ids).mapTo(mutableSetOf()) {
            it.sourceDocumentId
        }
        rows.saveAll(ids.filterNot(existing::contains).map { IngestionEnumeratedDocumentEntity(attemptId, it) })
    }

    fun findMissingPage(pairId: Long, attemptId: Long, afterSourceDocumentId: String, limit: Int): List<String> =
        rows.findMissingSourceDocumentIds(
            pairId,
            attemptId,
            afterSourceDocumentId,
            org.springframework.data.domain.PageRequest.of(0, limit),
        )
}

interface IngestionEnumerationJpaRepository :
    JpaRepository<IngestionEnumeratedDocumentEntity, IngestionEnumeratedDocumentId> {
    fun findAllByAttemptIdAndSourceDocumentIdIn(
        attemptId: Long,
        sourceDocumentIds: Collection<String>,
    ): List<IngestionEnumeratedDocumentEntity>

    @Query(
        """
            SELECT document.sourceDocumentId
            FROM IndexedDocumentEntity document
            WHERE document.ccPairId = :pairId
              AND document.sourceDocumentId > :afterSourceDocumentId
              AND NOT EXISTS (
                  SELECT enumerated.sourceDocumentId FROM IngestionEnumeratedDocumentEntity enumerated
                  WHERE enumerated.attemptId = :attemptId
                    AND enumerated.sourceDocumentId = document.sourceDocumentId
              )
            ORDER BY document.sourceDocumentId
        """,
    )
    fun findMissingSourceDocumentIds(
        @Param("pairId") pairId: Long,
        @Param("attemptId") attemptId: Long,
        @Param("afterSourceDocumentId") afterSourceDocumentId: String,
        pageable: Pageable,
    ): List<String>
}
