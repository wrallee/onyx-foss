package com.onyx.foss.kotlin.domain

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.JsonNode
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Index
import jakarta.persistence.Table
import org.hibernate.annotations.CreationTimestamp
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.annotations.UpdateTimestamp
import org.hibernate.type.SqlTypes
import java.time.Instant
import java.util.UUID

enum class ConnectorSource(@get:JsonValue val value: String) {
    FILE("file"),
    JIRA("jira"),
    CONFLUENCE("confluence"),
    GITHUB("github");

    companion object {
        @JvmStatic
        @JsonCreator
        fun fromValue(value: String): ConnectorSource =
            entries.firstOrNull { it.value.equals(value, ignoreCase = true) }
                ?: throw IllegalArgumentException("Unsupported connector type: $value")
    }
}

enum class PairStatus { SCHEDULED, INITIAL_INDEXING, ACTIVE, PAUSED, DELETING, INVALID }
enum class AttemptStatus(@get:JsonValue val value: String) {
    NOT_STARTED("not_started"),
    IN_PROGRESS("in_progress"),
    SUCCESS("success"),
    FAILED("failed"),
    COMPLETED_WITH_ERRORS("completed_with_errors"),
    CANCELED("canceled"),
}
enum class JobState { QUEUED, RUNNING, SUCCEEDED, FAILED }
enum class DocumentSetSyncStatus { PENDING, IN_PROGRESS, DONE }

@Entity
@Table(name = "credentials")
class CredentialEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Enumerated(EnumType.STRING)
    var source: ConnectorSource = ConnectorSource.FILE,
    var name: String? = null,
    @Column(name = "secret_json", nullable = false, columnDefinition = "text")
    var secretJson: String = "",
    @Column(name = "admin_public", nullable = false)
    var adminPublic: Boolean = true,
    @Column(name = "curator_public", nullable = false)
    var curatorPublic: Boolean = true,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "connectors")
class ConnectorEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    var name: String = "",
    @Enumerated(EnumType.STRING)
    var source: ConnectorSource = ConnectorSource.FILE,
    @Column(name = "input_type", nullable = false)
    var inputType: String = "load_state",
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "connector_specific_config", nullable = false, columnDefinition = "jsonb")
    var connectorSpecificConfig: JsonNode? = null,
    @Column(name = "refresh_freq")
    var refreshFreq: Long? = null,
    @Column(name = "prune_freq")
    var pruneFreq: Long? = null,
    @Column(name = "indexing_start")
    var indexingStart: Instant? = null,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(
    name = "connector_credential_pairs",
    indexes = [Index(name = "idx_cc_pair_connector", columnList = "connector_id")],
)
class ConnectorCredentialPairEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "connector_id", nullable = false)
    var connectorId: Long = 0,
    @Column(name = "credential_id", nullable = false)
    var credentialId: Long = 0,
    var name: String = "",
    @Column(name = "access_type", nullable = false)
    var accessType: String = "public",
    @Enumerated(EnumType.STRING)
    var status: PairStatus = PairStatus.ACTIVE,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "auto_sync_options", columnDefinition = "jsonb")
    var autoSyncOptions: JsonNode? = null,
    @Column(name = "processing_mode", nullable = false)
    var processingMode: String = "REGULAR",
    @Column(name = "in_repeated_error_state", nullable = false)
    var inRepeatedErrorState: Boolean = false,
    @Column(name = "last_pruned_at")
    var lastPrunedAt: Instant? = null,
    @Column(name = "ingestion_claim_token")
    var ingestionClaimToken: UUID? = null,
    @Column(name = "ingestion_lease_expires_at")
    var ingestionLeaseExpiresAt: Instant? = null,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "document_sets")
class DocumentSetEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    var name: String = "",
    var description: String? = null,
    @Column(name = "is_public", nullable = false)
    var isPublic: Boolean = true,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "document_set_cc_pairs")
class DocumentSetPairEntity(
    @Id
    @Column(name = "document_set_id")
    var documentSetId: Long = 0,
    @Column(name = "cc_pair_id")
    var ccPairId: Long = 0,
)

@Entity
@Table(name = "document_set_sync_outbox")
class DocumentSetSyncOutboxEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "cc_pair_ids", nullable = false, columnDefinition = "jsonb")
    var ccPairIds: JsonNode? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "document_set_ids", columnDefinition = "jsonb")
    var documentSetIds: JsonNode? = null,
    @Enumerated(EnumType.STRING)
    var status: DocumentSetSyncStatus = DocumentSetSyncStatus.PENDING,
    @Column(name = "attempt_count", nullable = false)
    var attemptCount: Int = 0,
    @Column(name = "last_error")
    var lastError: String? = null,
    @Column(name = "locked_at")
    var lockedAt: Instant? = null,
    @Column(name = "claim_token")
    var claimToken: UUID? = null,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "file_assets")
class FileAssetEntity(
    @Id
    var id: String = "",
    @Column(name = "original_name", nullable = false)
    var originalName: String = "",
    @Column(name = "media_type")
    var mediaType: String? = null,
    @Column(name = "byte_size", nullable = false)
    var byteSize: Long = 0,
    @Column(name = "storage_path", nullable = false)
    var storagePath: String = "",
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
)

@Entity
@Table(name = "ingestion_attempts", indexes = [Index(name = "idx_attempt_pair", columnList = "cc_pair_id")])
class IngestionAttemptEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "cc_pair_id", nullable = false)
    var ccPairId: Long = 0,
    @Enumerated(EnumType.STRING)
    var status: AttemptStatus = AttemptStatus.NOT_STARTED,
    @Column(name = "from_beginning", nullable = false)
    var fromBeginning: Boolean = false,
    @Column(name = "prune_only", nullable = false)
    var pruneOnly: Boolean = false,
    @Column(name = "enumeration_complete", nullable = false)
    var enumerationComplete: Boolean = false,
    @Column(name = "new_docs_indexed", nullable = false)
    var newDocsIndexed: Int = 0,
    @Column(name = "total_docs_indexed", nullable = false)
    var totalDocsIndexed: Int = 0,
    @Column(name = "docs_removed_from_index", nullable = false)
    var docsRemovedFromIndex: Int = 0,
    @Column(name = "error_msg")
    var errorMessage: String? = null,
    @Column(name = "full_exception_trace")
    var fullExceptionTrace: String? = null,
    @Column(name = "time_started")
    var timeStarted: Instant? = null,
    @UpdateTimestamp @Column(name = "time_updated")
    var timeUpdated: Instant? = null,
    @Column(name = "poll_range_start")
    var pollRangeStart: Instant? = null,
    @Column(name = "poll_range_end")
    var pollRangeEnd: Instant? = null,
)

@Entity
@Table(name = "ingestion_checkpoints")
class IngestionCheckpointEntity(
    @Id @Column(name = "cc_pair_id")
    var ccPairId: Long = 0,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "checkpoint_json", nullable = false, columnDefinition = "jsonb")
    var checkpointJson: JsonNode? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "ingestion_jobs", indexes = [Index(name = "idx_job_state", columnList = "state,run_after")])
class IngestionJobEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "attempt_id", nullable = false)
    var attemptId: Long = 0,
    @Column(name = "cc_pair_id", nullable = false)
    var ccPairId: Long = 0,
    @Enumerated(EnumType.STRING)
    var state: JobState = JobState.QUEUED,
    @Column(name = "run_after", nullable = false)
    var runAfter: Instant = Instant.now(),
    @Column(name = "locked_at")
    var lockedAt: Instant? = null,
    @Column(name = "locked_by")
    var lockedBy: String? = null,
    @Column(name = "claim_token")
    var claimToken: UUID? = null,
    @Column(name = "lease_expires_at")
    var leaseExpiresAt: Instant? = null,
    var attempts: Int = 0,
    @Column(name = "last_error")
    var lastError: String? = null,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)

@Entity
@Table(name = "indexed_documents")
class IndexedDocumentEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "cc_pair_id", nullable = false)
    var ccPairId: Long = 0,
    @Column(name = "source_document_id", nullable = false)
    var sourceDocumentId: String = "",
    var title: String = "",
    var link: String? = null,
    @Column(name = "content_hash", nullable = false)
    var contentHash: String = "",
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    var metadata: JsonNode? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "external_access", columnDefinition = "jsonb")
    var externalAccess: JsonNode? = null,
    @Column(name = "last_synced", nullable = false)
    var lastSynced: Instant = Instant.now(),
    @Column(name = "last_modified")
    var lastModified: Instant? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "primary_owners", nullable = false, columnDefinition = "jsonb")
    var primaryOwners: List<String> = emptyList(),
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "secondary_owners", nullable = false, columnDefinition = "jsonb")
    var secondaryOwners: List<String> = emptyList(),
)

@Entity
@Table(name = "ingestion_errors")
class IngestionErrorEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "attempt_id", nullable = false)
    var attemptId: Long = 0,
    @Column(name = "source_document_id")
    var sourceDocumentId: String? = null,
    @Column(name = "document_link")
    var documentLink: String? = null,
    @Column(name = "entity_id")
    var entityId: String? = null,
    @Column(name = "failed_time_range_start")
    var failedTimeRangeStart: Instant? = null,
    @Column(name = "failed_time_range_end")
    var failedTimeRangeEnd: Instant? = null,
    @Column(name = "failure_message", nullable = false)
    var failureMessage: String = "",
    @Column(name = "error_type")
    var errorType: String? = null,
    @Column(name = "is_resolved", nullable = false)
    var isResolved: Boolean = false,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
)

@Entity
@Table(name = "permission_sync_attempts")
class PermissionSyncAttemptEntity(
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "cc_pair_id", nullable = false)
    var ccPairId: Long = 0,
    @Enumerated(EnumType.STRING)
    var status: AttemptStatus = AttemptStatus.NOT_STARTED,
    @Column(name = "error_msg")
    var errorMessage: String? = null,
    @Column(name = "full_exception_trace")
    var fullExceptionTrace: String? = null,
    @Column(name = "total_docs_synced", nullable = false)
    var totalDocsSynced: Int = 0,
    @Column(name = "docs_with_permission_errors", nullable = false)
    var docsWithPermissionErrors: Int = 0,
    @Column(name = "time_started")
    var timeStarted: Instant? = null,
    @Column(name = "time_finished")
    var timeFinished: Instant? = null,
    @Column(name = "claim_token")
    var claimToken: UUID? = null,
    @Column(name = "lease_expires_at")
    var leaseExpiresAt: Instant? = null,
    @Column(name = "follow_up_requested", nullable = false)
    var followUpRequested: Boolean = false,
    @CreationTimestamp @Column(name = "created_at", updatable = false)
    var createdAt: Instant? = null,
    @UpdateTimestamp @Column(name = "updated_at")
    var updatedAt: Instant? = null,
)
