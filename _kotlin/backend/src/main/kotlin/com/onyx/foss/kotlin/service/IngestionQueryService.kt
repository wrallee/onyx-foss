package com.onyx.foss.kotlin.service

import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionErrorRepository
import com.onyx.foss.kotlin.domain.PermissionSyncAttemptRepository
import org.springframework.stereotype.Service

@Service
class IngestionQueryService(
    private val attempts: IngestionAttemptRepository,
    private val errors: IngestionErrorRepository,
    private val permissionAttempts: PermissionSyncAttemptRepository,
) {
    fun attempts(pairId: Long, page: Int, pageSize: Int): Map<String, Any?> {
        validatePage(page, pageSize)
        val all = attempts.findAllByCcPairIdOrderByIdDesc(pairId)
        val slice = all.drop(page * pageSize).take(pageSize)
        return mapOf(
            "items" to slice.map { attempt ->
                mapOf(
                    "id" to attempt.id,
                    "status" to attempt.status.value,
                    "from_beginning" to attempt.fromBeginning,
                    "new_docs_indexed" to attempt.newDocsIndexed,
                    "total_docs_indexed" to attempt.totalDocsIndexed,
                    "docs_removed_from_index" to attempt.docsRemovedFromIndex,
                    "error_msg" to attempt.errorMessage,
                    "error_count" to errors.findAllByAttemptIdOrderByIdDesc(requireNotNull(attempt.id)).size,
                    "full_exception_trace" to attempt.fullExceptionTrace,
                    "time_started" to attempt.timeStarted,
                    "time_updated" to attempt.timeUpdated,
                    "poll_range_start" to attempt.pollRangeStart,
                    "poll_range_end" to attempt.pollRangeEnd,
                )
            },
            "total_items" to all.size,
        )
    }

    fun errors(pairId: Long, page: Int, pageSize: Int): Map<String, Any?> {
        validatePage(page, pageSize)
        val ids = attempts.findAllByCcPairIdOrderByIdDesc(pairId).mapNotNull { it.id }
        val all = if (ids.isEmpty()) {
            emptyList()
        } else {
            errors.findAllByAttemptIdInOrderByIdDesc(ids).filterNot { it.isResolved }
        }
        return mapOf(
            "items" to all.drop(page * pageSize).take(pageSize).map {
                mapOf(
                    "id" to it.id,
                    "connector_credential_pair_id" to pairId,
                    "document_id" to it.sourceDocumentId,
                    "document_link" to it.documentLink,
                    "entity_id" to null,
                    "failed_time_range_start" to null,
                    "failed_time_range_end" to null,
                    "failure_message" to it.failureMessage,
                    "is_resolved" to it.isResolved,
                    "time_created" to it.createdAt,
                    "index_attempt_id" to it.attemptId,
                    "error_type" to it.errorType,
                )
            },
            "total_items" to all.size,
        )
    }

    fun permissionAttempts(pairId: Long, page: Int, pageSize: Int): Map<String, Any?> {
        validatePage(page, pageSize)
        val all = permissionAttempts.findAllByCcPairIdOrderByIdDesc(pairId)
        return mapOf(
            "applicable" to true,
            "items" to all.drop(page * pageSize).take(pageSize).map { attempt ->
                mapOf(
                    "id" to attempt.id,
                    "status" to attempt.status.value,
                    "error_message" to attempt.errorMessage,
                    "time_created" to attempt.createdAt,
                    "time_started" to attempt.timeStarted,
                    "time_finished" to attempt.timeFinished,
                )
            },
            "total_items" to all.size,
        )
    }

    private fun validatePage(page: Int, pageSize: Int) {
        require(page >= 0) { "page_num must be non-negative" }
        require(pageSize > 0) { "page_size must be positive" }
    }
}
