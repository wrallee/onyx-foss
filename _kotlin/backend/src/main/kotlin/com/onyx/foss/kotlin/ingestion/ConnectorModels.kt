package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.domain.ConnectorSource
import java.time.Instant

data class ExternalAccess(
    val externalUserEmails: Set<String> = emptySet(),
    val externalUserGroupIds: Set<String> = emptySet(),
    val isPublic: Boolean,
) {
    val numEntries: Int get() = externalUserEmails.size + externalUserGroupIds.size
}

sealed interface FailureTarget {
    data class Document(val id: String, val link: String? = null) : FailureTarget
    data class Entity(val id: String, val missedStart: Instant? = null, val missedEnd: Instant? = null) : FailureTarget
}

data class ConnectorFailure(val target: FailureTarget, val message: String, val errorType: String? = null)

data class ConnectorCheckpoint(val value: JsonNode, val hasMore: Boolean)

data class ConnectorBatch(
    val documents: List<SourceDocument> = emptyList(),
    val failures: List<ConnectorFailure> = emptyList(),
    val checkpoint: ConnectorCheckpoint,
)

data class SourceDocument(
    val id: String,
    val title: String,
    val content: String,
    val link: String? = null,
    val metadata: Map<String, Any?> = emptyMap(),
    val externalAccess: ExternalAccess? = null,
    val source: ConnectorSource? = null,
    val updatedAt: Instant? = null,
    val primaryOwners: List<String> = emptyList(),
    val secondaryOwners: List<String> = emptyList(),
)
