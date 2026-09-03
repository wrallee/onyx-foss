package com.onyx.foss.kotlin.api

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.PairStatus
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotEmpty
import java.time.Instant

data class CredentialRequest(
    val credentialJson: JsonNode,
    val adminPublic: Boolean = true,
    val source: ConnectorSource,
    val name: String? = null,
    val curatorPublic: Boolean = true,
    val groups: List<Long> = emptyList(),
)

data class CredentialUpdateRequest(
    @field:NotBlank val name: String,
    val credentialJson: JsonNode,
)

data class ConnectorRequest(
    @field:NotBlank val name: String,
    val source: ConnectorSource,
    val inputType: String = "load_state",
    val connectorSpecificConfig: JsonNode,
    val refreshFreq: Long? = null,
    val pruneFreq: Long? = null,
    val indexingStart: Instant? = null,
    val accessType: String = "public",
    val groups: List<Long> = emptyList(),
)

data class PairMetadataRequest(
    @field:NotBlank val name: String,
    val accessType: String = "public",
    val autoSyncOptions: JsonNode? = null,
    val groups: List<Long>? = null,
    val processingMode: String = "REGULAR",
)

data class PairStatusRequest(val status: PairStatus)

data class CCPropertyUpdateRequest(
    @field:NotBlank val name: String,
    @field:NotBlank val value: String,
)

data class DeletionAttemptRequest(
    val connectorId: Long,
    val credentialId: Long,
)

data class RunConnectorRequest(
    val connectorId: Long,
    @JsonAlias("credentialIds") val credentialIds: List<Long>? = null,
    val fromBeginning: Boolean = false,
)

data class IndexingStatusRequest(
    val source: ConnectorSource? = null,
    val sourceToPage: Map<String, Int> = emptyMap(),
    val nameFilter: String? = null,
)

data class DocumentSetRequest(
    val id: Long? = null,
    @field:NotBlank val name: String,
    val description: String = "",
    @JsonAlias("cc_pair_ids") val ccPairIds: List<Long> = emptyList(),
    val isPublic: Boolean = true,
    val users: List<String> = emptyList(),
    val groups: List<Long> = emptyList(),
)

data class StatusResponse(val success: Boolean, val message: String, val data: Long? = null)
data class ObjectCreationResponse(val id: Long, val credential: Map<String, Any?>? = null)
