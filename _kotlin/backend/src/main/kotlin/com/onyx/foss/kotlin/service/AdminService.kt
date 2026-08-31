package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.onyx.foss.kotlin.api.ApiException
import com.onyx.foss.kotlin.api.CCPropertyUpdateRequest
import com.onyx.foss.kotlin.api.ConnectorRequest
import com.onyx.foss.kotlin.api.CredentialRequest
import com.onyx.foss.kotlin.api.CredentialUpdateRequest
import com.onyx.foss.kotlin.api.DeletionAttemptRequest
import com.onyx.foss.kotlin.api.DocumentSetRequest
import com.onyx.foss.kotlin.api.ObjectCreationResponse
import com.onyx.foss.kotlin.api.PairMetadataRequest
import com.onyx.foss.kotlin.api.RunConnectorRequest
import com.onyx.foss.kotlin.api.StatusResponse
import com.onyx.foss.kotlin.domain.AttemptStatus
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairEntity
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorEntity
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.CredentialEntity
import com.onyx.foss.kotlin.domain.CredentialRepository
import com.onyx.foss.kotlin.domain.DocumentSetEntity
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.domain.IndexedDocumentRepository
import com.onyx.foss.kotlin.domain.IngestionAttemptEntity
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.IngestionJobEntity
import com.onyx.foss.kotlin.domain.IngestionJobRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import com.onyx.foss.kotlin.security.CredentialCipher
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class AdminService(
    private val mapper: ObjectMapper,
    private val cipher: CredentialCipher,
    private val credentials: CredentialRepository,
    private val connectors: ConnectorRepository,
    private val pairs: ConnectorCredentialPairRepository,
    private val sets: DocumentSetRepository,
    private val attempts: IngestionAttemptRepository,
    private val jobs: IngestionJobRepository,
    private val documents: IndexedDocumentRepository,
    private val indexer: OpenSearchIndexer,
    private val jdbc: JdbcTemplate,
) {
    @Transactional
    fun createCredential(request: CredentialRequest): ObjectCreationResponse {
        val saved = credentials.save(
            CredentialEntity(
                source = request.source,
                name = request.name?.trim()?.ifBlank { null },
                secretJson = cipher.encrypt(request.credentialJson),
                adminPublic = request.adminPublic,
                curatorPublic = request.curatorPublic,
            ),
        )
        return ObjectCreationResponse(id(saved), credentialSnapshot(saved))
    }

    fun listCredentials(source: ConnectorSource?): List<Map<String, Any?>> =
        (source?.let(credentials::findAllBySource) ?: credentials.findAll()).map(::credentialSnapshot)

    fun credentialSnapshot(value: CredentialEntity): Map<String, Any?> = mapOf(
        "id" to id(value),
        "credential_json" to cipher.masked(cipher.decrypt(value.secretJson)),
        "admin_public" to value.adminPublic,
        "curator_public" to value.curatorPublic,
        "groups" to emptyList<Long>(),
        "source" to value.source.value,
        "name" to value.name,
        "user_id" to null,
        "user_email" to null,
        "time_created" to value.createdAt,
        "time_updated" to value.updatedAt,
    )

    @Transactional
    fun updateCredential(credentialId: Long, request: CredentialUpdateRequest): Map<String, Any?> {
        val value = credential(credentialId)
        value.name = request.name.trim()
        value.secretJson = cipher.encrypt(mergeMaskedCredential(cipher.decrypt(value.secretJson), request.credentialJson))
        return credentialSnapshot(credentials.save(value))
    }

    @Transactional
    fun deleteCredential(credentialId: Long): StatusResponse {
        if (pairs.findAllByCredentialId(credentialId).isNotEmpty()) {
            throw ApiException(HttpStatus.CONFLICT, "Credential is still associated with a connector")
        }
        credentials.delete(credential(credentialId))
        return StatusResponse(true, "Credential deleted successfully", credentialId)
    }

    @Transactional
    fun createConnector(request: ConnectorRequest): ObjectCreationResponse {
        val saved = connectors.save(
            ConnectorEntity(
                name = request.name.trim(),
                source = request.source,
                inputType = request.inputType,
                connectorSpecificConfig = request.connectorSpecificConfig,
                refreshFreq = request.refreshFreq,
                pruneFreq = request.pruneFreq,
                indexingStart = request.indexingStart,
            ),
        )
        return ObjectCreationResponse(id(saved))
    }
    @Transactional
    fun createConnectorWithMockCredential(request: ConnectorRequest): StatusResponse {
        val connector = connectors.save(
            ConnectorEntity(
                name = request.name.trim(),
                source = request.source,
                inputType = request.inputType,
                connectorSpecificConfig = request.connectorSpecificConfig,
                refreshFreq = request.refreshFreq,
                pruneFreq = request.pruneFreq,
                indexingStart = request.indexingStart,
            ),
        )
        val credential = credentials.save(
            CredentialEntity(
                source = request.source,
                name = request.name.trim(),
                secretJson = cipher.encrypt(mapper.createObjectNode()),
            ),
        )
        val pair = pairs.save(
            ConnectorCredentialPairEntity(
                connectorId = id(connector),
                credentialId = id(credential),
                name = request.name.trim(),
                accessType = request.accessType,
            ),
        )
        enqueuePair(id(pair), false)
        return StatusResponse(true, "Connector created successfully", id(pair))
    }

    fun listConnectors(credentialId: Long?): List<Map<String, Any?>> {
        val values = if (credentialId == null) connectors.findAll() else {
            pairs.findAllByCredentialId(credentialId).mapNotNull { connectors.findById(it.connectorId).orElse(null) }
        }
        return values.distinctBy { it.id }.map(::connectorSnapshot)
    }

    fun connectorSnapshot(value: ConnectorEntity): Map<String, Any?> = mapOf(
        "id" to id(value),
        "name" to value.name,
        "source" to value.source.value,
        "input_type" to value.inputType,
        "connector_specific_config" to (value.connectorSpecificConfig ?: mapper.createObjectNode()),
        "refresh_freq" to value.refreshFreq,
        "prune_freq" to value.pruneFreq,
        "indexing_start" to value.indexingStart,
        "credential_ids" to pairs.findAllByConnectorId(id(value)).map { it.credentialId },
        "time_created" to value.createdAt,
        "time_updated" to value.updatedAt,
    )

    @Transactional
    fun updateConnector(connectorId: Long, request: ConnectorRequest): Map<String, Any?> {
        val value = connector(connectorId)
        value.name = request.name.trim()
        value.source = request.source
        value.inputType = request.inputType
        value.connectorSpecificConfig = request.connectorSpecificConfig
        value.refreshFreq = request.refreshFreq
        value.pruneFreq = request.pruneFreq
        value.indexingStart = request.indexingStart
        return connectorSnapshot(connectors.save(value))
    }

    @Transactional
    fun deleteConnector(connectorId: Long): StatusResponse {
        val pairIds = pairs.findAllByConnectorId(connectorId).map(::id)
        pairIds.forEach { pairId ->
            indexer.deletePair(pairId)
            documents.deleteAllByCcPairId(pairId)
        }
        connectors.delete(connector(connectorId))
        return StatusResponse(true, "Connector deleted successfully", connectorId)
    }

    @Transactional
    fun deletePair(request: DeletionAttemptRequest): StatusResponse {
        val pair = pairs.findByConnectorIdAndCredentialId(request.connectorId, request.credentialId)
            ?: throw ApiException(HttpStatus.NOT_FOUND, "Connector credential pair not found")
        val pairId = id(pair)
        val hasOtherPairs = pairs.findAllByConnectorId(request.connectorId).any { id(it) != pairId }
        indexer.deletePair(pairId)
        documents.deleteAllByCcPairId(pairId)
        pairs.delete(pair)
        if (!hasOtherPairs) connectors.delete(connector(request.connectorId))
        return StatusResponse(true, "Connector deletion completed", pairId)
    }

    @Transactional
    fun associate(connectorId: Long, credentialId: Long, request: PairMetadataRequest): StatusResponse {
        val connector = connector(connectorId)
        val credential = credential(credentialId)
        if (connector.source != credential.source) {
            throw ApiException(HttpStatus.BAD_REQUEST, "Connector and credential source do not match")
        }
        val existingPair = pairs.findByConnectorIdAndCredentialId(connectorId, credentialId)
        val pair = existingPair ?: ConnectorCredentialPairEntity(connectorId = connectorId, credentialId = credentialId)
        pair.name = request.name.trim()
        pair.accessType = request.accessType
        pair.autoSyncOptions = request.autoSyncOptions
        pair.processingMode = request.processingMode
        pair.status = if (existingPair == null) PairStatus.SCHEDULED else PairStatus.ACTIVE
        val pairId = id(pairs.save(pair))
        if (existingPair == null) enqueuePair(pairId, fromBeginning = true)
        return StatusResponse(true, "Credential linked successfully", pairId)
    }

    fun pairDetail(pairId: Long): Map<String, Any?> {
        val pair = pair(pairId)
        val latest = attempts.findFirstByCcPairIdOrderByIdDesc(pairId)
        return mapOf(
            "id" to pairId,
            "name" to pair.name,
            "status" to pair.status.name,
            "in_repeated_error_state" to pair.inRepeatedErrorState,
            "num_docs_indexed" to documents.countByCcPairId(pairId),
            "connector" to connectorSnapshot(connector(pair.connectorId)),
            "credential" to credentialSnapshot(credential(pair.credentialId)),
            "number_of_index_attempts" to attempts.findAllByCcPairIdOrderByIdDesc(pairId).size,
            "last_index_attempt_status" to latest?.status?.value,
            "latest_deletion_attempt" to null,
            "access_type" to pair.accessType,
            "is_editable_for_current_user" to true,
            "permissions" to mapOf("edit" to true, "delete" to true, "manage" to true),
            "deletion_failure_message" to null,
            "indexing" to (latest?.status == AttemptStatus.IN_PROGRESS),
            "creator" to null,
            "creator_email" to null,
            "last_indexed" to latest?.takeIf { it.status == AttemptStatus.SUCCESS }?.timeUpdated,
            "last_pruned" to null,
            "last_full_permission_sync" to null,
            "overall_indexing_speed" to null,
            "latest_checkpoint_description" to null,
            "last_permission_sync_attempt_status" to null,
            "permission_syncing" to false,
            "last_permission_sync_attempt_finished" to null,
            "last_permission_sync_attempt_error_message" to null,
            "supports_targeted_reindex" to false,
        )
    }

    @Transactional
    fun setPairStatus(pairId: Long, status: PairStatus): Map<String, Any?> {
        val pair = pair(pairId)
        pair.status = status
        pairs.save(pair)
        return pairDetail(pairId)
    }

    @Transactional
    fun enqueue(request: RunConnectorRequest): StatusResponse {
        connector(request.connectorId)
        val all = pairs.findAllByConnectorId(request.connectorId)
        val selected = if (request.credentialIds.isNullOrEmpty() || request.credentialIds == listOf(0L)) {
            all
        } else {
            all.filter { request.credentialIds.contains(it.credentialId) }
        }
        if (selected.isEmpty()) throw ApiException(HttpStatus.BAD_REQUEST, "Connector has no valid credentials")
        selected.forEach { enqueuePair(id(it), request.fromBeginning) }
        return StatusResponse(true, "Connector indexing requested", request.connectorId)
    }

    @Transactional
    fun enqueuePair(pairId: Long, fromBeginning: Boolean, pruneOnly: Boolean = false): Long {
        val attempt = attempts.save(
            IngestionAttemptEntity(
                ccPairId = pairId,
                fromBeginning = fromBeginning,
                pruneOnly = pruneOnly,
            ),
        )
        return id(jobs.save(IngestionJobEntity(attemptId = id(attempt), state = JobState.QUEUED)))
    }

    fun indexingStatus(source: ConnectorSource?, filter: String?): List<Map<String, Any?>> =
        pairs.findAll().asSequence()
            .filter { source == null || connector(it.connectorId).source == source }
            .filter { filter.isNullOrBlank() || it.name.contains(filter, true) }
            .groupBy { connector(it.connectorId).source }
            .map { (kind, values) ->
                mapOf(
                    "source" to kind.value,
                    "summary" to mapOf(
                        "total_connectors" to values.size,
                        "active_connectors" to values.count { it.status == PairStatus.ACTIVE },
                        "public_connectors" to values.count { it.accessType == "public" },
                        "total_docs_indexed" to values.sumOf { documents.countByCcPairId(id(it)) },
                    ),
                    "current_page" to 1,
                    "total_pages" to 1,
                    "indexing_statuses" to values.map(::indexingRow),
                )
            }
    fun connectorStatuses(): List<Map<String, Any?>> = pairs.findAll().map { pair -> mapOf("cc_pair_id" to id(pair), "name" to pair.name, "connector" to connectorSnapshot(connector(pair.connectorId)), "credential" to credentialSnapshot(credential(pair.credentialId)), "access_type" to pair.accessType, "groups" to emptyList<Long>()) }

    private fun indexingRow(pair: ConnectorCredentialPairEntity): Map<String, Any?> {
        val pairId = id(pair)
        val latest = attempts.findFirstByCcPairIdOrderByIdDesc(pairId)
        return mapOf(
            "cc_pair_id" to pairId,
            "name" to pair.name,
            "source" to connector(pair.connectorId).source.value,
            "access_type" to pair.accessType,
            "cc_pair_status" to pair.status.name,
            "in_progress" to (latest?.status == AttemptStatus.IN_PROGRESS),
            "in_repeated_error_state" to pair.inRepeatedErrorState,
            "last_finished_status" to latest?.status?.takeIf { it != AttemptStatus.IN_PROGRESS }?.value,
            "last_status" to latest?.status?.value,
            "last_success" to latest?.takeIf { it.status == AttemptStatus.SUCCESS }?.timeUpdated,
            "is_editable" to true,
            "permissions" to mapOf("edit" to true, "delete" to true, "manage" to true),
            "docs_indexed" to documents.countByCcPairId(pairId),
            "latest_index_attempt_docs_indexed" to latest?.totalDocsIndexed,
        )
    }

    @Transactional
    fun createSet(request: DocumentSetRequest): Long {
        validatePairs(request.ccPairIds)
        if (sets.existsByName(request.name.trim())) {
            throw ApiException(HttpStatus.CONFLICT, "Document set name already exists")
        }
        val set = sets.save(DocumentSetEntity(name = request.name.trim(), description = request.description, isPublic = true))
        replaceSetPairs(id(set), request.ccPairIds)
        return id(set)
    }

    @Transactional
    fun updateSet(request: DocumentSetRequest) {
        val setId = request.id ?: throw ApiException(HttpStatus.BAD_REQUEST, "Document set id is required")
        val set = sets.findById(setId).orElseThrow { ApiException(HttpStatus.NOT_FOUND, "Document set not found") }
        validatePairs(request.ccPairIds)
        if (sets.existsByNameAndIdNot(request.name.trim(), setId)) {
            throw ApiException(HttpStatus.CONFLICT, "Document set name already exists")
        }
        set.name = request.name.trim()
        set.description = request.description
        set.isPublic = true
        sets.save(set)
        replaceSetPairs(setId, request.ccPairIds)
    }

    @Transactional
    fun deleteSet(setId: Long) {
        if (!sets.existsById(setId)) throw ApiException(HttpStatus.NOT_FOUND, "Document set not found")
        jdbc.update("DELETE FROM document_set_cc_pairs WHERE document_set_id = ?", setId)
        sets.deleteById(setId)
    }

    fun listSets(): List<Map<String, Any?>> = sets.findAll().map(::setSnapshot)

    fun setSnapshot(set: DocumentSetEntity): Map<String, Any?> {
        val setId = id(set)
        val pairIds = jdbc.queryForList(
            "SELECT cc_pair_id FROM document_set_cc_pairs WHERE document_set_id = ? ORDER BY cc_pair_id",
            Long::class.java,
            setId,
        )
        return mapOf(
            "id" to setId,
            "name" to set.name,
            "description" to set.description,
            "cc_pair_summaries" to pairIds.map(::pairSummary),
            "cc_pair_descriptors" to pairIds.map(::pairDescriptor),
            "is_up_to_date" to true,
            "is_public" to true,
            "users" to emptyList<String>(),
            "groups" to emptyList<Long>(),
            "permissions" to mapOf("edit" to true, "delete" to true),
            "federated_connector_summaries" to emptyList<Any>(),
            "federated_connectors" to emptyList<Any>(),
        )
    }

    private fun pairSummary(pairId: Long): Map<String, Any?> {
        val pair = pair(pairId)
        return mapOf("id" to pairId, "name" to pair.name, "source" to connector(pair.connectorId).source.value, "access_type" to pair.accessType)
    }

    private fun pairDescriptor(pairId: Long): Map<String, Any?> {
        val pair = pair(pairId)
        return mapOf(
            "id" to pairId,
            "name" to pair.name,
            "connector" to connectorSnapshot(connector(pair.connectorId)),
            "credential" to credentialSnapshot(credential(pair.credentialId)),
            "access_type" to pair.accessType,
        )
    }

    private fun replaceSetPairs(setId: Long, pairIds: List<Long>) {
        jdbc.update("DELETE FROM document_set_cc_pairs WHERE document_set_id = ?", setId)
        pairIds.distinct().forEach { jdbc.update("INSERT INTO document_set_cc_pairs(document_set_id, cc_pair_id) VALUES (?, ?)", setId, it) }
    }

    private fun validatePairs(pairIds: List<Long>) {
        if (pairIds.any { !pairs.existsById(it) }) throw ApiException(HttpStatus.BAD_REQUEST, "Document set references a missing connector")
    }

    private fun mergeMaskedCredential(current: JsonNode, update: JsonNode): JsonNode {
        if (!current.isObject || !update.isObject) return update
        val merged = update.deepCopy<ObjectNode>()
        update.fields().forEach { (name, value) ->
            if (value.isTextual && value.asText() == "********") merged.set<JsonNode>(name, current.path(name))
        }
        return merged
    }

    fun connector(connectorId: Long): ConnectorEntity =
        connectors.findById(connectorId).orElseThrow { ApiException(HttpStatus.NOT_FOUND, "Connector not found") }

    fun credential(credentialId: Long): CredentialEntity =
        credentials.findById(credentialId).orElseThrow { ApiException(HttpStatus.NOT_FOUND, "Credential not found") }

    fun credentialSecret(credentialId: Long) =
        cipher.decrypt(credential(credentialId).secretJson)

    fun pair(pairId: Long): ConnectorCredentialPairEntity =
        pairs.findById(pairId).orElseThrow { ApiException(HttpStatus.NOT_FOUND, "CC Pair not found") }

    fun documentSet(setId: Long): Map<String, Any?> = setSnapshot(
        sets.findById(setId).orElseThrow { ApiException(HttpStatus.NOT_FOUND, "Document set not found") },
    )
    @Transactional
    fun renamePair(pairId: Long, name: String): Map<String, Any?> {
        val value = pair(pairId)
        value.name = name.trim()
        pairs.save(value)
        return pairDetail(pairId)
    }

    @Transactional
    fun updatePairProperty(pairId: Long, request: CCPropertyUpdateRequest): StatusResponse {
        val pair = pair(pairId)
        val connector = connector(pair.connectorId)
        val value = request.value.toLongOrNull()
            ?: throw ApiException(HttpStatus.BAD_REQUEST, "Property value must be an integer")
        val message = when (request.name) {
            "refresh_frequency" -> {
                if (value < 60) {
                    throw ApiException(HttpStatus.BAD_REQUEST, "Refresh frequency must be at least 60 seconds")
                }
                connector.refreshFreq = value
                "Refresh frequency updated successfully"
            }
            "pruning_frequency" -> {
                if (value < 300) {
                    throw ApiException(HttpStatus.BAD_REQUEST, "Pruning frequency must be at least 300 seconds")
                }
                connector.pruneFreq = value
                "Pruning frequency updated successfully"
            }
            else -> throw ApiException(HttpStatus.BAD_REQUEST, "Property name ${request.name} is not valid")
        }
        connectors.save(connector)
        return StatusResponse(true, message, pairId)
    }

    private fun id(entity: Any): Long = when (entity) {
        is CredentialEntity -> requireNotNull(entity.id)
        is ConnectorEntity -> requireNotNull(entity.id)
        is ConnectorCredentialPairEntity -> requireNotNull(entity.id)
        is DocumentSetEntity -> requireNotNull(entity.id)
        is IngestionAttemptEntity -> requireNotNull(entity.id)
        is IngestionJobEntity -> requireNotNull(entity.id)
        else -> error("Unsupported entity id")
    }
}
