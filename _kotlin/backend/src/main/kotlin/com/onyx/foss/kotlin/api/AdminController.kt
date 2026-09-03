package com.onyx.foss.kotlin.api

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.service.AdminService
import com.onyx.foss.kotlin.service.FileStorageService
import com.onyx.foss.kotlin.service.IngestionQueryService
import jakarta.validation.Valid
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.multipart.MultipartFile

@RestController
@RequestMapping("/manage")
class AdminController(
    private val admin: AdminService,
    private val files: FileStorageService,
    private val ingestion: IngestionQueryService,
    private val mapper: ObjectMapper,
) {
    @PostMapping("/credential")
    fun createCredential(@Valid @RequestBody request: CredentialRequest): ObjectCreationResponse =
        admin.createCredential(request)

    @GetMapping("/credential")
    fun credentials(): List<Map<String, Any?>> = admin.listCredentials(null)

    @GetMapping("/credential/{credentialId}")
    fun credential(@PathVariable credentialId: Long): Map<String, Any?> =
        admin.credentialSnapshot(admin.credential(credentialId))

    @GetMapping("/admin/credential")
    fun adminCredentials(): List<Map<String, Any?>> = admin.listCredentials(null)

    @GetMapping("/admin/similar-credentials/{source}")
    fun similarCredentials(@PathVariable source: String): List<Map<String, Any?>> =
        admin.listCredentials(ConnectorSource.fromValue(source))

    @PutMapping("/admin/credential/{credentialId}")
    fun updateCredential(
        @PathVariable credentialId: Long,
        @Valid @RequestBody request: CredentialUpdateRequest,
    ): Map<String, Any?> = admin.updateCredential(credentialId, request)

    @DeleteMapping("/credential/{credentialId}", "/admin/credential/{credentialId}")
    fun deleteCredential(@PathVariable credentialId: Long): StatusResponse = admin.deleteCredential(credentialId)

    @GetMapping("/connector")
    fun connectors(): List<Map<String, Any?>> = admin.listConnectors(null)

    @GetMapping("/connector/{connectorId}")
    fun connector(@PathVariable connectorId: Long): Map<String, Any?> =
        admin.connectorSnapshot(admin.connector(connectorId))

    @GetMapping("/admin/connector")
    fun adminConnectors(@RequestParam("credential", required = false) credentialId: Long?): List<Map<String, Any?>> =
        admin.listConnectors(credentialId)

    @PostMapping("/admin/connector")
    fun createConnector(@Valid @RequestBody request: ConnectorRequest): ObjectCreationResponse =
        admin.createConnector(request)
    @PostMapping("/admin/connector-with-mock-credential")
    fun createConnectorWithMockCredential(@Valid @RequestBody request: ConnectorRequest): StatusResponse =
        admin.createConnectorWithMockCredential(request)

    @PatchMapping("/admin/connector/{connectorId}")
    fun updateConnector(
        @PathVariable connectorId: Long,
        @Valid @RequestBody request: ConnectorRequest,
    ): Map<String, Any?> = admin.updateConnector(connectorId, request)

    @DeleteMapping("/admin/connector/{connectorId}")
    fun deleteConnector(@PathVariable connectorId: Long): StatusResponse = admin.deleteConnector(connectorId)

    @PostMapping("/admin/deletion-attempt")
    fun deletePair(@RequestBody request: DeletionAttemptRequest): StatusResponse = admin.deletePair(request)

    @PutMapping("/connector/{connectorId}/credential/{credentialId}")
    fun associateCredential(
        @PathVariable connectorId: Long,
        @PathVariable credentialId: Long,
        @Valid @RequestBody request: PairMetadataRequest,
    ): StatusResponse = admin.associate(connectorId, credentialId, request)

    @PostMapping("/admin/connector/run-once")
    fun runConnector(@RequestBody request: RunConnectorRequest): StatusResponse = admin.enqueue(request)

    @PostMapping("/admin/connector/indexing-status")
    fun indexingStatus(@RequestBody request: IndexingStatusRequest): List<Map<String, Any?>> =
        admin.indexingStatus(request.source, request.nameFilter)

    @GetMapping("/admin/connector/status", "/connector-status")
    fun connectorStatus(): List<Map<String, Any?>> = admin.connectorStatuses()

    @PostMapping("/admin/connector/file/upload", consumes = [MediaType.MULTIPART_FORM_DATA_VALUE])
    fun uploadFiles(@RequestParam("files") uploaded: List<MultipartFile>): Map<String, Any?> = files.upload(uploaded)

    @GetMapping("/admin/connector/{connectorId}/files")
    fun connectorFiles(@PathVariable connectorId: Long): Map<String, Any?> = files.listConnectorFiles(connectorId)

    @PostMapping("/admin/connector/{connectorId}/files/update", consumes = [MediaType.MULTIPART_FORM_DATA_VALUE])
    fun updateConnectorFiles(
        @PathVariable connectorId: Long,
        @RequestParam("files", required = false) uploaded: List<MultipartFile>?,
        @RequestParam("file_ids_to_remove", defaultValue = "[]") idsToRemove: String,
    ): Map<String, Any?> = files.updateConnectorFiles(
        connectorId,
        uploaded ?: emptyList(),
        mapper.readValue(idsToRemove, object : TypeReference<List<String>>() {}),
    )

    @GetMapping("/admin/cc-pair/{pairId}")
    fun ccPair(@PathVariable pairId: Long): Map<String, Any?> = admin.pairDetail(pairId)

    @PutMapping("/admin/cc-pair/{pairId}/status")
    fun updatePairStatus(
        @PathVariable pairId: Long,
        @RequestBody request: PairStatusRequest,
    ): Map<String, Any?> = admin.setPairStatus(pairId, request.status)

    @PutMapping("/admin/cc-pair/{pairId}/name")
    fun updatePairName(
        @PathVariable pairId: Long,
        @RequestParam("new_name") name: String,
    ): Map<String, Any?> = admin.renamePair(pairId, name)

    @PutMapping("/admin/cc-pair/{pairId}/property")
    fun updatePairProperty(
        @PathVariable pairId: Long,
        @Valid @RequestBody request: CCPropertyUpdateRequest,
    ): StatusResponse = admin.updatePairProperty(pairId, request)

    @GetMapping("/admin/cc-pair/{pairId}/index-attempts")
    fun attempts(
        @PathVariable pairId: Long,
        @RequestParam("page_num", defaultValue = "0") page: Int,
        @RequestParam("page_size", defaultValue = "10") pageSize: Int,
    ): Map<String, Any?> = ingestion.attempts(pairId, page, pageSize)

    @GetMapping("/admin/cc-pair/{pairId}/errors")
    fun errors(
        @PathVariable pairId: Long,
        @RequestParam("page_num", defaultValue = "0") page: Int,
        @RequestParam("page_size", defaultValue = "10") pageSize: Int,
    ): Map<String, Any?> = ingestion.errors(pairId, page, pageSize)

    @GetMapping("/admin/cc-pair/{pairId}/permission-sync-attempts")
    fun permissionSync(
        @PathVariable pairId: Long,
        @RequestParam("page_num", defaultValue = "0") page: Int,
        @RequestParam("page_size", defaultValue = "10") pageSize: Int,
    ): Map<String, Any?> = ingestion.permissionAttempts(pairId, page, pageSize)

    @GetMapping("/admin/cc-pair/{pairId}/external-group-sync-attempts")
    fun groupSync(@PathVariable pairId: Long): Map<String, Any?> =
        mapOf("applicable" to false, "items" to emptyList<Any>(), "total_items" to 0)

    @PostMapping("/admin/document-set")
    fun createSet(@Valid @RequestBody request: DocumentSetRequest): Long = admin.createSet(request)

    @PatchMapping("/admin/document-set")
    fun updateSet(@Valid @RequestBody request: DocumentSetRequest) = admin.updateSet(request)

    @DeleteMapping("/admin/document-set/{setId}")
    fun deleteSet(@PathVariable setId: Long) = admin.deleteSet(setId)

    @GetMapping("/admin/document-set/{setId}")
    fun documentSet(@PathVariable setId: Long): Map<String, Any?> =
        admin.documentSet(setId)

    @GetMapping("/document-set")
    fun documentSets(): List<Map<String, Any?>> = admin.listSets()

    @GetMapping("/document-set-public")
    fun documentSetPublic(): Map<String, Boolean> = mapOf("is_public" to true)
}
