package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.onyx.foss.kotlin.api.ApiException
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.domain.FileAssetEntity
import com.onyx.foss.kotlin.domain.FileAssetRepository
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.multipart.MultipartFile
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID

@Service
class FileStorageService(
    private val properties: OnyxProperties,
    private val mapper: ObjectMapper,
    private val fileAssets: FileAssetRepository,
    private val admin: AdminService,
) {
    private val root: Path = Path.of(properties.storage.root).toAbsolutePath().normalize()

    init {
        Files.createDirectories(root)
    }

    @Transactional
    fun upload(files: List<MultipartFile>): Map<String, Any?> {
        if (files.isEmpty()) throw ApiException(HttpStatus.BAD_REQUEST, "At least one file is required")
        val assets = files.filter { !it.isEmpty }.map(::store)
        if (assets.isEmpty()) throw ApiException(HttpStatus.BAD_REQUEST, "No non-empty files were uploaded")
        return mapOf(
            "file_paths" to assets.map { it.id },
            "file_names" to assets.map { it.originalName },
            "zip_metadata_file_id" to null,
        )
    }

    fun listConnectorFiles(connectorId: Long): Map<String, Any?> {
        val connector = admin.connector(connectorId)
        if (connector.source != ConnectorSource.FILE) {
            throw ApiException(HttpStatus.BAD_REQUEST, "This endpoint only works with file connectors")
        }
        val locations = connector.connectorSpecificConfig?.path("file_locations") ?: mapper.createArrayNode()
        val names = connector.connectorSpecificConfig?.path("file_names") ?: mapper.createArrayNode()
        val files = locations.mapIndexed { index, location ->
            val asset = fileAssets.findById(location.asText()).orElse(null)
            mapOf(
                "file_id" to location.asText(),
                "file_name" to (asset?.originalName ?: names.get(index)?.asText() ?: location.asText()),
                "file_size" to asset?.byteSize,
                "upload_date" to asset?.createdAt,
            )
        }
        return mapOf("files" to files)
    }

    @Transactional
    fun updateConnectorFiles(
        connectorId: Long,
        newFiles: List<MultipartFile>,
        idsToRemove: List<String>,
    ): Map<String, Any?> {
        val connector = admin.connector(connectorId)
        if (connector.source != ConnectorSource.FILE) {
            throw ApiException(HttpStatus.BAD_REQUEST, "This endpoint only works with file connectors")
        }
        val config = ((connector.connectorSpecificConfig ?: mapper.createObjectNode()).deepCopy<ObjectNode>())
        val currentLocations = config.withArray("file_locations")
            .map { it.asText() }
            .filterNot(idsToRemove::contains)
            .toMutableList()
        val currentNames = config.withArray("file_names").map { it.asText() }.toMutableList()
        val added = newFiles.filter { !it.isEmpty }.map(::store)
        currentLocations += added.map { it.id }
        currentNames += added.map { it.originalName }
        config.set<ArrayNode>("file_locations", mapper.valueToTree(currentLocations))
        config.set<ArrayNode>("file_names", mapper.valueToTree(currentNames))
        connector.connectorSpecificConfig = config
        admin.updateConnector(
            connectorId,
            com.onyx.foss.kotlin.api.ConnectorRequest(
                name = connector.name,
                source = connector.source,
                inputType = connector.inputType,
                connectorSpecificConfig = config,
                refreshFreq = connector.refreshFreq,
                pruneFreq = connector.pruneFreq,
                indexingStart = connector.indexingStart,
            ),
        )
        admin.enqueue(
            com.onyx.foss.kotlin.api.RunConnectorRequest(
                connectorId = connectorId,
                fromBeginning = true,
            ),
        )
        return mapOf(
            "file_paths" to added.map { it.id },
            "file_names" to added.map { it.originalName },
            "zip_metadata_file_id" to null,
        )
    }

    fun filePath(assetId: String): Path {
        val asset = fileAssets.findById(assetId).orElseThrow {
            ApiException(HttpStatus.NOT_FOUND, "Uploaded file not found")
        }
        val path = Path.of(asset.storagePath).toAbsolutePath().normalize()
        if (!path.startsWith(root) || !Files.isRegularFile(path)) {
            throw ApiException(HttpStatus.NOT_FOUND, "Uploaded file content is unavailable")
        }
        return path
    }

    private fun store(file: MultipartFile): FileAssetEntity {
        val assetId = UUID.randomUUID().toString()
        val path = root.resolve(assetId).normalize()
        if (!path.startsWith(root)) error("Invalid file storage path")
        file.inputStream.use { Files.copy(it, path, StandardCopyOption.REPLACE_EXISTING) }
        return fileAssets.save(
            FileAssetEntity(
                id = assetId,
                originalName = file.originalFilename?.substringAfterLast('/')?.substringAfterLast('\\') ?: assetId,
                mediaType = file.contentType,
                byteSize = file.size,
                storagePath = path.toString(),
            ),
        )
    }
}
