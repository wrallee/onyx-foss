package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorSource
import com.onyx.foss.kotlin.service.FileStorageService
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.sax.BodyContentHandler
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

@Service
class FileConnectorLoader(
    private val mapper: ObjectMapper,
    private val files: FileStorageService,
) {
    fun load(config: JsonNode?): Sequence<ConnectorBatch> {
        val locations = config?.path("file_locations")?.takeIf(JsonNode::isArray) ?: mapper.createArrayNode()
        val names = config?.path("file_names")?.takeIf(JsonNode::isArray) ?: mapper.createArrayNode()
        val zipMetadata = config?.path("zip_metadata_file_id")?.asText()?.takeIf(String::isNotBlank)
            ?.let(::readZipMetadata).orEmpty()
        val documents = locations.mapIndexed { index, location ->
            val assetId = location.asText()
            val name = names.get(index)?.asText()?.takeIf(String::isNotBlank) ?: assetId
            document(assetId, name, zipMetadata[name] ?: zipMetadata[name.fileName()])
        }
        return sequenceOf(
            ConnectorBatch(
                documents = documents,
                checkpoint = ConnectorCheckpoint(
                    mapper.valueToTree(mapOf("last_success_at" to Instant.now().toString(), "documents" to documents.size)),
                    hasMore = false,
                ),
            ),
        )
    }

    private fun document(assetId: String, name: String, zipMetadata: JsonNode?): SourceDocument {
        val path = files.filePath(assetId)
        val extracted = extract(path)
        val embedded = extracted.metadata
        val metadata = zipMetadata.asMap() + embedded
        val customMetadata = metadata.filterKeys { it !in ONYX_METADATA_KEYS }
        val documentMetadata = mutableMapOf<String, Any?>("source" to "file")
        documentMetadata.putAll(customMetadata.mapValues { mapper.convertValue(it.value, Any::class.java) })
        if (name.isTabular()) documentMetadata["file_id"] = assetId
        return SourceDocument(
            id = "FILE_CONNECTOR__$assetId",
            title = metadata["title"]?.asText()?.takeIf(String::isNotBlank)
                ?: metadata["file_display_name"]?.asText()?.takeIf(String::isNotBlank)
                ?: name.fileName(),
            content = extracted.content,
            link = metadata["link"]?.asText()?.takeIf(String::isNotBlank),
            metadata = documentMetadata,
            source = metadata["connector_type"]?.asText()?.let { value ->
                ConnectorSource.entries.firstOrNull { it.value.equals(value, ignoreCase = true) }
            } ?: ConnectorSource.FILE,
            updatedAt = metadata["doc_updated_at"]?.asText()?.takeIf(String::isNotBlank)?.let(Instant::parse),
            primaryOwners = metadata["primary_owners"].stringList(),
            secondaryOwners = metadata["secondary_owners"].stringList(),
        )
    }

    private fun extract(path: Path): ExtractedFile {
        val handler = BodyContentHandler(-1)
        Files.newInputStream(path).use { input -> AutoDetectParser().parse(input, handler, Metadata()) }
        val content = handler.toString().trim()
        val firstLine = content.lineSequence().firstOrNull().orEmpty()
        val metadata = ONYX_METADATA.find(firstLine)?.groups?.drop(1)?.firstNotNullOfOrNull { it?.value }
            ?.let { mapper.readTree("{$it}") }
        return ExtractedFile(if (metadata == null) content else content.removePrefix(firstLine).trim(), metadata?.asMap().orEmpty())
    }

    private fun readZipMetadata(assetId: String): Map<String, JsonNode> = try {
        val metadata = mapper.readTree(Files.readString(files.filePath(assetId)))
        when {
            metadata.isArray -> metadata.mapNotNull { entry -> entry.path("filename").asText().takeIf(String::isNotBlank)?.let { it to entry } }.toMap()
            metadata.isObject -> metadata.properties().asSequence().associate { it.key to it.value }
            else -> emptyMap()
        }
    } catch (_: Exception) {
        emptyMap()
    }

    private fun JsonNode?.asMap(): Map<String, JsonNode> =
        if (this?.isObject == true) properties().associate { it.key to it.value } else emptyMap()

    private fun JsonNode?.stringList(): List<String> =
        if (this?.isArray == true) toList().mapNotNull { it.asText().takeIf(String::isNotBlank) } else emptyList()

    private fun String.fileName(): String = substringAfterLast('/').substringAfterLast('\\')

    private fun String.isTabular(): Boolean = listOf(".csv", ".tsv", ".xlsx", ".xlsm").any(lowercase()::endsWith)

    private data class ExtractedFile(val content: String, val metadata: Map<String, JsonNode>)

    private companion object {
        val ONYX_METADATA = Regex("(?:<!--\\s*ONYX_METADATA=\\{(.*?)\\}\\s*-->|#ONYX_METADATA=\\{(.*?)\\})")
        val ONYX_METADATA_KEYS = setOf(
            "document_id", "time_updated", "doc_updated_at", "link", "primary_owners", "secondary_owners",
            "filename", "file_display_name", "title", "connector_type", "pdf_password", "mime_type",
        )
    }
}
