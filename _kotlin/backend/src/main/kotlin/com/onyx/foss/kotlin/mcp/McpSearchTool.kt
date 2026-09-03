package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.service.SearchService
import io.modelcontextprotocol.spec.McpSchema
import org.springframework.stereotype.Component

@Component
class McpSearchTool(
    private val search: SearchService,
    private val mapper: ObjectMapper,
) {
    fun definition(): McpSchema.Tool = McpSchema.Tool.builder("search", INPUT_SCHEMA)
        .description("Search indexed Onyx documents. document_sets uses union semantics.")
        .build()

    private companion object {
        val INPUT_SCHEMA: Map<String, Any> = mapOf(
                "type" to "object",
                "properties" to mapOf(
                    "query" to mapOf("type" to "string", "minLength" to 1),
                    "document_sets" to mapOf(
                        "type" to "array",
                        "items" to mapOf("type" to "string", "minLength" to 1),
                        "uniqueItems" to true,
                    ),
                    "limit" to mapOf("type" to "integer", "minimum" to 1, "maximum" to 20, "default" to 10),
                ),
                "required" to listOf("query"),
                "additionalProperties" to false,
            )
    }

    fun call(arguments: Map<String, Any>, defaultDocumentSets: List<String> = emptyList()): McpSchema.CallToolResult = try {
        val query = arguments["query"] as? String ?: throw IllegalArgumentException("query must be a string")
        val requestedSets = (arguments["document_sets"] as? List<*>)?.map {
            it as? String ?: throw IllegalArgumentException("document_sets must contain strings")
        }
        val documentSets = if (!requestedSets.isNullOrEmpty()) requestedSets else defaultDocumentSets
        val limit = (arguments["limit"] as? Number)?.toInt() ?: 10
        val response = search.search(query, documentSets, limit)
        McpSchema.CallToolResult.builder()
            .addTextContent(mapper.writeValueAsString(response))
            .structuredContent(response)
            .build()
    } catch (error: Exception) {
        McpSchema.CallToolResult.builder()
            .addTextContent(error.message ?: "Search failed")
            .isError(true)
            .build()
    }
}
