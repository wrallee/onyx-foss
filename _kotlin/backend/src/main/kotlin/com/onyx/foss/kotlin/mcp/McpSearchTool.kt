package com.onyx.foss.kotlin.mcp

import tools.jackson.core.type.TypeReference
import tools.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.service.SearchResponse
import com.onyx.foss.kotlin.service.SearchResult
import com.onyx.foss.kotlin.service.SearchService
import com.onyx.foss.kotlin.service.SearchType
import io.modelcontextprotocol.spec.McpSchema
import org.springframework.stereotype.Component

@Component
class McpSearchTool(
    private val search: SearchService,
    private val mapper: ObjectMapper,
) {
    fun searchDefinition(name: String = TOOL_SEARCH_INDEXED_DOCUMENTS): McpSchema.Tool =
        McpSchema.Tool.builder(name, SEARCH_INPUT_SCHEMA)
            .description("Search the user's knowledge base indexed in Onyx. Supports hybrid, keyword, and semantic search types.")
            .build()

    fun fusionDefinition(): McpSchema.Tool =
        McpSchema.Tool.builder(TOOL_WEIGHTED_RRF, FUSION_INPUT_SCHEMA)
            .description("Merge multiple ranked result lists using weighted Reciprocal Rank Fusion (RRF). Intelligently combines rankings from decomposed queries.")
            .build()

    fun callSearch(
        arguments: Map<String, Any>,
        defaultDocumentSets: List<String> = emptyList(),
    ): McpSchema.CallToolResult = try {
        val query = arguments["query"] as? String ?: throw IllegalArgumentException("query must be a string")
        val requestedSets = ((arguments["document_set_names"] ?: arguments["document_sets"]) as? List<*>)?.map {
            it as? String ?: throw IllegalArgumentException("document_sets must contain strings")
        }
        val documentSets = if (!requestedSets.isNullOrEmpty()) requestedSets else defaultDocumentSets
        val limit = (arguments["limit"] as? Number)?.toInt() ?: 10
        val searchTypeStr = arguments["search_type"] as? String
        val searchType = SearchType.fromString(searchTypeStr)

        val response = search.search(query, documentSets, limit, searchType)
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

    fun callFusion(arguments: Map<String, Any>): McpSchema.CallToolResult = try {
        val rankedResultsRaw = arguments["ranked_results"] as? List<*>
            ?: throw IllegalArgumentException("ranked_results must be a list of result lists")

        val typeRef = object : TypeReference<List<Map<String, Any?>>>() {}
        val rankedResults: List<List<Map<String, Any?>>> = rankedResultsRaw.map { list ->
            mapper.convertValue(list, typeRef)
        }

        val weightsRaw = arguments["weights"] as? List<*>
        val weights: List<Double> = if (weightsRaw != null) {
            weightsRaw.map { (it as? Number)?.toDouble() ?: 1.0 }
        } else {
            List(rankedResults.size) { 1.0 }
        }

        val k = (arguments["k"] as? Number)?.toInt() ?: SearchService.DEFAULT_RRF_K

        val merged = search.weightedReciprocalRankFusion(
            rankedResults = rankedResults,
            weights = weights,
            idExtractor = { item -> extractItemId(item) },
            k = k,
        )

        val response = mapOf("results" to merged)
        McpSchema.CallToolResult.builder()
            .addTextContent(mapper.writeValueAsString(response))
            .structuredContent(response)
            .build()
    } catch (error: Exception) {
        McpSchema.CallToolResult.builder()
            .addTextContent(error.message ?: "Weighted RRF failed")
            .isError(true)
            .build()
    }

    private fun extractItemId(item: Map<String, Any?>): String {
        val docId = item["sourceDocumentId"] ?: item["source_document_id"] ?: item["id"] ?: item["link"] ?: item.hashCode().toString()
        val chunkId = item["chunkId"] ?: item["chunk_id"] ?: ""
        return "${docId}_$chunkId"
    }

    companion object {
        const val TOOL_SEARCH_INDEXED_DOCUMENTS = "search_indexed_documents"
        const val TOOL_SEARCH_LEGACY = "search"
        const val TOOL_WEIGHTED_RRF = "weighted_reciprocal_rank_fusion"

        val SEARCH_INPUT_SCHEMA: Map<String, Any> = mapOf(
            "type" to "object",
            "properties" to mapOf(
                "query" to mapOf("type" to "string", "minLength" to 1),
                "source_types" to mapOf(
                    "type" to "array",
                    "items" to mapOf("type" to "string"),
                ),
                "document_set_names" to mapOf(
                    "type" to "array",
                    "items" to mapOf("type" to "string", "minLength" to 1),
                    "uniqueItems" to true,
                ),
                "document_sets" to mapOf(
                    "type" to "array",
                    "items" to mapOf("type" to "string", "minLength" to 1),
                    "uniqueItems" to true,
                ),
                "time_cutoff" to mapOf("type" to "string"),
                "search_type" to mapOf(
                    "type" to "string",
                    "enum" to listOf("hybrid", "keyword", "semantic"),
                    "default" to "hybrid",
                ),
                "limit" to mapOf("type" to "integer", "minimum" to 1, "maximum" to 20, "default" to 10),
            ),
            "required" to listOf("query"),
            "additionalProperties" to false,
        )

        val FUSION_INPUT_SCHEMA: Map<String, Any> = mapOf(
            "type" to "object",
            "properties" to mapOf(
                "ranked_results" to mapOf(
                    "type" to "array",
                    "items" to mapOf(
                        "type" to "array",
                        "items" to mapOf("type" to "object"),
                    ),
                    "description" to "List of ranked result lists to fuse together.",
                ),
                "weights" to mapOf(
                    "type" to "array",
                    "items" to mapOf("type" to "number"),
                    "description" to "Optional weights for each ranked list. Defaults to 1.0 for each list.",
                ),
                "k" to mapOf(
                    "type" to "integer",
                    "default" to 50,
                    "description" to "RRF constant parameter k (default: 50).",
                ),
            ),
            "required" to listOf("ranked_results"),
            "additionalProperties" to false,
        )
    }
}

