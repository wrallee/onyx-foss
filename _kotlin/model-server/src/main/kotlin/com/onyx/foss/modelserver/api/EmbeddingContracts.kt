package com.onyx.foss.modelserver.api

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonValue

data class EmbedRequest(
    @JsonProperty("texts")
    val texts: List<String> = emptyList(),
    @JsonProperty("model_name")
    val modelName: String? = null,
    @JsonProperty("deployment_name")
    val deploymentName: String? = null,
    @JsonProperty("max_context_length")
    val maxContextLength: Int? = null,
    @JsonProperty("normalize_embeddings")
    val normalizeEmbeddings: Boolean? = null,
    @JsonProperty("api_key")
    val apiKey: String? = null,
    @JsonProperty("provider_type")
    val providerType: String? = null,
    @JsonProperty("text_type")
    val textType: EmbedTextType? = null,
    @JsonProperty("manual_query_prefix")
    val manualQueryPrefix: String? = null,
    @JsonProperty("manual_passage_prefix")
    val manualPassagePrefix: String? = null,
    @JsonProperty("api_url")
    val apiUrl: String? = null,
    @JsonProperty("api_version")
    val apiVersion: String? = null,
    @JsonProperty("reduced_dimension")
    val reducedDimension: Int? = null,
)

enum class EmbedTextType(@get:JsonValue val wireValue: String) {
    QUERY("query"),
    PASSAGE("passage");

    companion object {
        @JvmStatic
        @JsonCreator
        fun fromWireValue(value: String): EmbedTextType =
            entries.firstOrNull { it.wireValue == value }
                ?: throw IllegalArgumentException("Unsupported text_type: $value")
    }
}

data class EmbedResponse(
    @JsonProperty("embeddings")
    val embeddings: List<List<Float>>,
)

data class ApiError(
    @JsonProperty("code")
    val code: String,
    @JsonProperty("message")
    val message: String,
)

data class GpuStatusResponse(
    @JsonProperty("gpu_available")
    val gpuAvailable: Boolean,
    @JsonProperty("type")
    val type: String,
)

data class RerankRequest(
    @JsonProperty("query")
    val query: String? = null,
    @JsonProperty("documents")
    val documents: List<String> = emptyList(),
    @JsonProperty("model_name")
    val modelName: String? = null,
    @JsonProperty("provider_type")
    val providerType: String? = null,
)

data class RerankResponse(
    @JsonProperty("scores")
    val scores: List<Float>,
)

data class ValidatedEmbedRequest(
    val texts: List<String>,
    val modelName: String,
    val maxContextLength: Int,
    val normalizeEmbeddings: Boolean,
    val textType: EmbedTextType,
    val manualQueryPrefix: String?,
    val manualPassagePrefix: String?,
)

data class ModelServerStatusResponse(
    val embedding: com.onyx.foss.modelserver.runtime.EmbeddingRuntimeReadiness,
    val reranker: RerankerStatusResponse,
)

data class RerankerStatusResponse(
    val ready: Boolean,
    val code: String,
    val message: String,
)
