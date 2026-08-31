package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.annotation.JsonProperty
import com.onyx.foss.kotlin.api.RankedCandidate
import com.onyx.foss.kotlin.api.RerankCandidate
import com.onyx.foss.kotlin.api.RerankCandidatesRequest
import com.onyx.foss.kotlin.api.RerankCandidatesResponse
import com.onyx.foss.kotlin.config.OnyxProperties
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

@Service
class RerankingService(
    private val properties: OnyxProperties,
    private val clientBuilder: WebClient.Builder,
) {
    fun rerank(request: RerankCandidatesRequest): RerankCandidatesResponse {
        val config = properties.modelServer
        require(request.candidates.size <= config.rerankerMaxDocuments) {
            "candidates exceeds maximum ${config.rerankerMaxDocuments}"
        }
        if (!config.rerankerEnabled) {
            return fallback(request.candidates, "Reranking is disabled.")
        }

        return try {
            val response = clientBuilder.build().post()
                .uri(config.baseUrl.trimEnd('/') + "/encoder/cross-encoder-scores")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(
                    RerankerModelRequest(
                        query = request.query,
                        documents = request.candidates.map { it.content },
                        modelName = config.rerankerModelName,
                    ),
                )
                .retrieve()
                .bodyToMono(RerankerModelResponse::class.java)
                .block(Duration.ofMillis(config.rerankerTimeoutMs))
                ?: error("Model server returned no reranker response")
            require(response.scores.size == request.candidates.size) {
                "Reranker score count does not match candidate count"
            }
            val ranked = request.candidates.mapIndexed { index, candidate ->
                candidate.toRanked(index, response.scores[index])
            }.sortedByDescending { it.rerankScore }
            RerankCandidatesResponse(reranked = true, candidates = ranked)
        } catch (error: Exception) {
            if (!config.rerankerFallbackOnError) throw error
            fallback(request.candidates, "Reranker unavailable: ${error.message}")
        }
    }

    private fun fallback(
        candidates: List<RerankCandidate>,
        warning: String,
    ): RerankCandidatesResponse = RerankCandidatesResponse(
        reranked = false,
        candidates = candidates.mapIndexed { index, candidate ->
            candidate.toRanked(index, null)
        },
        warning = warning,
    )

    private fun RerankCandidate.toRanked(index: Int, score: Double?) = RankedCandidate(
        id = id,
        title = title,
        content = content,
        retrievalScore = retrievalScore,
        rerankScore = score,
        originalIndex = index,
    )
}

private data class RerankerModelRequest(
    val query: String,
    val documents: List<String>,
    @JsonProperty("model_name") val modelName: String,
    @JsonProperty("provider_type") val providerType: String? = null,
)

private data class RerankerModelResponse(val scores: List<Double>)
