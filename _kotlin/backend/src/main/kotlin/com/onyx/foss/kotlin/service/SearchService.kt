package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.api.RerankCandidate
import com.onyx.foss.kotlin.api.RerankCandidatesRequest
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.ingestion.ModelServerClient
import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import com.onyx.foss.kotlin.ingestion.SearchCandidate
import org.springframework.stereotype.Service

@Service
class SearchService(
    private val properties: OnyxProperties,
    private val modelServer: ModelServerClient,
    private val indexer: OpenSearchIndexer,
    private val reranking: RerankingService,
    private val documentSetRepository: DocumentSetRepository,
) {
    fun search(query: String, documentSets: List<String> = emptyList(), limit: Int = 10): SearchResponse {
        require(query.isNotBlank()) { "query must not be blank" }
        require(limit in 1..MAX_RESULTS) { "limit must be between 1 and $MAX_RESULTS" }
        val selectedSets = documentSets.map(String::trim).distinct()
        require(selectedSets.none(String::isBlank)) { "document set names must not be blank" }
        if (selectedSets.isNotEmpty()) {
            val known = documentSetRepository.findAllByNameIn(selectedSets).mapTo(mutableSetOf()) { it.name }
            val unknown = selectedSets.filterNot(known::contains)
            require(unknown.isEmpty()) { "Unknown document sets: ${unknown.joinToString()}" }
        }

        val config = properties.modelServer
        val candidates = indexer.searchCandidates(
            query,
            modelServer.embedQuery(query),
            selectedSets,
            config.searchCandidates,
        )
        val fused = fuse(candidates.keyword, candidates.vector)
            .take(maxOf(limit, config.searchRerankCandidates).coerceAtMost(config.rerankerMaxDocuments))
        if (fused.isEmpty()) return SearchResponse(false, emptyList())

        val sources = fused.associateBy(SearchCandidate::id)
        val ranked = reranking.rerank(
            RerankCandidatesRequest(
                query,
                fused.map { candidate ->
                    RerankCandidate(
                        candidate.id,
                        candidate.title,
                        candidate.content,
                        candidate.retrievalScore,
                    )
                },
            ),
        )
        return SearchResponse(
            reranked = ranked.reranked,
            results = ranked.candidates.take(limit).map { candidate ->
                val source = requireNotNull(sources[candidate.id])
                SearchResult(
                    source.sourceDocumentId,
                    source.chunkId,
                    source.title,
                    source.content,
                    source.link,
                    source.metadata,
                    candidate.retrievalScore,
                    candidate.rerankScore,
                )
            },
            warning = ranked.warning,
        )
    }

    private fun fuse(keyword: List<SearchCandidate>, vector: List<SearchCandidate>): List<SearchCandidate> {
        val sources = linkedMapOf<String, SearchCandidate>()
        val scores = mutableMapOf<String, Double>()
        listOf(keyword, vector).forEach { results ->
            results.forEachIndexed { index, candidate ->
                sources.putIfAbsent(candidate.id, candidate)
                scores[candidate.id] = scores.getOrDefault(candidate.id, 0.0) + 1.0 / (RRF_K + index + 1)
            }
        }
        return sources.values.map { it.copy(retrievalScore = requireNotNull(scores[it.id])) }
            .sortedWith(compareByDescending<SearchCandidate> { it.retrievalScore }.thenBy { it.id })
    }

    private companion object {
        const val MAX_RESULTS = 20
        const val RRF_K = 60.0
    }
}

data class SearchResponse(
    val reranked: Boolean,
    val results: List<SearchResult>,
    val warning: String? = null,
)

data class SearchResult(
    val sourceDocumentId: String,
    val chunkId: Int,
    val title: String,
    val content: String,
    val link: String?,
    val metadata: JsonNode,
    val retrievalScore: Double?,
    val rerankScore: Double?,
)
