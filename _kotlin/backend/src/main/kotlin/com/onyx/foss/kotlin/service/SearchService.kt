package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.databind.JsonNode
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
    private val documentSetRepository: DocumentSetRepository,
) {
    @JvmOverloads
    fun search(
        query: String,
        documentSets: List<String> = emptyList(),
        limit: Int = 10,
        searchType: SearchType = SearchType.HYBRID,
    ): SearchResponse {
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
        val fused = when (searchType) {
            SearchType.HYBRID -> fuse(candidates.keyword, candidates.vector)
            SearchType.KEYWORD -> {
                val normKeyword = normalize(candidates.keyword)
                candidates.keyword.map { it.copy(retrievalScore = normKeyword[it.id] ?: 0.0) }
                    .sortedWith(compareByDescending<SearchCandidate> { it.retrievalScore ?: 0.0 }.thenBy { it.id })
            }
            SearchType.SEMANTIC -> {
                val normVector = normalize(candidates.vector)
                candidates.vector.map { it.copy(retrievalScore = normVector[it.id] ?: 0.0) }
                    .sortedWith(compareByDescending<SearchCandidate> { it.retrievalScore ?: 0.0 }.thenBy { it.id })
            }
        }
        if (fused.isEmpty()) return SearchResponse(emptyList())

        val results = fused.take(limit).map { candidate ->
            SearchResult(
                sourceDocumentId = candidate.sourceDocumentId,
                chunkId = candidate.chunkId,
                title = candidate.title,
                content = candidate.content,
                link = candidate.link,
                metadata = candidate.metadata,
                retrievalScore = candidate.retrievalScore,
            )
        }
        return SearchResponse(results = results)
    }

    @JvmOverloads
    fun <T> weightedReciprocalRankFusion(
        rankedResults: List<List<T>>,
        weights: List<Double>,
        idExtractor: (T) -> String,
        k: Int = DEFAULT_RRF_K,
    ): List<T> {
        require(rankedResults.size == weights.size) {
            "Number of ranked results (${rankedResults.size}) must match number of weights (${weights.size})"
        }
        val rrfScores = mutableMapOf<String, Double>()
        val idToItem = mutableMapOf<String, T>()
        val idToSourceIndex = mutableMapOf<String, Int>()
        val idToSourceRank = mutableMapOf<String, Int>()

        rankedResults.forEachIndexed { sourceIdx, resultList ->
            val weight = weights[sourceIdx]
            resultList.forEachIndexed { index, item ->
                val rank = index + 1
                val itemId = idExtractor(item)
                rrfScores[itemId] = (rrfScores[itemId] ?: 0.0) + (weight / (k + rank))
                if (itemId !in idToItem) {
                    idToItem[itemId] = item
                    idToSourceIndex[itemId] = sourceIdx
                    idToSourceRank[itemId] = rank
                }
            }
        }

        return rrfScores.keys.sortedWith(
            compareByDescending<String> { rrfScores[it] ?: 0.0 }
                .thenBy { idToSourceRank[it] ?: Int.MAX_VALUE }
                .thenBy { idToSourceIndex[it] ?: Int.MAX_VALUE },
        ).map { idToItem.getValue(it) }
    }

    private fun fuse(keyword: List<SearchCandidate>, vector: List<SearchCandidate>): List<SearchCandidate> {
        val normKeyword = normalize(keyword)
        val normVector = normalize(vector)
        val sources = linkedMapOf<String, SearchCandidate>()
        (keyword + vector).forEach { candidate ->
            sources.putIfAbsent(candidate.id, candidate)
        }
        return sources.values.map { candidate ->
            val kScore = normKeyword[candidate.id] ?: 0.0
            val vScore = normVector[candidate.id] ?: 0.0
            candidate.copy(retrievalScore = KEYWORD_WEIGHT * kScore + VECTOR_WEIGHT * vScore)
        }.sortedWith(compareByDescending<SearchCandidate> { it.retrievalScore ?: 0.0 }.thenBy { it.id })
    }

    private fun normalize(candidates: List<SearchCandidate>): Map<String, Double> {
        if (candidates.isEmpty()) return emptyMap()
        val scores = candidates.mapNotNull { it.retrievalScore }
        if (scores.isEmpty()) return candidates.associate { it.id to 0.0 }
        val min = scores.minOrNull() ?: 0.0
        val max = scores.maxOrNull() ?: 0.0
        return candidates.associate { candidate ->
            val score = candidate.retrievalScore ?: 0.0
            val normalized = if (max == min) 1.0 else (score - min) / (max - min)
            candidate.id to normalized
        }
    }

    companion object {
        const val MAX_RESULTS = 20
        const val KEYWORD_WEIGHT = 0.5
        const val VECTOR_WEIGHT = 0.5
        const val DEFAULT_RRF_K = 50
    }
}

enum class SearchType {
    HYBRID,
    KEYWORD,
    SEMANTIC;

    companion object {
        fun fromString(value: String?): SearchType {
            if (value.isNullOrBlank()) return HYBRID
            return entries.firstOrNull { it.name.equals(value.trim(), ignoreCase = true) }
                ?: throw IllegalArgumentException("Unsupported search_type: $value. Supported types: hybrid, keyword, semantic")
        }
    }
}

data class SearchResponse(
    val results: List<SearchResult>,
)

data class SearchResult(
    val sourceDocumentId: String,
    val chunkId: Int,
    val title: String,
    val content: String,
    val link: String?,
    val metadata: JsonNode,
    val retrievalScore: Double?,
)

