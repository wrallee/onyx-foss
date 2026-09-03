package com.onyx.foss.kotlin.service

import tools.jackson.databind.JsonNode
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.ingestion.ModelServerClient
import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import com.onyx.foss.kotlin.ingestion.SearchCandidate
import com.onyx.foss.kotlin.rag.RrfDocumentJoiner
import com.onyx.foss.kotlin.rag.ScoreNormalizedFusionJoiner
import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query
import org.springframework.stereotype.Service

@Service
class SearchService(
    private val properties: OnyxProperties,
    private val modelServer: ModelServerClient,
    private val indexer: OpenSearchIndexer,
    private val documentSetRepository: DocumentSetRepository,
) {
    private val fusionJoiner = ScoreNormalizedFusionJoiner(listOf(KEYWORD_WEIGHT, VECTOR_WEIGHT))

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
        val itemById = mutableMapOf<String, T>()
        val docLists = rankedResults.map { list ->
            list.map { item ->
                val id = idExtractor(item)
                itemById.putIfAbsent(id, item)
                Document.builder().id(id).text(id).build()
            }
        }
        val joiner = RrfDocumentJoiner(weights, k)
        val joined = joiner.join(mapOf(Query("rrf") to docLists))
        return joined.map { itemById.getValue(it.id) }
    }

    private fun fuse(keyword: List<SearchCandidate>, vector: List<SearchCandidate>): List<SearchCandidate> {
        val keywordDocs = keyword.map { toDocument(it) }
        val vectorDocs = vector.map { toDocument(it) }
        val joined = fusionJoiner.join(mapOf(Query("hybrid") to listOf(keywordDocs, vectorDocs)))
        val candidatesById = (keyword + vector).associateBy { it.id }
        return joined.mapNotNull { doc ->
            candidatesById[doc.id]?.copy(retrievalScore = doc.score ?: 0.0)
        }
    }

    private fun toDocument(candidate: SearchCandidate): Document =
        Document.builder()
            .id(candidate.id)
            .text(candidate.content)
            .score(candidate.retrievalScore)
            .build()

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
    val metadata: JsonNode?,
    val retrievalScore: Double?,
)
