package com.onyx.foss.kotlin.rag

import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query
import org.springframework.ai.rag.retrieval.join.DocumentJoiner

class RrfDocumentJoiner(
    private val weights: List<Double>? = null,
    private val k: Int = DEFAULT_RRF_K,
) : DocumentJoiner {

    override fun join(documentsByQuery: Map<Query, List<List<Document>>>): List<Document> {
        val rankedResults = documentsByQuery.values.flatten()
        if (rankedResults.isEmpty()) return emptyList()

        val effectiveWeights = weights ?: List(rankedResults.size) { 1.0 }
        require(rankedResults.size == effectiveWeights.size) {
            "Number of ranked results (${rankedResults.size}) must match number of weights (${effectiveWeights.size})"
        }

        val rrfScores = mutableMapOf<String, Double>()
        val idToDoc = mutableMapOf<String, Document>()
        val idToSourceIndex = mutableMapOf<String, Int>()
        val idToSourceRank = mutableMapOf<String, Int>()

        rankedResults.forEachIndexed { sourceIdx, resultList ->
            val weight = effectiveWeights[sourceIdx]
            resultList.forEachIndexed { index, doc ->
                val rank = index + 1
                val docId = doc.id
                rrfScores[docId] = (rrfScores[docId] ?: 0.0) + (weight / (k + rank))
                if (docId !in idToDoc) {
                    idToDoc[docId] = doc
                    idToSourceIndex[docId] = sourceIdx
                    idToSourceRank[docId] = rank
                }
            }
        }

        return rrfScores.keys.sortedWith(
            compareByDescending<String> { rrfScores[it] ?: 0.0 }
                .thenBy { idToSourceRank[it] ?: Int.MAX_VALUE }
                .thenBy { idToSourceIndex[it] ?: Int.MAX_VALUE },
        ).map { id ->
            val doc = idToDoc.getValue(id)
            doc.mutate().score(rrfScores[id]).build()
        }
    }

    companion object {
        const val DEFAULT_RRF_K = 50
    }
}
