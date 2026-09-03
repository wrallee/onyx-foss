package com.onyx.foss.kotlin.rag

import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query
import org.springframework.ai.rag.retrieval.join.DocumentJoiner

class ScoreNormalizedFusionJoiner(
    private val weights: List<Double> = listOf(0.5, 0.5),
) : DocumentJoiner {

    override fun join(documentsByQuery: Map<Query, List<List<Document>>>): List<Document> {
        val allLists = documentsByQuery.values.flatten()
        if (allLists.isEmpty()) return emptyList()
        if (allLists.size == 1) return allLists.first()

        val effectiveWeights = if (weights.size >= allLists.size) {
            weights.take(allLists.size)
        } else {
            List(allLists.size) { 1.0 / allLists.size }
        }

        val normalizedScoresPerList = allLists.map(::normalize)
        val docsById = linkedMapOf<String, Document>()
        allLists.forEach { list ->
            list.forEach { doc ->
                docsById.putIfAbsent(doc.id, doc)
            }
        }

        val fusedScores = mutableMapOf<String, Double>()
        docsById.keys.forEach { id ->
            var totalScore = 0.0
            allLists.indices.forEach { listIdx ->
                val weight = effectiveWeights[listIdx]
                val normScore = normalizedScoresPerList[listIdx][id] ?: 0.0
                totalScore += weight * normScore
            }
            fusedScores[id] = totalScore
        }

        return docsById.values.map { doc ->
            val score = fusedScores[doc.id] ?: 0.0
            doc.mutate().score(score).build()
        }.sortedWith(compareByDescending<Document> { it.score ?: 0.0 }.thenBy { it.id })
    }

    private fun normalize(docs: List<Document>): Map<String, Double> {
        if (docs.isEmpty()) return emptyMap()
        val scores = docs.mapNotNull { it.score }
        if (scores.isEmpty()) return docs.associate { it.id to 0.0 }
        val min = scores.minOrNull() ?: 0.0
        val max = scores.maxOrNull() ?: 0.0
        return docs.associate { doc ->
            val s = doc.score ?: 0.0
            val normalized = if (max == min) 1.0 else (s - min) / (max - min)
            doc.id to normalized
        }
    }
}
