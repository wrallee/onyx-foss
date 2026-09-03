package com.onyx.foss.kotlin.rag

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query

class ScoreNormalizedFusionJoinerTest {

    @Test
    fun `fuses keyword and vector with min-max normalization and 50-50 weights`() {
        val doc1 = Document.builder().id("1").text("first").score(10.0).build()
        val doc2 = Document.builder().id("2").text("second").score(2.0).build()
        val doc3 = Document.builder().id("3").text("third").score(6.0).build()

        val keywordDocs = listOf(doc1, doc2) // min: 2.0, max: 10.0 -> doc1: 1.0, doc2: 0.0
        val vectorDocs = listOf(doc2, doc3) // min: 2.0, max: 6.0 -> doc2: 0.0, doc3: 1.0

        val joiner = ScoreNormalizedFusionJoiner(listOf(0.5, 0.5))
        val query = Query("test")
        val joined = joiner.join(mapOf(query to listOf(keywordDocs, vectorDocs)))

        // doc1: 0.5 * 1.0 + 0.5 * 0.0 = 0.5
        // doc2: 0.5 * 0.0 + 0.5 * 0.0 = 0.0
        // doc3: 0.5 * 0.0 + 0.5 * 1.0 = 0.5
        // tie-breaking by id: doc1 before doc3
        assertEquals(listOf("1", "3", "2"), joined.map { it.id })
        assertEquals(0.5, joined[0].score!!, 0.001)
        assertEquals(0.5, joined[1].score!!, 0.001)
        assertEquals(0.0, joined[2].score!!, 0.001)
    }

    @Test
    fun `handles empty or single list gracefully`() {
        val joiner = ScoreNormalizedFusionJoiner()
        val query = Query("test")

        assertEquals(emptyList<Document>(), joiner.join(emptyMap()))

        val singleDoc = Document.builder().id("1").text("one").score(5.0).build()
        val singleResult = joiner.join(mapOf(query to listOf(listOf(singleDoc))))
        assertEquals(listOf("1"), singleResult.map { it.id })
    }
}
