package com.onyx.foss.kotlin.rag

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query

class RrfDocumentJoinerTest {

    @Test
    fun `combines ranked lists using reciprocal rank fusion`() {
        val docA = Document.builder().id("A").text("A").build()
        val docB = Document.builder().id("B").text("B").build()
        val docC = Document.builder().id("C").text("C").build()

        // List 1: A (rank 1), B (rank 2)
        // List 2: B (rank 1), C (rank 2)
        // k = 50, weight = 1.0
        // A: 1/(50+1) = 1/51 ≈ 0.0196
        // B: 1/(50+2) + 1/(50+1) = 1/52 + 1/51 ≈ 0.0192 + 0.0196 = 0.0388
        // C: 1/(50+2) = 1/52 ≈ 0.0192
        // Rank order should be: B, A, C
        val list1 = listOf(docA, docB)
        val list2 = listOf(docB, docC)

        val joiner = RrfDocumentJoiner(listOf(1.0, 1.0), k = 50)
        val query = Query("rrf test")
        val joined = joiner.join(mapOf(query to listOf(list1, list2)))

        assertEquals(listOf("B", "A", "C"), joined.map { it.id })
    }
}
