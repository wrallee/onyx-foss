package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.DocumentSetEntity
import com.onyx.foss.kotlin.domain.DocumentSetRepository
import com.onyx.foss.kotlin.ingestion.ModelServerClient
import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import com.onyx.foss.kotlin.ingestion.SearchCandidate
import com.onyx.foss.kotlin.ingestion.SearchCandidateResults
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.mock
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`

class SearchServiceTest {
    private val modelServer = mock(ModelServerClient::class.java)
    private val indexer = mock(OpenSearchIndexer::class.java)
    private val documentSets = mock(DocumentSetRepository::class.java)
    private val properties = OnyxProperties(
        modelServer = OnyxProperties.ModelServer(
            embeddingDimension = 3,
            searchCandidates = 50,
        ),
    )
    private val service = SearchService(properties, modelServer, indexer, documentSets)

    @Test
    fun `fuses retrieval and returns top candidates up to limit`() {
        val keyword = (0 until 25).map { candidate("keyword-$it") }
        val vector = (15 until 40).map { candidate("keyword-$it") }
        `when`(documentSets.findAllByNameIn(listOf("Engineering", "Operations"))).thenReturn(
            listOf(DocumentSetEntity(name = "Engineering"), DocumentSetEntity(name = "Operations")),
        )
        `when`(modelServer.embedQuery("deployment guide")).thenReturn(listOf(0.1, 0.2, 0.3))
        `when`(
            indexer.searchCandidates(
                "deployment guide",
                listOf(0.1, 0.2, 0.3),
                listOf("Engineering", "Operations"),
                50,
            ),
        ).thenReturn(SearchCandidateResults(keyword, vector))

        val response = service.search("deployment guide", listOf("Engineering", "Operations"), 10)

        assertThat(response.results).hasSize(10)
        assertThat(response.results.first().sourceDocumentId).isNotEmpty()
    }

    @Test
    fun `fuses retrieval with min-max normalization and weighted score merge`() {
        val candidateA = candidate("doc-a", score = 10.0)
        val candidateB = candidate("doc-b", score = 0.0)
        val candidateC = candidate("doc-c", score = 5.0)

        val keyword = listOf(candidateA, candidateC, candidateB) // scores: 10, 5, 0 -> norm: 1.0, 0.5, 0.0
        val vectorA = candidate("doc-a", score = 20.0)
        val vectorB = candidate("doc-b", score = 10.0)
        val vector = listOf(vectorA, vectorB) // scores: 20, 10 -> norm: 1.0, 0.0 (doc-c has 0.0)

        // Expected final scores:
        // doc-a: 0.5 * 1.0 + 0.5 * 1.0 = 1.0
        // doc-c: 0.5 * 0.5 + 0.5 * 0.0 = 0.25
        // doc-b: 0.5 * 0.0 + 0.5 * 0.0 = 0.0

        `when`(documentSets.findAllByNameIn(emptyList())).thenReturn(emptyList())
        `when`(modelServer.embedQuery("test query")).thenReturn(listOf(0.1, 0.2, 0.3))
        `when`(
            indexer.searchCandidates("test query", listOf(0.1, 0.2, 0.3), emptyList(), 50),
        ).thenReturn(SearchCandidateResults(keyword, vector))

        val response = service.search("test query", emptyList(), 10)

        assertThat(response.results).hasSize(3)
        assertThat(response.results[0].sourceDocumentId).isEqualTo("doc-doc-a")
        assertThat(response.results[0].retrievalScore).isEqualTo(1.0)
        assertThat(response.results[1].sourceDocumentId).isEqualTo("doc-doc-c")
        assertThat(response.results[1].retrievalScore).isEqualTo(0.25)
        assertThat(response.results[2].sourceDocumentId).isEqualTo("doc-doc-b")
        assertThat(response.results[2].retrievalScore).isEqualTo(0.0)
    }

    @Test
    fun `rejects unknown document sets before model calls`() {
        `when`(documentSets.findAllByNameIn(listOf("Missing"))).thenReturn(emptyList())

        val error = assertThrows<IllegalArgumentException> {
            service.search("query", listOf("Missing"), 10)
        }

        assertThat(error.message).contains("Missing")
        verifyNoInteractions(modelServer)
    }

    @Test
    fun `weightedReciprocalRankFusion combines lists with weights and tie breaking`() {
        // Doc A: in list 1 (rank 1), in list 2 (rank 2)
        // Doc B: in list 1 (rank 2)
        // Doc C: in list 2 (rank 1)
        val list1 = listOf("doc-a", "doc-b")
        val list2 = listOf("doc-c", "doc-a")
        val weights = listOf(1.2, 1.0)
        val k = 50

        // Doc A: 1.2 / (50 + 1) + 1.0 / (50 + 2) = 1.2/51 + 1.0/52 = 0.02353 + 0.01923 = 0.04276
        // Doc C: 1.0 / (50 + 1) = 0.01961
        // Doc B: 1.2 / (50 + 2) = 0.02308
        // Expected order: Doc A, Doc B, Doc C

        val merged = service.weightedReciprocalRankFusion(
            rankedResults = listOf(list1, list2),
            weights = weights,
            idExtractor = { it },
            k = k,
        )

        assertThat(merged).containsExactly("doc-a", "doc-b", "doc-c")
    }

    @Test
    fun `search returns only keyword results when search_type is KEYWORD`() {
        val keyword = listOf(candidate("k1", 10.0), candidate("k2", 5.0))
        val vector = listOf(candidate("v1", 20.0))

        `when`(documentSets.findAllByNameIn(emptyList())).thenReturn(emptyList())
        `when`(modelServer.embedQuery("query")).thenReturn(listOf(0.1, 0.2, 0.3))
        `when`(indexer.searchCandidates("query", listOf(0.1, 0.2, 0.3), emptyList(), 50))
            .thenReturn(SearchCandidateResults(keyword, vector))

        val response = service.search("query", emptyList(), 10, SearchType.KEYWORD)

        assertThat(response.results).hasSize(2)
        assertThat(response.results.map { it.sourceDocumentId }).containsExactly("doc-k1", "doc-k2")
    }

    private fun candidate(id: String, score: Double = 1.0) = SearchCandidate(
        id = id,
        sourceDocumentId = "doc-$id",
        chunkId = 0,
        title = "Title $id",
        content = "Content $id",
        link = "https://example.test/$id",
        metadata = jacksonObjectMapper().createObjectNode(),
        retrievalScore = score,
    )
}
