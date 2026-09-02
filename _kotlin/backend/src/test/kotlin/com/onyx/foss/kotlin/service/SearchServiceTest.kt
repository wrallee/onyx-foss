package com.onyx.foss.kotlin.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.api.RankedCandidate
import com.onyx.foss.kotlin.api.RerankCandidate
import com.onyx.foss.kotlin.api.RerankCandidatesRequest
import com.onyx.foss.kotlin.api.RerankCandidatesResponse
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
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`

class SearchServiceTest {
    private val modelServer = mock(ModelServerClient::class.java)
    private val indexer = mock(OpenSearchIndexer::class.java)
    private val reranking = mock(RerankingService::class.java)
    private val documentSets = mock(DocumentSetRepository::class.java)
    private val properties = OnyxProperties(
        modelServer = OnyxProperties.ModelServer(
            embeddingDimension = 3,
            searchCandidates = 50,
            searchRerankCandidates = 30,
        ),
    )
    private val service = SearchService(properties, modelServer, indexer, reranking, documentSets)

    @Test
    fun `fuses retrieval and reranks only the configured candidate count`() {
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
        `when`(
            reranking.rerank(
                any(RerankCandidatesRequest::class.java)
                    ?: RerankCandidatesRequest("unused", listOf(RerankCandidate("unused", content = "unused"))),
            ),
        ).thenAnswer { invocation ->
            val request = invocation.getArgument<RerankCandidatesRequest>(0)
            RerankCandidatesResponse(
                reranked = true,
                candidates = request.candidates.mapIndexed { index, candidate ->
                    RankedCandidate(
                        candidate.id,
                        candidate.title,
                        candidate.content,
                        candidate.retrievalScore,
                        1.0 - index / 100.0,
                        index,
                    )
                },
            )
        }

        val response = service.search("deployment guide", listOf("Engineering", "Operations"), 10)

        val request = ArgumentCaptor.forClass(RerankCandidatesRequest::class.java)
        verify(reranking).rerank(
            request.capture()
                ?: RerankCandidatesRequest("unused", listOf(RerankCandidate("unused", content = "unused"))),
        )
        assertThat(request.value.candidates).hasSize(30)
        assertThat(response.results).hasSize(10)
        assertThat(response.reranked).isTrue()
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

    private fun candidate(id: String) = SearchCandidate(
        id = id,
        sourceDocumentId = "doc-$id",
        chunkId = 0,
        title = "Title $id",
        content = "Content $id",
        link = "https://example.test/$id",
        metadata = jacksonObjectMapper().createObjectNode(),
        retrievalScore = 1.0,
    )
}
