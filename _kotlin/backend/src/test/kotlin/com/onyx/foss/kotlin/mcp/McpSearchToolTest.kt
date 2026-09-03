package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.service.SearchResponse
import com.onyx.foss.kotlin.service.SearchService
import com.onyx.foss.kotlin.service.SearchType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`

class McpSearchToolTest {
    private lateinit var search: SearchService
    private lateinit var tool: McpSearchTool

    @BeforeEach
    fun setUp() {
        search = mock(SearchService::class.java)
        tool = McpSearchTool(search, jacksonObjectMapper())
    }

    @Test
    fun `search tool forwards document sets and limit`() {
        val response = SearchResponse(results = emptyList())
        `when`(search.search("deployment guide", listOf("Engineering", "Operations"), 7, SearchType.HYBRID)).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "deployment guide",
                "document_sets" to listOf("Engineering", "Operations"),
                "limit" to 7,
            ),
        )

        verify(search).search("deployment guide", listOf("Engineering", "Operations"), 7, SearchType.HYBRID)
        assertThat(result.isError() == true).isFalse()
        assertThat(tool.searchDefinition().name()).isEqualTo("search_indexed_documents")
    }

    @Test
    fun `search tool handles search_type and document_set_names`() {
        val response = SearchResponse(results = emptyList())
        `when`(search.search("auth error", listOf("Backend"), 5, SearchType.KEYWORD)).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "auth error",
                "document_set_names" to listOf("Backend"),
                "search_type" to "keyword",
                "limit" to 5,
            ),
        )

        verify(search).search("auth error", listOf("Backend"), 5, SearchType.KEYWORD)
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `search tool uses default document sets when none provided in arguments`() {
        val response = SearchResponse(results = emptyList())
        `when`(search.search("deployment guide", listOf("DefaultSet"), 10, SearchType.HYBRID)).thenReturn(response)

        val result = tool.callSearch(
            mapOf("query" to "deployment guide"),
            listOf("DefaultSet"),
        )

        verify(search).search("deployment guide", listOf("DefaultSet"), 10, SearchType.HYBRID)
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `search tool argument document sets override defaults`() {
        val response = SearchResponse(results = emptyList())
        `when`(search.search("deployment guide", listOf("CustomSet"), 10, SearchType.HYBRID)).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "deployment guide",
                "document_sets" to listOf("CustomSet"),
            ),
            listOf("DefaultSet"),
        )

        verify(search).search("deployment guide", listOf("CustomSet"), 10, SearchType.HYBRID)
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `fusion tool combines ranked results using weighted RRF`() {
        val list1 = listOf(
            mapOf("sourceDocumentId" to "doc-1", "chunkId" to 0, "title" to "Doc 1"),
            mapOf("sourceDocumentId" to "doc-2", "chunkId" to 0, "title" to "Doc 2"),
        )
        val list2 = listOf(
            mapOf("sourceDocumentId" to "doc-2", "chunkId" to 0, "title" to "Doc 2"),
            mapOf("sourceDocumentId" to "doc-3", "chunkId" to 0, "title" to "Doc 3"),
        )
        org.mockito.Mockito.doCallRealMethod().`when`(search).weightedReciprocalRankFusion<Map<String, Any?>>(
            org.mockito.ArgumentMatchers.anyList(),
            org.mockito.ArgumentMatchers.anyList(),
            anyNonNull(),
            org.mockito.ArgumentMatchers.anyInt(),
        )

        val result = tool.callFusion(
            mapOf(
                "ranked_results" to listOf(list1, list2),
                "weights" to listOf(1.0, 1.0),
                "k" to 50,
            ),
        )

        assertThat(result.isError() == true).isFalse()
        assertThat(result.content()).isNotEmpty()
    }

    private fun <T> anyNonNull(): T {
        org.mockito.Mockito.any<T>()
        @Suppress("UNCHECKED_CAST")
        return (({ _: Any? -> "" }) as Any) as T
    }
}
