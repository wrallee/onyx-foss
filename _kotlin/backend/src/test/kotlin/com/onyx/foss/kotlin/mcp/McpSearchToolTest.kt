package com.onyx.foss.kotlin.mcp

import tools.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.service.DocumentContextResponse
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
    fun `search tool forwards source types and time cutoff`() {
        val response = SearchResponse(results = emptyList())
        val cutoff = java.time.Instant.parse("2026-01-01T00:00:00Z")
        `when`(
            search.search(
                "deployment guide",
                emptyList(),
                10,
                SearchType.HYBRID,
                listOf("jira", "github"),
                cutoff,
            ),
        ).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "deployment guide",
                "source_types" to listOf("jira", "github"),
                "time_cutoff" to "2026-01-01T00:00:00Z",
            ),
        )

        verify(search).search(
            "deployment guide",
            emptyList(),
            10,
            SearchType.HYBRID,
            listOf("jira", "github"),
            cutoff,
        )
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `search tool skips unknown source types instead of failing`() {
        val response = SearchResponse(results = emptyList())
        `when`(
            search.search("deployment guide", emptyList(), 10, SearchType.HYBRID, listOf("jira"), null),
        ).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "deployment guide",
                "source_types" to listOf("jira", "not-a-real-source"),
            ),
        )

        verify(search).search("deployment guide", emptyList(), 10, SearchType.HYBRID, listOf("jira"), null)
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `search tool ignores an unparseable time cutoff instead of failing`() {
        val response = SearchResponse(results = emptyList())
        `when`(
            search.search("deployment guide", emptyList(), 10, SearchType.HYBRID, emptyList(), null),
        ).thenReturn(response)

        val result = tool.callSearch(
            mapOf(
                "query" to "deployment guide",
                "time_cutoff" to "not-a-date",
            ),
        )

        verify(search).search("deployment guide", emptyList(), 10, SearchType.HYBRID, emptyList(), null)
        assertThat(result.isError() == true).isFalse()
    }

    @Test
    fun `context tool forwards document, chunk and window arguments`() {
        val response = DocumentContextResponse("doc-1", emptyList())
        `when`(search.getDocumentContext("doc-1", 5, 1, 3)).thenReturn(response)

        val result = tool.callGetDocumentContext(
            mapOf(
                "source_document_id" to "doc-1",
                "chunk_id" to 5,
                "chunks_above" to 1,
                "chunks_below" to 3,
            ),
        )

        verify(search).getDocumentContext("doc-1", 5, 1, 3)
        assertThat(result.isError() == true).isFalse()
        assertThat(tool.contextDefinition().name()).isEqualTo("get_document_context")
    }

    @Test
    fun `context tool uses default window when not specified`() {
        val response = DocumentContextResponse("doc-1", emptyList())
        `when`(
            search.getDocumentContext(
                "doc-1",
                5,
                SearchService.DEFAULT_CONTEXT_CHUNKS,
                SearchService.DEFAULT_CONTEXT_CHUNKS,
            ),
        ).thenReturn(response)

        val result = tool.callGetDocumentContext(mapOf("source_document_id" to "doc-1", "chunk_id" to 5))

        verify(search).getDocumentContext(
            "doc-1",
            5,
            SearchService.DEFAULT_CONTEXT_CHUNKS,
            SearchService.DEFAULT_CONTEXT_CHUNKS,
        )
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
