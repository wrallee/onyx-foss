package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.service.SearchResponse
import com.onyx.foss.kotlin.service.SearchService
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`

class McpSearchToolTest {
    private val search = mock(SearchService::class.java)
    private val tool = McpSearchTool(search, jacksonObjectMapper())

    @Test
    fun `search tool forwards document sets and limit`() {
        val response = SearchResponse(reranked = true, results = emptyList())
        `when`(search.search("deployment guide", listOf("Engineering", "Operations"), 7)).thenReturn(response)

        val result = tool.call(
            mapOf(
                "query" to "deployment guide",
                "document_sets" to listOf("Engineering", "Operations"),
                "limit" to 7,
            ),
        )

        verify(search).search("deployment guide", listOf("Engineering", "Operations"), 7)
        assertThat(result.isError() == true).isFalse()
        assertThat(result.structuredContent()).isEqualTo(response)
        assertThat(tool.definition().description()).contains("3 search calls")
    }
}
