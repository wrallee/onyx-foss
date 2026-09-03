package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.service.SearchResponse
import com.onyx.foss.kotlin.service.SearchService
import io.modelcontextprotocol.client.McpClient
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.spec.McpSchema
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.time.Duration

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "spring.datasource.url=jdbc:h2:mem:mcp;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "onyx.worker.enabled=false",
        "onyx.storage.root=/tmp/onyx-kotlin-mcp-tests",
        "onyx.crypto.key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    ],
)
class McpEndpointIntegrationTest {
    @LocalServerPort
    private var port: Int = 0

    @Autowired
    private lateinit var mapper: ObjectMapper

    @MockitoBean
    private lateinit var search: SearchService

    @Test
    fun `remote client discovers and calls search`() {
        `when`(search.search("deployment guide", listOf("Engineering"), 5))
            .thenReturn(SearchResponse(results = emptyList()))
        val transport = HttpClientStreamableHttpTransport.builder("http://localhost:$port")
            .endpoint("/mcp")
            .jsonMapper(JacksonMcpJsonMapper(mapper))
            .build()

        McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build().use { client ->
            val initialized = client.initialize()
            assertThat(initialized.serverInfo().name()).isEqualTo("onyx-search")
            assertThat(client.listTools().tools().map(McpSchema.Tool::name)).containsExactlyInAnyOrder(
                "search",
                "search_indexed_documents",
                "weighted_reciprocal_rank_fusion",
            )
            val result = client.callTool(
                McpSchema.CallToolRequest.builder("search_indexed_documents")
                    .arguments(
                        mapOf(
                            "query" to "deployment guide",
                            "document_set_names" to listOf("Engineering"),
                            "limit" to 5,
                        ),
                    )
                    .build(),
            )
            assertThat(result.isError() == true).isFalse()
        }

        verify(search).search("deployment guide", listOf("Engineering"), 5, com.onyx.foss.kotlin.service.SearchType.HYBRID)
    }

    @Test
    fun `remote client uses header document sets when none provided in arguments`() {
        `when`(search.search("guide", listOf("Engineering", "Operations"), 5))
            .thenReturn(SearchResponse(results = emptyList()))
        val transport = HttpClientStreamableHttpTransport.builder("http://localhost:$port")
            .endpoint("/mcp")
            .jsonMapper(JacksonMcpJsonMapper(mapper))
            .httpRequestCustomizer { builder, _, _, _, _ -> builder.header("X-Onyx-Document-Sets", "Engineering,Operations") }
            .build()

        McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build().use { client ->
            client.initialize()
            val result = client.callTool(
                McpSchema.CallToolRequest.builder("search")
                    .arguments(mapOf("query" to "guide", "limit" to 5))
                    .build(),
            )
            assertThat(result.isError() == true).isFalse()
        }

        verify(search).search("guide", listOf("Engineering", "Operations"), 5)
    }

    @Test
    fun `remote client uses query parameter document sets as fallback`() {
        `when`(search.search("guide", listOf("FallbackSet"), 5))
            .thenReturn(SearchResponse(results = emptyList()))
        val transport = HttpClientStreamableHttpTransport.builder("http://localhost:$port")
            .endpoint("/mcp?document_sets=FallbackSet")
            .jsonMapper(JacksonMcpJsonMapper(mapper))
            .build()

        McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build().use { client ->
            client.initialize()
            val result = client.callTool(
                McpSchema.CallToolRequest.builder("search")
                    .arguments(mapOf("query" to "guide", "limit" to 5))
                    .build(),
            )
            assertThat(result.isError() == true).isFalse()
        }

        verify(search).search("guide", listOf("FallbackSet"), 5)
    }

    @Test
    fun `remote client prefers header over query parameter when both are present`() {
        `when`(search.search("guide", listOf("HeaderSet"), 5))
            .thenReturn(SearchResponse(results = emptyList()))
        val transport = HttpClientStreamableHttpTransport.builder("http://localhost:$port")
            .endpoint("/mcp?document_sets=FallbackSet")
            .jsonMapper(JacksonMcpJsonMapper(mapper))
            .httpRequestCustomizer { builder, _, _, _, _ -> builder.header("X-Onyx-Document-Sets", "HeaderSet") }
            .build()

        McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10)).build().use { client ->
            client.initialize()
            val result = client.callTool(
                McpSchema.CallToolRequest.builder("search")
                    .arguments(mapOf("query" to "guide", "limit" to 5))
                    .build(),
            )
            assertThat(result.isError() == true).isFalse()
        }

        verify(search).search("guide", listOf("HeaderSet"), 5)
    }
}
