package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema
import jakarta.servlet.http.HttpServletRequest
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class McpConfiguration {
    @Bean
    fun mcpJsonMapper(mapper: ObjectMapper): McpJsonMapper = JacksonMcpJsonMapper(mapper)

    @Bean
    fun mcpTransport(jsonMapper: McpJsonMapper): HttpServletStreamableServerTransportProvider =
        HttpServletStreamableServerTransportProvider.builder()
            .jsonMapper(jsonMapper)
            .mcpEndpoint("/mcp")
            .contextExtractor { request ->
                val documentSets = extractDocumentSets(request)
                if (documentSets.isNotEmpty()) {
                    McpTransportContext.create(mapOf(DEFAULT_DOCUMENT_SETS_KEY to documentSets))
                } else {
                    McpTransportContext.EMPTY
                }
            }
            .build()

    @Bean
    fun mcpServlet(transport: HttpServletStreamableServerTransportProvider) =
        ServletRegistrationBean(transport, "/mcp").apply { setAsyncSupported(true) }

    @Bean(destroyMethod = "close")
    fun mcpServer(
        transport: HttpServletStreamableServerTransportProvider,
        jsonMapper: McpJsonMapper,
        searchTool: McpSearchTool,
    ): McpSyncServer = McpServer.sync(transport)
        .jsonMapper(jsonMapper)
        .serverInfo("onyx-search", "0.1.0")
        .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
        .toolCall(searchTool.definition()) { exchange, request ->
            val defaultSets = (exchange.transportContext()?.get(DEFAULT_DOCUMENT_SETS_KEY) as? List<*>)
                ?.mapNotNull { it as? String }
                .orEmpty()
            searchTool.call(request.arguments(), defaultSets)
        }
        .build()

    companion object {
        const val DEFAULT_DOCUMENT_SETS_KEY = "default_document_sets"

        fun extractDocumentSets(request: HttpServletRequest): List<String> {
            val headerVal = request.getHeader("X-Onyx-Document-Sets")
                ?: request.getHeader("X-Document-Sets")
            if (!headerVal.isNullOrBlank()) {
                return parseDocumentSets(headerVal)
            }

            val queryVal = request.getParameter("document_sets")
            if (!queryVal.isNullOrBlank()) {
                return parseDocumentSets(queryVal)
            }

            return emptyList()
        }

        private fun parseDocumentSets(value: String): List<String> =
            value.split(",").map(String::trim).filter(String::isNotEmpty)
    }
}
