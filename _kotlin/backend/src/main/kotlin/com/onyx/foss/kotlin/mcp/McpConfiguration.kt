package com.onyx.foss.kotlin.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema
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
        .instructions(
            "Use search once with a focused query. Only retry when evidence is insufficient or ambiguous. " +
                "Use at most 3 search calls per user request. Multiple document_sets are searched as a union.",
        )
        .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
        .toolCall(searchTool.definition()) { _, request -> searchTool.call(request.arguments()) }
        .build()
}
