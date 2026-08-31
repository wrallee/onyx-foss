package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.onyx.foss.kotlin.domain.ConnectorSource
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class RemoteConnectorLoaders(
    private val jira: JiraConnectorLoader,
    private val confluence: ConfluenceConnectorLoader,
    private val github: GithubConnectorLoader,
) {
    fun load(
        source: ConnectorSource,
        config: JsonNode?,
        credentials: JsonNode,
        checkpoint: JsonNode?,
        start: Instant? = null,
        end: Instant? = null,
    ): Sequence<ConnectorBatch> = when (source) {
        ConnectorSource.JIRA -> {
            jira.validate(config, credentials)
            jira.load(config, credentials, checkpoint, start = start, end = end)
        }
        ConnectorSource.CONFLUENCE -> {
            confluence.validate(config, credentials)
            confluence.load(config, credentials, checkpoint, start = start, end = end)
        }
        ConnectorSource.GITHUB -> {
            github.validate(config, credentials)
            github.load(config, credentials, checkpoint, start = start, end = end)
        }
        else -> error("Unsupported remote connector: ${source.value}")
    }
}
