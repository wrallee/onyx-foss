package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
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
            if (!checkpoint.isActive()) jira.validate(config, credentials)
            jira.load(config, credentials, checkpoint, start = start, end = end)
        }
        ConnectorSource.CONFLUENCE -> {
            if (!checkpoint.isActive()) confluence.validate(config, credentials)
            confluence.load(config, credentials, checkpoint, start = start, end = end)
        }
        ConnectorSource.GITHUB -> {
            if (!checkpoint.isActive()) github.validate(config, credentials)
            github.load(config, credentials, checkpoint, start = start, end = end)
        }
        else -> error("Unsupported remote connector: ${source.value}")
    }

    fun loadSlim(
        source: ConnectorSource,
        config: JsonNode?,
        credentials: JsonNode,
        start: Instant? = null,
        end: Instant? = null,
        heartbeat: (() -> Unit)? = null,
    ): Sequence<ConnectorBatch> = when (source) {
        ConnectorSource.JIRA -> {
            heartbeat?.invoke()
            jira.validate(config, credentials)
            jira.retrieveAllSlimDocuments(config, credentials, start, end)
        }
        ConnectorSource.CONFLUENCE -> {
            heartbeat?.invoke()
            confluence.validate(config, credentials)
            confluence.retrieveAllSlimDocuments(config, credentials, start, end)
        }
        ConnectorSource.GITHUB -> {
            val renew = heartbeat ?: {}
            github.validate(config, credentials, renew)
            github.retrieveAllSlimDocuments(config, credentials, start, end, renew)
        }
        else -> error("Unsupported slim remote connector: ${source.value}")
    }

    private fun JsonNode?.isActive(): Boolean =
        this?.path("hasMore")?.takeIf(JsonNode::isBoolean)?.asBoolean() == true
}
