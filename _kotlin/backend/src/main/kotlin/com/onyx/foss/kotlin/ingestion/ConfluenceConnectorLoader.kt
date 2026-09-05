package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ArrayNode
import tools.jackson.databind.node.ObjectNode
import com.onyx.foss.kotlin.domain.ConnectorSource
import org.apache.tika.metadata.Metadata
import org.apache.tika.metadata.TikaCoreProperties
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.html.HtmlMapper
import org.apache.tika.parser.html.IdentityHtmlMapper
import org.apache.tika.parser.html.JSoupParser
import org.apache.tika.sax.BodyContentHandler
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.util.UriUtils
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.Collections

data class ConfluenceCheckpoint(
    val hasMore: Boolean = true,
    val nextPageUrl: String? = null,
)

@Service
class ConfluenceConnectorLoader(
    private val http: RemoteJsonClient,
    private val mapper: ObjectMapper,
) {
    internal var sleepMillis: (Long) -> Unit = Thread::sleep

    fun load(
        config: JsonNode?,
        credentials: JsonNode,
        checkpointNode: JsonNode?,
        start: Instant? = null,
        end: Instant? = null,
    ): Sequence<ConnectorBatch> {
        val context = context(config, credentials)
        val saved = checkpointNode?.let { mapper.treeToValue(it, ConfluenceCheckpoint::class.java) }
        val checkpoint = saved?.takeIf(ConfluenceCheckpoint::hasMore) ?: ConfluenceCheckpoint()
        return loadPages(context, checkpoint, start, end)
    }

    fun validate(config: JsonNode?, credentials: JsonNode) {
        val context = context(config, credentials)
        val cloudV2 = context.isCloud && !context.config.boolean("scoped_token", false)
        val initial = if (cloudV2) "/wiki/api/v2/spaces" else "/rest/api/space"
        var path = updateQuery(initial, "limit", "1")
        if (!cloudV2) path = updateQuery(path, "start", "0")
        val first = try {
            val response = try {
                get(context, path)
            } catch (error: WebClientResponseException.NotFound) {
                if (!cloudV2) throw error
                path = updateQuery("/rest/api/space", "limit", "1")
                path = updateQuery(path, "start", "0")
                get(context, path)
            }
            response.path("results").firstOrNull()
        } catch (error: WebClientResponseException) {
            throw when (error.statusCode.value()) {
                401 -> IllegalArgumentException("Invalid or expired Confluence credentials (HTTP 401).", error)
                403 -> IllegalArgumentException("Insufficient permissions to access Confluence resources (HTTP 403).", error)
                else -> IllegalArgumentException(
                    "Unexpected Confluence error (status=${error.statusCode.value()}): ${error.responseBodyAsString}",
                    error,
                )
            }
        }
        require(first != null) { "No Confluence spaces found or the credential cannot list spaces." }
        val space = config.text("space") ?: return
        try {
            get(context, "/rest/api/space/${segment(space)}")
        } catch (error: WebClientResponseException) {
            throw IllegalArgumentException("Invalid Confluence space key '$space'.", error)
        }
    }

    fun reindex(
        config: JsonNode?,
        credentials: JsonNode,
        failures: List<ConnectorFailure>,
    ): Sequence<ConnectorBatch> {
        val documentFailures = failures.mapNotNull { failure ->
            (failure.target as? FailureTarget.Document)?.let { it to extractPageId(it.id) }
        }
        if (documentFailures.isEmpty()) return emptySequence()
        val parseFailures = documentFailures.filter { it.second == null }.map { (target) ->
            ConnectorFailure(
                FailureTarget.Document(target.id, target.link ?: target.id),
                "Cannot extract page id from doc URL '${target.id}'; targeted reindex supports /pages/<id>/ and pageId=<id> URL shapes.",
                "confluence_target_resolution",
            )
        }
        val targets = documentFailures.mapNotNull { (target, id) -> id?.let { it to target } }.toMap()
        if (targets.isEmpty()) {
            return sequenceOf(finalBatch(emptyList(), parseFailures))
        }

        val context = context(config, credentials)
        val cql = "type=page and id IN (${targets.keys.joinToString(",") { "'$it'" }})"
        val pages = paginate(context, buildCqlPath(cql, PAGE_EXPAND), DEFAULT_PAGE_SIZE)
        val documents = mutableListOf<SourceDocument>()
        val outputFailures = parseFailures.toMutableList()
        val seen = mutableSetOf<String>()
        pages.forEach { page ->
            val pageId = page.path("id").asText()
            seen += pageId
            val result = processPage(
                context,
                page,
                start = null,
                end = null,
            )
            documents += result.documents
            outputFailures += result.failures.map { failure ->
                if (failure.errorType == "confluence_page_processing") {
                    val original = targets[pageId]
                    if (original != null) failure.copy(target = FailureTarget.Document(original.id, original.link)) else failure
                } else {
                    failure
                }
            }
        }
        targets.filterKeys { it !in seen }.forEach { (pageId, target) ->
            outputFailures += ConnectorFailure(
                FailureTarget.Document(target.id, target.link ?: target.id),
                "Confluence returned no page for id=$pageId during targeted reindex.",
                "confluence_target_missing",
            )
        }
        return sequenceOf(finalBatch(documents, outputFailures))
    }

    fun retrieveAllSlimDocuments(
        config: JsonNode?,
        credentials: JsonNode,
        start: Instant? = null,
        end: Instant? = null,
    ): Sequence<ConnectorBatch> {
        val context = context(config, credentials)
        return retrieveSlim(context, start, end)
    }

    internal fun constructPageCql(config: JsonNode?, start: Instant?, end: Instant?): String {
        var query = when {
            config.text("cql_query") != null -> config.text("cql_query")!!
            config.text("page_id") != null && config.boolean("index_recursively", false) ->
                "type=page and (ancestor='${config.text("page_id")}' or id='${config.text("page_id")}')"
            config.text("page_id") != null -> "type=page and id='${config.text("page_id")}'"
            config.text("space") != null -> "type=page and space='${config.text("space")}'"
            else -> "type=page"
        }
        val labels = config?.path("labels_to_skip")?.takeIf(JsonNode::isArray)
            ?.mapNotNull { it.asText().takeIf(String::isNotBlank) }.orEmpty().distinct()
        if (labels.isNotEmpty()) query += " and label not in (${labels.joinToString(",") { "'$it'" }})"
        val offsetSeconds = ((config?.path("timezone_offset")?.asDouble(0.0) ?: 0.0) * 3600).toInt()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneOffset.ofTotalSeconds(offsetSeconds))
        if (start != null) query += " and lastmodified >= '${formatter.format(start)}'"
        if (end != null) query += " and lastmodified <= '${formatter.format(end)}'"
        return "$query order by lastmodified asc"
    }

    internal fun paginate(
        config: JsonNode?,
        credentials: JsonNode,
        initialPath: String,
        limit: Int,
        forceOffsetPagination: Boolean = false,
    ): List<JsonNode> = paginate(context(config, credentials), initialPath, limit, forceOffsetPagination)

    internal fun cqlPaginateAllExpansions(
        config: JsonNode?,
        credentials: JsonNode,
        cql: String,
        expand: String?,
        limit: Int,
    ): List<JsonNode> {
        val context = context(config, credentials)
        return paginate(context, buildCqlPath(cql, expand), limit).map { item ->
            item.deepCopy().also { expandNested(context, it, limit) }
        }
    }

    internal fun parseHtml(html: String): String {
        val handler = ConfluenceHtmlHandler()
        val context = ParseContext().also { it.set(HtmlMapper::class.java, IdentityHtmlMapper.INSTANCE) }
        JSoupParser().parse(ByteArrayInputStream(html.toByteArray()), handler, Metadata(), context)
        return handler.result()
    }

    private fun loadPages(
        context: Context,
        initialCheckpoint: ConfluenceCheckpoint,
        start: Instant?,
        end: Instant?,
    ): Sequence<ConnectorBatch> = sequence {
        var path = initialCheckpoint.nextPageUrl ?: buildCqlPath(
            constructPageCql(context.config, start, end),
            PAGE_EXPAND,
        )
        var limit = context.config?.path("batch_size")?.asInt(DEFAULT_PAGE_SIZE)?.coerceAtLeast(1) ?: DEFAULT_PAGE_SIZE
        while (true) {
            val page = fetchPage(context, path, limit)
            limit = page.effectiveLimit
            val documents = mutableListOf<SourceDocument>()
            val failures = mutableListOf<ConnectorFailure>()
            page.results.forEach { item ->
                val result = processPage(context, item, start, end)
                documents += result.documents
                failures += result.failures
            }
            val checkpoint = ConfluenceCheckpoint(page.nextPath != null, page.nextPath)
            yield(
                ConnectorBatch(
                    documents,
                    failures,
                    ConnectorCheckpoint(mapper.valueToTree(checkpoint), checkpoint.hasMore),
                    failures.all { it.target is FailureTarget.Document },
                ),
            )
            if (!checkpoint.hasMore) break
            path = checkpoint.nextPageUrl!!
        }
    }

    private fun paginate(
        context: Context,
        initialPath: String,
        limit: Int,
        forceOffsetPagination: Boolean = false,
        allowAllFailedRecovery: Boolean = true,
    ): List<JsonNode> {
        val output = mutableListOf<JsonNode>()
        var path: String? = initialPath
        var currentLimit = limit
        val visited = mutableSetOf<URI>()
        while (path != null) {
            require(visited.add(approvedRequestUri(context, path))) { "Confluence pagination cycle detected" }
            val page = fetchPage(
                context,
                path,
                currentLimit,
                forceOffsetPagination,
                allowAllFailedRecovery,
            )
            output += page.results
            require(output.size <= MAX_PAGINATED_RESULTS) { "Confluence pagination exceeded $MAX_PAGINATED_RESULTS results" }
            path = page.nextPath
            currentLimit = page.effectiveLimit
        }
        return output
    }

    private fun fetchPage(
        context: Context,
        initialPath: String,
        requestedLimit: Int,
        forceOffsetPagination: Boolean = false,
        allowAllFailedRecovery: Boolean = true,
    ): PageFetch {
        var currentLimit = requestedLimit
        var path = updateQuery(initialPath, "limit", currentLimit.toString())
        var rateLimitAttempts = 0
        while (true) {
            val response = try {
                get(context, path)
            } catch (error: WebClientResponseException) {
                val status = error.statusCode.value()
                if (status == 429 || error.responseBodyAsString.contains(RATE_LIMIT_MESSAGE, ignoreCase = true)) {
                    rateLimitAttempts += 1
                    if (rateLimitAttempts >= MAX_SOURCE_RETRIES) throw error
                    sleepMillis(retryAfterMillis(error, rateLimitAttempts - 1))
                    continue
                }
                if (path.contains(PROBLEMATIC_BODY_EXPAND)) {
                    path = path.replace(PROBLEMATIC_BODY_EXPAND, REPLACEMENT_BODY_EXPAND)
                    continue
                }
                if (status in SERVER_ERROR_CODES) {
                    if (currentLimit > MINIMUM_PAGE_SIZE) {
                        currentLimit = maxOf(currentLimit / 2, MINIMUM_PAGE_SIZE)
                        path = updateQuery(path, "limit", currentLimit.toString())
                        continue
                    }
                    if (!context.isCloud) {
                        return recoverOneByOne(context, path, currentLimit, error, allowAllFailedRecovery)
                    }
                }
                throw error
            }
            val results = response.path("results").takeIf(JsonNode::isArray)?.toList().orEmpty()
            val oldPath = path
            val nextStart = queryInt(oldPath, "start") + results.size
            var next = response.path("_links").path("next").asText().takeIf(String::isNotBlank)
            if (next != null && currentLimit != requestedLimit) next = updateQuery(next, "limit", currentLimit.toString())
            if (next != null && !context.isCloud && results.isNotEmpty()) next = updateQuery(next, "start", nextStart.toString())
            if (forceOffsetPagination && next == null && results.size >= currentLimit) {
                next = updateQuery(oldPath, "start", nextStart.toString())
            }
            if (results.isEmpty()) next = null
            return PageFetch(results, next, currentLimit)
        }
    }

    private fun recoverOneByOne(
        context: Context,
        path: String,
        limit: Int,
        original: WebClientResponseException,
        allowAllFailedRecovery: Boolean,
    ): PageFetch {
        val initialStart = queryInt(path, "start")
        val results = mutableListOf<JsonNode>()
        var receivedResponse = false
        repeat(limit) { index ->
            val recoveryPath = updateQuery(updateQuery(path, "start", (initialStart + index).toString()), "limit", "1")
            try {
                val response = get(context, recoveryPath)
                receivedResponse = true
                val recovered = response.path("results").takeIf(JsonNode::isArray)?.toList().orEmpty()
                if (recovered.isEmpty()) return PageFetch(results, null, limit)
                results += recovered
            } catch (_: Exception) {
                // A single broken offset must not hide the remaining items.
            }
        }
        if (!receivedResponse && !allowAllFailedRecovery) throw original
        return PageFetch(results, updateQuery(path, "start", (initialStart + limit).toString()), limit)
    }

    private fun expandNested(context: Context, node: JsonNode, limit: Int) {
        when (node) {
            is ObjectNode -> {
                val next = node.path("_links").path("next").asText().takeIf(String::isNotBlank)
                val results = node.path("results") as? ArrayNode
                if (next != null && results != null) paginate(context, next, limit).forEach(results::add)
                node.properties().forEach { (_, value) -> expandNested(context, value, limit) }
            }
            is ArrayNode -> node.forEach { expandNested(context, it, limit) }
        }
    }

    private fun processPage(
        context: Context,
        page: JsonNode,
        start: Instant?,
        end: Instant?,
    ): ProcessResult {
        val pageId = page.path("id").asText().ifBlank { "unknown" }
        val pageUrl = page.path("_links").path("webui").asText().takeIf(String::isNotBlank)
            ?.let { buildContentUrl(context, it) }
        val access = ExternalAccess(isPublic = true)
        val pageDocument = try {
            require(pageId != "unknown") { "Confluence page id is missing" }
            val title = page.path("title").asText().ifBlank { pageId }
            val updated = page.path("version").path("when").asText().takeIf(String::isNotBlank)
                ?: error("Confluence page $pageId has no version timestamp")
            requireNotNull(pageUrl) { "Confluence page $pageId has no web link" }
            val html = page.path("body").path("storage").path("value").asText()
                .ifBlank { page.path("body").path("view").path("value").asText() }
            val comments = if (context.config.boolean("include_comments", true)) comments(context, pageId) else ""
            val labels = page.path("metadata").path("labels").path("results").toList().mapNotNull{ it.path("name").asText().takeIf(String::isNotBlank) }
            val spaceKey = page.path("space").path("key").asText()
            val metadata = linkedMapOf<String, Any?>(
                "source" to "confluence",
                "confluence_page_id" to pageId,
                "space" to spaceKey,
                "labels" to labels,
                "created" to page.path("history").path("createdDate").asText(),
                "parent_hierarchy_raw_node_id" to (
                    page.path("ancestors").lastOrNull()?.path("id")?.asText()
                        ?.takeIf(String::isNotBlank) ?: spaceKey.takeIf(String::isNotBlank)
                    ),
            )
            SourceDocument(
                id = pageUrl,
                title = title,
                content = (parsePageHtml(context, html, mutableSetOf()) + comments).trim(),
                link = pageUrl,
                metadata = metadata,
                externalAccess = access,
                source = ConnectorSource.CONFLUENCE,
                updatedAt = parseInstant(updated),
                primaryOwners = page.path("version").path("by").let { owner ->
                    listOfNotNull(
                        owner.path("email").asText().takeIf(String::isNotBlank)
                            ?: owner.path("displayName").asText().takeIf(String::isNotBlank),
                    )
                },
            )
        } catch (error: Exception) {
            return ProcessResult(
                failures = listOf(
                    ConnectorFailure(
                        pageUrl?.let { FailureTarget.Document(it, it) }
                            ?: FailureTarget.Entity("confluence-page:$pageId"),
                        "Error converting Confluence page $pageId: ${error.message ?: error::class.simpleName}",
                        "confluence_page_processing",
                    ),
                ),
            )
        }

        val documents = mutableListOf(pageDocument)
        val failures = mutableListOf<ConnectorFailure>()
        val attachments = fetchAttachments(context, page, start, end, access)
        documents += attachments.documents
        failures += attachments.failures
        return ProcessResult(documents, failures)
    }

    private fun comments(context: Context, pageId: String): String {
        val cql = "type=comment and container='$pageId'${labelFilter(context.config)}"
        val comments = paginate(context, buildCqlPath(cql, COMMENT_EXPAND), DEFAULT_PAGE_SIZE)
            .map { parsePageHtml(context, it.path("body").path("storage").path("value").asText(), mutableSetOf()) }
            .filter(String::isNotBlank)
        return comments.joinToString(separator = "", prefix = if (comments.isEmpty()) "" else "\n") { "Comment:\n$it" }
    }

    private fun fetchAttachments(
        context: Context,
        page: JsonNode,
        start: Instant?,
        end: Instant?,
        inheritedAccess: ExternalAccess?,
    ): ProcessResult {
        if (!context.config.boolean("include_attachments", true)) return ProcessResult()
        val pageId = page.path("id").asText().ifBlank { "unknown" }
        val pageUrl = page.path("_links").path("webui").asText().takeIf(String::isNotBlank)
            ?.let { buildContentUrl(context, it) } ?: "page_id:$pageId"
        val documents = mutableListOf<SourceDocument>()
        val failures = mutableListOf<ConnectorFailure>()
        var path: String? = buildCqlPath(constructAttachmentCql(context.config, pageId, start, end), ATTACHMENT_EXPAND)
        var limit = DEFAULT_PAGE_SIZE
        var resultCount = 0
        val visited = mutableSetOf<URI>()
        try {
            while (path != null) {
                require(visited.add(approvedRequestUri(context, path))) { "Confluence attachment pagination cycle detected" }
                val result = fetchPage(context, path, limit, allowAllFailedRecovery = false)
                limit = result.effectiveLimit
                resultCount += result.results.size
                require(resultCount <= MAX_PAGINATED_RESULTS) {
                    "Confluence attachment pagination exceeded $MAX_PAGINATED_RESULTS results"
                }
                result.results.forEach { attachment ->
                    if (!includeAttachment(context.config, attachment)) return@forEach
                    val converted = convertAttachment(context, page, pageUrl, attachment, inheritedAccess)
                    if (converted.document != null) documents += converted.document
                    if (converted.failure != null) failures += converted.failure
                }
                path = result.nextPath
            }
        } catch (error: WebClientResponseException) {
            val status = error.statusCode.value()
            if (status == 400 && isDateError(error)) throw error
            if (status !in setOf(400, 401, 403)) throw error
            if (status == 401 || status == 403) {
                documents.clear()
                failures.clear()
            }
            failures += ConnectorFailure(
                FailureTarget.Document(pageUrl, pageUrl),
                when (status) {
                    400 -> "Bad request (400) while paginating attachments for page '$pageId'. Keeping the attachments retrieved so far."
                    401 -> "Invalid credentials (401) when fetching attachments for page '$pageId'. Skipping attachments."
                    else -> "Permission denied (403) when fetching attachments for page '$pageId'. Skipping attachments."
                },
                "confluence_attachment_pagination",
            )
        }
        return ProcessResult(documents, failures)
    }

    private fun convertAttachment(
        context: Context,
        page: JsonNode,
        pageUrl: String,
        attachment: JsonNode,
        inheritedAccess: ExternalAccess?,
    ): AttachmentResult {
        val pageId = page.path("id").asText()
        val attachmentId = attachment.path("id").asText()
        val title = attachment.path("title").asText().ifBlank { attachmentId }
        val mediaType = attachment.path("metadata").path("mediaType").asText()
        val webui = attachment.path("_links").path("webui").asText()
        val documentId = buildContentUrl(context, webui)
        val objectPath = if (context.isCloud) {
            "/download/attachments/${segment(pageId)}/${pathSegment(title)}"
        } else {
            attachment.path("_links").path("download").asText()
        }
        val objectUrl = buildContentUrl(context, objectPath)
        return try {
            val downloadPath = if (context.isCloud) {
                "/rest/api/content/${segment(pageId)}/child/attachment/${segment(attachmentId)}/download"
            } else {
                attachment.path("_links").path("download").asText()
            }
            val bytes = getBytes(context, downloadPath)
            require(bytes.isNotEmpty()) { "Attachment download was empty" }
            val isImage = mediaType.startsWith("image/")
            val content = if (isImage) "" else extractAttachment(bytes, title)
            if (!isImage && content.length > context.config.int("attachment_char_count_threshold", DEFAULT_ATTACHMENT_CHAR_LIMIT)) {
                return AttachmentResult()
            }
            val labels = attachment.path("metadata").path("labels").path("results").toList().mapNotNull{ it.path("name").asText().takeIf(String::isNotBlank) }
            AttachmentResult(
                document = SourceDocument(
                    id = documentId,
                    title = title,
                    content = content,
                    link = objectUrl,
                    metadata = mapOf(
                        "source" to "confluence",
                        "space" to attachment.path("space").path("key").asText()
                            .ifBlank { page.path("space").path("key").asText() },
                        "labels" to labels,
                        "parent_page_id" to pageUrl,
                        "parent_hierarchy_raw_node_id" to pageUrl,
                        "mime_type" to mediaType,
                        "image" to isImage,
                    ),
                    externalAccess = inheritedAccess,
                    source = ConnectorSource.CONFLUENCE,
                    updatedAt = attachment.path("version").path("when").asText().takeIf(String::isNotBlank)?.let(::parseInstant),
                    primaryOwners = attachment.path("version").path("by").let { owner ->
                        listOfNotNull(
                            owner.path("email").asText().takeIf(String::isNotBlank)
                                ?: owner.path("displayName").asText().takeIf(String::isNotBlank),
                        )
                    },
                ),
            )
        } catch (error: Exception) {
            AttachmentResult(
                failure = ConnectorFailure(
                    FailureTarget.Document(documentId, objectUrl),
                    "Failed to extract attachment $title: ${error.message ?: error::class.simpleName}",
                    "confluence_attachment_processing",
                ),
            )
        }
    }

    private fun extractAttachment(bytes: ByteArray, title: String): String {
        val handler = BodyContentHandler(-1)
        val metadata = Metadata().also { it.set(TikaCoreProperties.RESOURCE_NAME_KEY, title) }
        AutoDetectParser().parse(ByteArrayInputStream(bytes), handler, metadata)
        return handler.toString().trim()
    }

    private fun retrieveSlim(
        context: Context,
        start: Instant?,
        end: Instant?,
    ): Sequence<ConnectorBatch> = sequence {
        val expand = PRUNING_EXPAND
        var path: String? = buildCqlPath(constructPageCql(context.config, start, end), expand)
        var limit = SLIM_PAGE_SIZE
        while (path != null) {
            val result = fetchPage(context, path, limit)
            limit = result.effectiveLimit
            val documents = mutableListOf<SourceDocument>()
            val failures = mutableListOf<ConnectorFailure>()
            result.results.forEach { rawPage ->
                val page = rawPage.deepCopy().also { expandNested(context, it, limit) }
                val pageId = page.path("id").asText()
                val pageUrl = buildContentUrl(context, page.path("_links").path("webui").asText())
                val spaceKey = page.path("space").path("key").asText()
                val access = ExternalAccess(isPublic = true)
                documents += SourceDocument(
                    id = pageUrl,
                    title = page.path("title").asText(pageId),
                    content = "",
                    link = pageUrl,
                    metadata = mapOf(
                        "source" to "confluence",
                        "confluence_page_id" to pageId,
                        "space" to spaceKey,
                        "parent_hierarchy_raw_node_id" to (
                            page.path("ancestors").lastOrNull()?.path("id")?.asText()
                                ?.takeIf(String::isNotBlank) ?: spaceKey.takeIf(String::isNotBlank)
                            ),
                    ),
                    externalAccess = access,
                    source = ConnectorSource.CONFLUENCE,
                )
                if (context.config.boolean("include_attachments", true)) {
                    retrieveSlimAttachments(context, pageId, start, end)
                        .filter { includeAttachment(context.config, it) }
                        .forEach { attachment ->
                            val attachmentUrl = buildContentUrl(context, attachment.path("_links").path("webui").asText())
                            documents += SourceDocument(
                                id = attachmentUrl,
                                title = attachment.path("title").asText(attachmentUrl),
                                content = "",
                                link = attachmentUrl,
                                metadata = mapOf(
                                    "source" to "confluence",
                                    "space" to attachment.path("space").path("key").asText().ifBlank { spaceKey },
                                    "parent_page_id" to pageUrl,
                                    "parent_hierarchy_raw_node_id" to pageUrl,
                                    "mime_type" to attachment.path("metadata").path("mediaType").asText(),
                                ),
                                externalAccess = access,
                                source = ConnectorSource.CONFLUENCE,
                            )
                        }
                }
            }
            val checkpoint = ConfluenceCheckpoint(result.nextPath != null, result.nextPath)
            yield(
                ConnectorBatch(
                    documents,
                    failures,
                    ConnectorCheckpoint(mapper.valueToTree(checkpoint), checkpoint.hasMore),
                    failures.all { it.target is FailureTarget.Document },
                ),
            )
            path = result.nextPath
        }
    }

    private fun retrieveSlimAttachments(context: Context, pageId: String, start: Instant?, end: Instant?): List<JsonNode> {
        val path = buildCqlPath(constructAttachmentCql(context.config, pageId, start, end), PRUNING_EXPAND)
        repeat(SLIM_ATTACHMENT_ATTEMPTS) { attempt ->
            try {
                return paginate(context, path, SLIM_PAGE_SIZE, allowAllFailedRecovery = false)
            } catch (error: WebClientResponseException) {
                if (error.statusCode.value() != 400 || isDateError(error) || attempt == SLIM_ATTACHMENT_ATTEMPTS - 1) throw error
            }
        }
        error("unreachable")
    }

    private fun cqlPaginateAllExpansions(context: Context, cql: String, expand: String, limit: Int): List<JsonNode> =
        paginate(context, buildCqlPath(cql, expand), limit).map { item ->
            item.deepCopy().also { expandNested(context, it, limit) }
        }

    private fun resolveDisplayName(context: Context, userId: String): String {
        val cache = context.userCaches.displayNames
        if (cache.containsKey(userId)) return cache[userId] ?: UNKNOWN_USER
        val displayName = listOf("key", "accountId").firstNotNullOfOrNull { field ->
            try {
                get(context, "/rest/api/user?$field=${query(userId)}").path("displayName").asText().takeIf(String::isNotBlank)
            } catch (_: Exception) {
                null
            }
        }
        cache[userId] = displayName
        return displayName ?: UNKNOWN_USER
    }


    private fun context(config: JsonNode?, credentials: JsonNode): Context {
        val wikiBase = config?.firstText("wiki_base", "confluence_base_url", "base_url")
            ?: error("Connector configuration is missing wiki_base")
        val isCloud = config.boolean("is_cloud", false)
        val token = credentials.firstText("confluence_access_token", "api_token", "access_token", "token")
            ?: error("Connector credential does not contain a Confluence access token")
        val username = credentials.firstText("confluence_username", "email", "username")
        val authorization = if (isCloud && !username.isNullOrBlank()) {
            "Basic " + Base64.getEncoder().encodeToString("$username:$token".toByteArray(StandardCharsets.UTF_8))
        } else {
            "Bearer $token"
        }
        val headers = mapOf(HttpHeaders.AUTHORIZATION to authorization)
        val apiBase = if (config.boolean("scoped_token", false)) {
            val parsed = URI.create(wikiBase)
            val tenantBase = "${parsed.scheme}://${parsed.rawAuthority}"
            val cloudId = http.get(tenantBase, "/_edge/tenant_info", headers).path("cloudId").asText()
            require(cloudId.isNotBlank()) { "Confluence scoped token could not resolve a Cloud ID" }
            "https://api.atlassian.com/ex/confluence/${segment(cloudId)}${parsed.rawPath.trimEnd('/')}"
        } else {
            wikiBase.trimEnd('/')
        }
        return Context(config, wikiBase.trimEnd('/'), apiBase, isCloud, headers)
    }

    private fun get(context: Context, path: String): JsonNode =
        http.get("", approvedRequestUri(context, path).toASCIIString(), context.headers)

    private fun getBytes(context: Context, path: String): ByteArray =
        http.getBytes("", approvedRequestUri(context, path).toASCIIString(), context.headers)

    private fun approvedRequestUri(context: Context, path: String): URI {
        val apiBase = URI.create(context.apiBase)
        val supplied = URI.create(path)
        val resolved = when {
            path.startsWith("//") -> URI.create("${apiBase.scheme}:$path")
            supplied.isAbsolute -> supplied
            else -> URI.create("${context.apiBase.trimEnd('/')}/${path.trimStart('/')}")
        }
        require(resolved.userInfo == null) { "Confluence response URL must not contain user information" }
        val requestedOrigin = resolved.origin()
        val allowedOrigins = setOf(URI.create(context.wikiBase).origin(), apiBase.origin())
        require(requestedOrigin in allowedOrigins) {
            "Confluence response URL origin $requestedOrigin is not an approved configured/API origin"
        }
        return resolved
    }

    private fun parsePageHtml(context: Context, html: String, fetchedTitles: MutableSet<String>): String {
        var text = parseHtml(html)
        USER_TOKEN.findAll(text).map { it.groupValues[1] }.toSet().forEach { userId ->
            val displayName = resolveDisplayName(context, userId)
            text = text.replace("{{USER:$userId}}", "@$displayName")
        }
        INCLUDE_TOKEN.findAll(text).map { it.groupValues[1] }.toSet().forEach { title ->
            if (!fetchedTitles.add(title)) {
                text = text.replace("{{INCLUDE:$title}}", "")
            } else {
                val page = runCatching {
                    paginate(context, buildCqlPath("type=page and title='$title'", PROBLEMATIC_BODY_EXPAND), 1).firstOrNull()
                }.getOrNull()
                val included = page?.path("body")?.path("storage")?.path("value")?.asText()
                    ?.let { parsePageHtml(context, it, fetchedTitles) }.orEmpty()
                text = text.replace("{{INCLUDE:$title}}", included)
            }
        }
        return text
    }

    private fun constructAttachmentCql(config: JsonNode?, pageId: String, start: Instant?, end: Instant?): String {
        var query = "type=attachment and container='$pageId'${labelFilter(config)}"
        val offsetSeconds = ((config?.path("timezone_offset")?.asDouble(0.0) ?: 0.0) * 3600).toInt()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneOffset.ofTotalSeconds(offsetSeconds))
        if (start != null) query += " and lastmodified >= '${formatter.format(start)}'"
        if (end != null) query += " and lastmodified <= '${formatter.format(end)}'"
        return "$query order by lastmodified asc"
    }

    private fun labelFilter(config: JsonNode?): String {
        val labels = config?.path("labels_to_skip")?.takeIf(JsonNode::isArray)
            ?.mapNotNull { it.asText().takeIf(String::isNotBlank) }.orEmpty().distinct()
        return if (labels.isEmpty()) "" else " and label not in (${labels.joinToString(",") { "'$it'" }})"
    }

    private fun includeAttachment(config: JsonNode?, attachment: JsonNode): Boolean {
        val mediaType = attachment.path("metadata").path("mediaType").asText().lowercase()
        if (mediaType.startsWith("image/") && !config.boolean("allow_images", false)) return false
        if (mediaType.startsWith("image/")) return mediaType in IMAGE_MIME_TYPES
        val size = attachment.path("extensions").path("fileSize").asLong(0)
        if (size > config.int("attachment_size_threshold", DEFAULT_ATTACHMENT_SIZE_LIMIT)) return false
        val title = attachment.path("title").asText().lowercase()
        return ALLOWED_EXTENSIONS.any(title::endsWith)
    }

    private fun buildCqlPath(cql: String, expand: String?): String =
        "/rest/api/content/search?cql=${query(cql)}" + if (expand == null) "" else "&expand=$expand"

    private fun buildContentUrl(context: Context, contentPath: String): String {
        if (contentPath.startsWith("http://") || contentPath.startsWith("https://")) return contentPath
        var base = context.wikiBase.trimEnd('/')
        if (context.isCloud && !URI.create(base).path.trimEnd('/').endsWith("/wiki")) base += "/wiki"
        return "$base/${contentPath.trimStart('/')}"
    }

    private fun finalBatch(documents: List<SourceDocument>, failures: List<ConnectorFailure>) = ConnectorBatch(
        documents,
        failures,
        ConnectorCheckpoint(mapper.valueToTree(ConfluenceCheckpoint(hasMore = false)), hasMore = false),
        failures.all { it.target is FailureTarget.Document },
    )

    private fun retryAfterMillis(error: WebClientResponseException, attempt: Int): Long {
        val raw = error.headers.getFirst("Retry-After")
        val parsedSeconds = raw?.trim()?.toDoubleOrNull()?.takeIf(Double::isFinite)?.coerceAtLeast(0.0)
            ?: raw?.let { value ->
                runCatching {
                    val target = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                    ((target.toEpochMilli() - Instant.now().toEpochMilli()) / 1_000.0).coerceAtLeast(0.0)
                }.getOrNull()
            }
        val delaySeconds = parsedSeconds?.coerceIn(MINIMUM_RETRY_SECONDS, MAXIMUM_RETRY_SECONDS)
            ?: minOf(STARTING_RETRY_SECONDS * (1L shl attempt), MAXIMUM_RETRY_SECONDS.toLong()).toDouble()
        return (delaySeconds * 1_000).toLong()
    }

    private fun isDateError(error: WebClientResponseException): Boolean =
        error.responseBodyAsString.contains("updated", ignoreCase = true) &&
            error.responseBodyAsString.contains("invalid", ignoreCase = true)

    private fun extractPageId(url: String): String? = PAGE_ID_PATTERNS.firstNotNullOfOrNull { it.find(url)?.groupValues?.get(1) }

    private fun updateQuery(path: String, name: String, value: String): String {
        val hashIndex = path.indexOf('#')
        val withoutFragment = if (hashIndex >= 0) path.substring(0, hashIndex) else path
        val fragment = if (hashIndex >= 0) path.substring(hashIndex) else ""
        val separator = withoutFragment.indexOf('?')
        val base = if (separator >= 0) withoutFragment.substring(0, separator) else withoutFragment
        val pairs = if (separator >= 0) withoutFragment.substring(separator + 1).split('&').filter(String::isNotBlank) else emptyList()
        val updated = mutableListOf<String>()
        var replaced = false
        pairs.forEach { pair ->
            if (URLDecoder.decode(pair.substringBefore('='), StandardCharsets.UTF_8) == name) {
                if (!replaced) updated += "${queryName(name)}=${queryValue(value)}"
                replaced = true
            } else {
                updated += pair
            }
        }
        if (!replaced) updated += "${queryName(name)}=${queryValue(value)}"
        return "$base?${updated.joinToString("&")}$fragment"
    }

    private fun queryInt(path: String, name: String): Int {
        val query = path.substringAfter('?', "").substringBefore('#')
        return query.split('&').firstNotNullOfOrNull { pair ->
            val key = URLDecoder.decode(pair.substringBefore('='), StandardCharsets.UTF_8)
            if (key == name) URLDecoder.decode(pair.substringAfter('=', ""), StandardCharsets.UTF_8).toIntOrNull() else null
        } ?: 0
    }

    private fun query(value: String): String = UriUtils.encodeQueryParam(value, StandardCharsets.UTF_8)
    private fun queryName(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
    private fun queryValue(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
    private fun segment(value: String): String = UriUtils.encodePathSegment(value, StandardCharsets.UTF_8)
    private fun pathSegment(value: String): String = UriUtils.encodePathSegment(value, StandardCharsets.UTF_8)
    private fun URI.origin(): Origin {
        val normalizedScheme = scheme?.lowercase()
        require(normalizedScheme in setOf("http", "https")) { "Confluence response URL must use HTTP or HTTPS" }
        val normalizedHost = requireNotNull(host?.lowercase()) { "Confluence response URL has no host" }
        val normalizedPort = if (port >= 0) port else if (normalizedScheme == "https") 443 else 80
        return Origin(requireNotNull(normalizedScheme), normalizedHost, normalizedPort)
    }
    private fun parseInstant(value: String): Instant = runCatching { Instant.parse(value) }
        .getOrElse { OffsetDateTime.parse(value).toInstant() }
    private fun String.parseVersion(): Pair<Int, Int>? {
        val parts = split('.')
        return if (parts.size < 2) null else parts[0].toIntOrNull()?.let { major -> parts[1].toIntOrNull()?.let { major to it } }
    }

    private fun JsonNode?.boolean(name: String, default: Boolean): Boolean = this?.path(name)?.asBoolean(default) ?: default
    private fun JsonNode?.int(name: String, default: Int): Int = this?.path(name)?.asInt(default) ?: default
    private fun JsonNode?.text(name: String): String? = this?.path(name)?.asText()?.takeIf(String::isNotBlank)
    private fun JsonNode.firstText(vararg names: String): String? = names.firstNotNullOfOrNull { name -> text(name) }

    private data class Context(
        val config: JsonNode?,
        val wikiBase: String,
        val apiBase: String,
        val isCloud: Boolean,
        val headers: Map<String, String>,
        val userCaches: UserCaches = UserCaches(),
    )

    private data class Origin(val scheme: String, val host: String, val port: Int)

    private class UserCaches {
        val displayNames = mutableMapOf<String, String?>()
    }

    private data class PageFetch(val results: List<JsonNode>, val nextPath: String?, val effectiveLimit: Int)
    private data class ProcessResult(
        val documents: List<SourceDocument> = emptyList(),
        val failures: List<ConnectorFailure> = emptyList(),
    )
    private data class AttachmentResult(val document: SourceDocument? = null, val failure: ConnectorFailure? = null)

    private class ConfluenceHtmlHandler : DefaultHandler() {
        private val output = StringBuilder()
        private val links = ArrayDeque<String>()
        private var includeDepth = 0

        override fun startElement(uri: String?, localName: String?, qName: String?, attributes: Attributes) {
            val tag = (qName?.takeIf(String::isNotBlank) ?: localName.orEmpty()).lowercase()
            when (tag) {
                "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr" -> newline()
                "li" -> {
                    newline()
                    output.append("- ")
                }
                "td", "th" -> output.append('\t')
                "br" -> output.append(if (output.lastOrNull() == '\t') "" else " ")
                "a" -> {
                    output.append('[')
                    links.addLast(attributes.value("href").orEmpty())
                }
                "ac:link-body" -> output.append("(LINK TEXT: ")
                "ac:structured-macro" -> if (attributes.value("ac:name") == "include") includeDepth += 1
                "ri:page" -> if (includeDepth > 0) {
                    attributes.value("ri:content-title")?.let { output.append("{{INCLUDE:").append(it).append("}}") }
                }
                "ri:user" -> {
                    val id = attributes.value("ri:account-id") ?: attributes.value("ri:userkey")
                    if (id != null) output.append("{{USER:").append(id).append("}}")
                }
                "ri:attachment" -> attributes.value("ri:filename")?.let { name ->
                    output.append("<attachment>").append(name.replace(' ', '_')).append("</attachment>")
                }
            }
        }

        override fun endElement(uri: String?, localName: String?, qName: String?) {
            val tag = (qName?.takeIf(String::isNotBlank) ?: localName.orEmpty()).lowercase()
            when (tag) {
                "a" -> output.append("](").append(links.removeLastOrNull().orEmpty()).append(')')
                "ac:link-body" -> output.append(')')
                "br" -> if (output.lastOrNull() !in setOf(null, ' ', '\n', '\t')) output.append(' ')
                "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "li" -> newline()
                "ac:structured-macro" -> if (includeDepth > 0) includeDepth -= 1
            }
        }

        override fun characters(ch: CharArray, start: Int, length: Int) {
            val text = String(ch, start, length).replace(Regex("\\s+"), " ")
            if (text.isBlank()) {
                if (output.isNotEmpty() && output.last() !in setOf(' ', '\n', '\t')) output.append(' ')
            } else {
                if (text.startsWith(' ') && output.lastOrNull() in setOf(null, ' ', '\n', '\t')) {
                    output.append(text.trimStart())
                } else {
                    output.append(text)
                }
            }
        }

        fun result(): String = output.toString().lineSequence()
            .map(String::trimEnd)
            .filter(String::isNotBlank)
            .joinToString("\n")
            .trim()

        private fun newline() {
            while (output.lastOrNull() == ' ') output.deleteCharAt(output.lastIndex)
            if (output.isNotEmpty() && output.last() != '\n') output.append('\n')
        }

        private fun Attributes.value(name: String): String? =
            getValue(name) ?: (0 until length).firstNotNullOfOrNull { index ->
                getValue(index).takeIf {
                    getQName(index).equals(name, true) || getLocalName(index).equals(name.substringAfter(':'), true)
                }
            }
    }

    private companion object {
        const val DEFAULT_PAGE_SIZE = 50
        const val SPACE_PAGE_SIZE = 5_000
        const val SLIM_PAGE_SIZE = 5_000
        const val SLIM_ATTACHMENT_ATTEMPTS = 3
        const val MINIMUM_PAGE_SIZE = 5
        const val MAX_SOURCE_RETRIES = 5
        const val MINIMUM_RETRY_SECONDS = 2.0
        const val MAXIMUM_RETRY_SECONDS = 60.0
        const val STARTING_RETRY_SECONDS = 5L
        const val MAX_PAGINATED_RESULTS = 100_000
        const val DEFAULT_ATTACHMENT_SIZE_LIMIT = 10 * 1024 * 1024
        const val DEFAULT_ATTACHMENT_CHAR_LIMIT = 200_000
        const val RATE_LIMIT_MESSAGE = "Rate limit exceeded"
        const val UNKNOWN_USER = "Unknown Confluence User"
        const val PROBLEMATIC_BODY_EXPAND = "body.storage.value"
        const val REPLACEMENT_BODY_EXPAND = "body.view.value"
        const val COMMENT_EXPAND = "body.storage.value"
        const val PAGE_EXPAND = "body.storage.value,version,space,metadata.labels,history.lastUpdated,ancestors"
        const val ATTACHMENT_EXPAND = "version,space,metadata.labels,history"
        const val PRUNING_EXPAND = "space,ancestors,history"

        val SERVER_ERROR_CODES = setOf(500, 502, 503, 504)
        val PAGE_ID_PATTERNS = listOf(Regex("/pages/(\\d+)(?:/|$)"), Regex("[?&]pageId=(\\d+)"))
        val IMAGE_MIME_TYPES = setOf("image/jpg", "image/jpeg", "image/png", "image/webp")
        val ALLOWED_EXTENSIONS = setOf(
            ".txt", ".md", ".mdx", ".conf", ".log", ".json", ".csv", ".tsv", ".xml", ".yml", ".yaml", ".sql",
            ".pdf", ".docx", ".pptx", ".eml", ".epub", ".html", ".xlsx", ".xlsm", ".png", ".jpg", ".jpeg", ".webp",
        )
        val USER_TOKEN = Regex("\\{\\{USER:([^}]+)}}")
        val INCLUDE_TOKEN = Regex("\\{\\{INCLUDE:([^}]+)}}")
    }
}
