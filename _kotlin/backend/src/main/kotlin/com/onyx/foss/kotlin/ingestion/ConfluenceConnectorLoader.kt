package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
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

data class ConfluenceUser(
    val userId: String,
    val username: String?,
    val displayName: String,
    val email: String?,
    val type: String,
)

class Confcloud77618Exception(url: String, body: String) : RuntimeException(
    "CONFCLOUD-77618: ancestor-restrictions expand 404 from $url: ${body.take(500)}",
)

class ConfluenceRestSpacePermissionsNotAvailableException(message: String) : RuntimeException(message)

@Service
class ConfluenceConnectorLoader(
    private val http: RemoteJsonClient,
    private val mapper: ObjectMapper,
) {
    internal var sleepMillis: (Long) -> Unit = Thread::sleep

    private val serverVersionCache = Collections.synchronizedMap(mutableMapOf<String, Pair<Int, Int>?>())

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
        val includePermissions = config.boolean("include_permissions", false)
        val spaceAccess = if (includePermissions) allSpacePermissions(context, prefixGroups = true) else emptyMap()
        return loadPages(context, checkpoint, start, end, includePermissions, spaceAccess)
    }

    fun validate(config: JsonNode?, credentials: JsonNode) {
        val context = context(config, credentials)
        val first = try {
            retrieveSpaces(context, 1).firstOrNull()
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
        includePermissions: Boolean = false,
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
        val spaceAccess = if (includePermissions) allSpacePermissions(context, prefixGroups = true) else emptyMap()
        val cql = "type=page and id IN (${targets.keys.joinToString(",") { "'$it'" }})"
        val expand = pageExpand(includePermissions)
        val pages = paginate(context, buildCqlPath(cql, expand), DEFAULT_PAGE_SIZE)
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
                includePermissions = includePermissions,
                spaceAccess = spaceAccess,
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
        includePermissions: Boolean = false,
    ): Sequence<ConnectorBatch> {
        val context = context(config, credentials)
        return if (!includePermissions) {
            retrieveSlim(context, start, end, includePermissions = false, perPageRestrictions = false)
        } else {
            sequence {
                try {
                    for (batch in retrieveSlim(context, start, end, includePermissions = true, perPageRestrictions = false)) {
                        yield(batch)
                    }
                    return@sequence
                } catch (_: Confcloud77618Exception) {
                    for (batch in retrieveSlim(context, start, end, includePermissions = true, perPageRestrictions = true)) {
                        yield(batch)
                    }
                }
            }
        }
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
            item.deepCopy<JsonNode>().also { expandNested(context, it, limit) }
        }
    }

    internal fun parseHtml(html: String): String {
        val handler = ConfluenceHtmlHandler()
        val context = ParseContext().also { it.set(HtmlMapper::class.java, IdentityHtmlMapper.INSTANCE) }
        JSoupParser().parse(ByteArrayInputStream(html.toByteArray()), handler, Metadata(), context)
        return handler.result()
    }

    internal fun isConfcloud77618(status: Int, body: String): Boolean =
        status == 404 && CONFCLOUD_77618_SIGNATURES.any(body::contains)

    private fun loadPages(
        context: Context,
        initialCheckpoint: ConfluenceCheckpoint,
        start: Instant?,
        end: Instant?,
        includePermissions: Boolean,
        spaceAccess: Map<String, ExternalAccess>,
    ): Sequence<ConnectorBatch> = sequence {
        var path = initialCheckpoint.nextPageUrl ?: buildCqlPath(
            constructPageCql(context.config, start, end),
            pageExpand(includePermissions),
        )
        var limit = context.config?.path("batch_size")?.asInt(DEFAULT_PAGE_SIZE)?.coerceAtLeast(1) ?: DEFAULT_PAGE_SIZE
        while (true) {
            val page = fetchPage(context, path, limit)
            limit = page.effectiveLimit
            val documents = mutableListOf<SourceDocument>()
            val failures = mutableListOf<ConnectorFailure>()
            page.results.forEach { item ->
                val result = processPage(context, item, start, end, includePermissions, spaceAccess)
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
                if (path.contains(ANCESTOR_RESTRICTIONS_EXPAND) && isConfcloud77618(status, error.responseBodyAsString)) {
                    throw Confcloud77618Exception(path, error.responseBodyAsString)
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
                node.fields().forEachRemaining { (_, value) -> expandNested(context, value, limit) }
            }
            is ArrayNode -> node.forEach { expandNested(context, it, limit) }
        }
    }

    private fun processPage(
        context: Context,
        page: JsonNode,
        start: Instant?,
        end: Instant?,
        includePermissions: Boolean,
        spaceAccess: Map<String, ExternalAccess>,
    ): ProcessResult {
        val pageId = page.path("id").asText().ifBlank { "unknown" }
        val pageUrl = page.path("_links").path("webui").asText().takeIf(String::isNotBlank)
            ?.let { buildContentUrl(context, it) }
        val pageDocument = try {
            require(pageId != "unknown") { "Confluence page id is missing" }
            val title = page.path("title").asText().ifBlank { pageId }
            val updated = page.path("version").path("when").asText().takeIf(String::isNotBlank)
                ?: error("Confluence page $pageId has no version timestamp")
            requireNotNull(pageUrl) { "Confluence page $pageId has no web link" }
            val html = page.path("body").path("storage").path("value").asText()
                .ifBlank { page.path("body").path("view").path("value").asText() }
            val comments = if (context.config.boolean("include_comments", true)) comments(context, pageId) else ""
            val labels = page.path("metadata").path("labels").path("results")
                .mapNotNull { it.path("name").asText().takeIf(String::isNotBlank) }
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
            val access = if (includePermissions) {
                runCatching {
                    resolveInlineRestrictions(context, page, groupPrefix = CONFLUENCE_GROUP_PREFIX)
                        ?: spaceAccess[spaceKey]
                        ?: PRIVATE_ACCESS
                }.getOrDefault(PRIVATE_ACCESS)
            } else {
                null
            }
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
        val attachments = fetchAttachments(context, page, start, end, pageDocument.externalAccess)
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
            val labels = attachment.path("metadata").path("labels").path("results")
                .mapNotNull { it.path("name").asText().takeIf(String::isNotBlank) }
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
        includePermissions: Boolean,
        perPageRestrictions: Boolean,
    ): Sequence<ConnectorBatch> = sequence {
        val expand = when {
            !includePermissions -> PRUNING_EXPAND
            perPageRestrictions -> PER_PAGE_RESTRICTIONS_EXPAND
            else -> RESTRICTIONS_EXPAND
        }
        val unresolvedSpaces = mutableSetOf<String>()
        val spaceAccess = if (includePermissions) {
            allSpacePermissions(context, prefixGroups = false, unresolvedSpaces = unresolvedSpaces)
        } else {
            emptyMap()
        }
        val ancestorCache = mutableMapOf<String, JsonNode?>()
        var path: String? = buildCqlPath(constructPageCql(context.config, start, end), expand)
        var limit = SLIM_PAGE_SIZE
        while (path != null) {
            val result = fetchPage(context, path, limit)
            limit = result.effectiveLimit
            val documents = mutableListOf<SourceDocument>()
            val failures = mutableListOf<ConnectorFailure>()
            result.results.forEach { rawPage ->
                val page = rawPage.deepCopy<JsonNode>().also { expandNested(context, it, limit) }
                val pageId = page.path("id").asText()
                val pageUrl = buildContentUrl(context, page.path("_links").path("webui").asText())
                val spaceKey = page.path("space").path("key").asText()
                val permission = if (includePermissions) {
                    resolveSlimPermission(
                        context,
                        page,
                        pageUrl,
                        spaceKey,
                        spaceAccess,
                        unresolvedSpaces,
                        ancestorCache,
                        perPageRestrictions,
                    )
                } else {
                    PermissionResolution(null)
                }
                if (permission.failure != null) failures += permission.failure
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
                    externalAccess = permission.access,
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
                                externalAccess = permission.access,
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

    private fun resolveSlimPermission(
        context: Context,
        page: JsonNode,
        pageUrl: String,
        spaceKey: String,
        spaceAccess: Map<String, ExternalAccess>,
        unresolvedSpaces: Set<String>,
        ancestorCache: MutableMap<String, JsonNode?>,
        perPageRestrictions: Boolean,
    ): PermissionResolution = try {
        val pageAccess = if (perPageRestrictions) {
            resolveRestrictions(
                page.path("restrictions"),
                page.path("ancestors").toList(),
                ancestorCache,
                fetch = { ancestorId -> fetchContentReadRestrictions(context, ancestorId) },
                emailResolver = { user -> resolveRestrictionEmail(context, user) },
            )
        } else {
            resolveInlineRestrictions(context, page)
        }
        val access = pageAccess ?: spaceAccess[spaceKey]
        if (access == null || spaceKey in unresolvedSpaces || pageAccess == PRIVATE_ACCESS) {
            unresolvedPermission(pageUrl, "Confluence could not determine document permissions")
        } else {
            PermissionResolution(access)
        }
    } catch (error: Exception) {
        unresolvedPermission(
            pageUrl,
            "Confluence could not determine document permissions: ${error.message ?: error::class.simpleName}",
        )
    }

    private fun unresolvedPermission(pageUrl: String, message: String): PermissionResolution = PermissionResolution(
        PRIVATE_ACCESS,
        ConnectorFailure(
            FailureTarget.Document(pageUrl, pageUrl),
            message,
            "confluence_permission_unresolved",
        ),
    )

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
            item.deepCopy<JsonNode>().also { expandNested(context, it, limit) }
        }

    internal fun retrieveUsers(config: JsonNode?, credentials: JsonNode): List<ConfluenceUser> {
        val overrides = config?.path("confluence_user_profiles_override")?.takeIf(JsonNode::isArray)
        if (overrides != null && !overrides.isEmpty) {
            return overrides.map { user ->
                ConfluenceUser(
                    user.path("user_id").asText(),
                    user.path("username").asText().takeIf(String::isNotBlank),
                    user.path("display_name").asText(),
                    user.path("email").asText().takeIf(String::isNotBlank),
                    user.path("type").asText("override"),
                )
            }
        }
        val context = context(config, credentials)
        return if (context.isCloud) {
            paginate(context, "/rest/api/search/user?cql=${query("type=user")}", DEFAULT_PAGE_SIZE, forceOffsetPagination = true)
                .mapNotNull { result ->
                    val user = result.path("user")
                    user.path("accountId").asText().takeIf(String::isNotBlank)?.let { id ->
                        ConfluenceUser(
                            id,
                            null,
                            user.path("displayName").asText(id),
                            user.path("email").asText().takeIf(String::isNotBlank),
                            user.path("accountType").asText("atlassian"),
                        )
                    }
                }
        } else {
            paginate(context, "/rest/api/user/list", DEFAULT_PAGE_SIZE).mapNotNull { user ->
                user.path("userKey").asText().takeIf(String::isNotBlank)?.let { id ->
                    ConfluenceUser(
                        id,
                        user.path("username").asText().takeIf(String::isNotBlank),
                        user.path("displayName").asText(id),
                        user.path("email").asText().takeIf(String::isNotBlank),
                        user.path("type").asText("user"),
                    )
                }
            }
        }
    }

    private fun resolveUsernameEmail(context: Context, username: String): String? {
        val cache = context.userCaches.usernameEmails
        if (cache.containsKey(username)) return cache[username]
        val email = try {
            get(context, "/rest/api/user?username=${query(username)}").path("email").asText().takeIf(String::isNotBlank)
        } catch (_: Exception) {
            null
        }
        cache[username] = email
        return email
    }

    private fun resolveUserKeyEmail(context: Context, userKey: String): String? {
        val cache = context.userCaches.userKeyEmails
        if (cache.containsKey(userKey)) return cache[userKey]
        val email = try {
            get(context, "/rest/api/user?key=${query(userKey)}").path("email").asText().takeIf(String::isNotBlank)
        } catch (_: Exception) {
            null
        }
        cache[userKey] = email
        return email
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

    internal fun fetchContentReadRestrictions(config: JsonNode?, credentials: JsonNode, contentId: String): JsonNode? =
        fetchContentReadRestrictions(context(config, credentials), contentId)

    private fun fetchContentReadRestrictions(context: Context, contentId: String): JsonNode? = try {
        get(context, "/rest/api/content/${segment(contentId)}/restriction/byOperation")
    } catch (error: WebClientResponseException) {
        if (error.statusCode.value() in setOf(403, 404)) null else throw error
    }

    internal fun resolveRestrictions(
        pageRestrictions: JsonNode,
        ancestors: List<JsonNode>,
        cache: MutableMap<String, JsonNode?>,
        emailResolver: (JsonNode) -> String? = { user -> user.path("email").asText().takeIf(String::isNotBlank) },
        groupPrefix: String = "",
        fetch: (String) -> JsonNode?,
    ): ExternalAccess? {
        parseRestrictions(pageRestrictions, emailResolver, groupPrefix)?.let { return it }
        ancestors.asReversed().forEach { ancestor ->
            val id = ancestor.path("id").asText().takeIf(String::isNotBlank) ?: return@forEach
            val restrictions = if (cache.containsKey(id)) cache[id] else fetch(id).also { cache[id] = it }
            parseRestrictions(restrictions, emailResolver, groupPrefix)?.let { return it }
        }
        return null
    }

    private fun resolveInlineRestrictions(context: Context, page: JsonNode, groupPrefix: String = ""): ExternalAccess? {
        val resolver = { user: JsonNode -> resolveRestrictionEmail(context, user) }
        parseRestrictions(page.path("restrictions"), resolver, groupPrefix)?.let { return it }
        page.path("ancestors").toList().asReversed().forEach { ancestor ->
            parseRestrictions(ancestor.path("restrictions"), resolver, groupPrefix)?.let { return it }
        }
        return null
    }

    private fun parseRestrictions(
        restrictionPayload: JsonNode?,
        emailResolver: (JsonNode) -> String?,
        groupPrefix: String,
    ): ExternalAccess? {
        if (restrictionPayload == null || restrictionPayload.isMissingNode || restrictionPayload.isNull) return null
        val read = restrictionPayload.path("read")
        val restrictions = when {
            read.path("restrictions").isObject -> read.path("restrictions")
            restrictionPayload.path("restrictions").isObject -> restrictionPayload.path("restrictions")
            else -> restrictionPayload
        }
        val userRestrictions = restrictions.path("user").path("results").toList()
        val groupRestrictions = restrictions.path("group").path("results").toList()
        if (userRestrictions.isEmpty() && groupRestrictions.isEmpty()) return null
        val resolvedUsers = userRestrictions.map(emailResolver)
        val resolvedGroups = groupRestrictions.map { group ->
            group.path("name").asText().ifBlank { group.path("id").asText() }.takeIf(String::isNotBlank)
                ?.let { groupPrefix + it }
        }
        if (resolvedUsers.any { it == null } || resolvedGroups.any { it == null }) return PRIVATE_ACCESS
        return ExternalAccess(resolvedUsers.filterNotNull().toSet(), resolvedGroups.filterNotNull().toSet(), isPublic = false)
    }

    private fun resolveRestrictionEmail(context: Context, user: JsonNode): String? {
        user.path("email").asText().takeIf(String::isNotBlank)?.let { return it }
        val overrides = context.config?.path("confluence_user_profiles_override")?.takeIf(JsonNode::isArray)
        if (overrides != null) {
            val identifiers = setOf(
                user.path("accountId").asText(),
                user.path("userKey").asText(),
                user.path("key").asText(),
                user.path("username").asText(),
            ).filter(String::isNotBlank).toSet()
            overrides.firstOrNull { override ->
                override.path("user_id").asText() in identifiers || override.path("username").asText() in identifiers
            }?.path("email")?.asText()?.takeIf(String::isNotBlank)?.let { return it }
        }
        user.path("userKey").asText().takeIf(String::isNotBlank)?.let { return resolveUserKeyEmail(context, it) }
        user.path("key").asText().takeIf(String::isNotBlank)?.let { return resolveUserKeyEmail(context, it) }
        user.path("username").asText().takeIf(String::isNotBlank)?.let { return resolveUsernameEmail(context, it) }
        val identifier = user.path("accountId").asText().takeIf(String::isNotBlank) ?: return null
        return try {
            get(context, "/rest/api/user?accountId=${query(identifier)}").path("email").asText().takeIf(String::isNotBlank)
        } catch (_: Exception) {
            null
        }
    }

    internal fun retrieveSpaces(config: JsonNode?, credentials: JsonNode, limit: Int): List<JsonNode> =
        retrieveSpaces(context(config, credentials), limit)

    private fun retrieveSpaces(context: Context, limit: Int): List<JsonNode> {
        val cloudV2 = context.isCloud && !context.config.boolean("scoped_token", false)
        val initial = if (cloudV2) "/wiki/api/v2/spaces" else "/rest/api/space"
        return try {
            paginateSpaces(context, initial, limit, cloudV2)
        } catch (error: WebClientResponseException.NotFound) {
            if (cloudV2) paginateSpaces(context, "/rest/api/space", limit, false) else throw error
        }
    }

    private fun paginateSpaces(context: Context, initialPath: String, limit: Int, cloudV2: Boolean): List<JsonNode> {
        val output = mutableListOf<JsonNode>()
        var start = 0
        var path: String? = updateQuery(initialPath, "limit", limit.toString())
        if (!cloudV2) path = updateQuery(requireNotNull(path), "start", "0")
        while (path != null) {
            val response = get(context, path)
            val results = response.path("results").takeIf(JsonNode::isArray)?.toList().orEmpty()
            if (results.isEmpty()) break
            output += results
            require(output.size <= MAX_PAGINATED_RESULTS) { "Confluence space pagination exceeded $MAX_PAGINATED_RESULTS results" }
            val next = response.path("_links").path("next").asText().takeIf(String::isNotBlank) ?: break
            path = if (cloudV2) {
                next
            } else {
                start += results.size
                updateQuery(initialPath, "limit", limit.toString()).let { updateQuery(it, "start", start.toString()) }
            }
        }
        return output
    }

    internal fun supportsRestSpacePermissions(config: JsonNode?, credentials: JsonNode): Boolean =
        supportsRestSpacePermissions(context(config, credentials))

    private fun supportsRestSpacePermissions(context: Context): Boolean {
        if (context.isCloud) return false
        val key = context.apiBase
        val version = synchronized(serverVersionCache) {
            if (serverVersionCache.containsKey(key)) return@synchronized serverVersionCache[key]
            val resolved = try {
                get(context, "/rest/api/server-information").path("version").asText().parseVersion()
            } catch (_: Exception) {
                null
            }
            serverVersionCache[key] = resolved
            resolved
        }
        return version != null && (version.first > 9 || version.first == 9 && version.second >= 1)
    }

    internal fun getAllSpacePermissionsServerRest(config: JsonNode?, credentials: JsonNode, spaceKey: String): List<JsonNode> =
        getAllSpacePermissionsServerRest(context(config, credentials), spaceKey)

    private fun getAllSpacePermissionsServerRest(context: Context, spaceKey: String): List<JsonNode> = try {
        val response = get(context, "/rest/api/space/${segment(spaceKey)}/permissions")
        if (response.isArray) response.toList() else emptyList()
    } catch (error: WebClientResponseException) {
        when (error.statusCode.value()) {
            404 -> throw ConfluenceRestSpacePermissionsNotAvailableException(
                "REST space-permissions endpoint is unavailable for '$spaceKey'; Confluence Data Center 9.1+ is required.",
            )
            500 -> throw IllegalArgumentException(
                "CONFSERVER-99908: Confluence returned HTTP 500 for space '$spaceKey'. Grant the bot account admin permissions.",
                error,
            )
            else -> throw error
        }
    }

    internal fun getAllSpacePermissionsServerJsonRpc(config: JsonNode?, credentials: JsonNode, spaceKey: String): List<JsonNode> =
        getAllSpacePermissionsServerJsonRpc(context(config, credentials), spaceKey)

    private fun getAllSpacePermissionsServerJsonRpc(context: Context, spaceKey: String): List<JsonNode> {
        val response = http.postText(
            context.apiBase,
            "/rpc/json-rpc/confluenceservice-v2",
            context.headers,
            mapOf("jsonrpc" to "2.0", "method" to "getSpacePermissionSets", "id" to 7, "params" to listOf(spaceKey)),
        )
        if (response.statusCode >= 400) throw IllegalArgumentException("Confluence JSON-RPC failed with HTTP ${response.statusCode}.")
        val payload = try {
            mapper.readTree(response.body)
        } catch (_: Exception) {
            throw IllegalArgumentException(
                "Confluence JSON-RPC returned a non-JSON response for space '$spaceKey' " +
                    "(HTTP ${response.statusCode}, Content-Type=${response.contentType ?: "<unset>"}). " +
                    "Secure Administrator Sessions (WebSudo) can cause this response. " +
                    "$WEBSUDO_KB_URL Response body: ${response.body.take(1_000)}",
            )
        }
        return payload.path("result").takeIf(JsonNode::isArray)?.toList().orEmpty()
    }

    private fun allSpacePermissions(
        context: Context,
        prefixGroups: Boolean,
        unresolvedSpaces: MutableSet<String>? = null,
    ): Map<String, ExternalAccess> =
        retrieveSpaces(context, SPACE_PAGE_SIZE).associate { space ->
            val key = space.path("key").asText().ifBlank { space.path("id").asText() }
            val access = try {
                val rows = if (context.isCloud) {
                    val response = get(context, "/rest/api/space/${segment(key)}?expand=permissions")
                    response.path("permissions").let { permissions ->
                        when {
                            permissions.isArray -> permissions.toList()
                            permissions.path("results").isArray -> permissions.path("results").toList()
                            else -> emptyList()
                        }
                    }
                } else if (supportsRestSpacePermissions(context)) {
                    getAllSpacePermissionsServerRest(context, key)
                } else {
                    getAllSpacePermissionsServerJsonRpc(context, key)
                }
                val resolution = parseSpacePermissions(
                    context,
                    rows,
                    if (prefixGroups) CONFLUENCE_GROUP_PREFIX else "",
                )
                if (resolution.hasUnresolvedSubjects) unresolvedSpaces?.add(key)
                resolution.access
            } catch (_: Exception) {
                unresolvedSpaces?.add(key)
                PRIVATE_ACCESS
            }
            key to access
        }

    private fun parseSpacePermissions(
        context: Context,
        rows: List<JsonNode>,
        groupPrefix: String,
    ): SpacePermissionResolution {
        val emails = mutableSetOf<String>()
        val groups = mutableSetOf<String>()
        var isPublic = false
        var hasUnresolvedSubjects = false
        rows.forEach { row ->
            val operation = row.path("operation")
            val key = operation.path("operationKey").asText().ifBlank { operation.path("operation").asText() }
            if (key.isNotBlank() && !key.equals("read", true) && !key.equals("view", true)) return@forEach
            val subject = row.path("subject")
            when (subject.path("type").asText().lowercase()) {
                "anonymous", "anyone" -> isPublic = true
                "group" -> {
                    val group = subject.path("name").asText().ifBlank { subject.path("id").asText() }
                        .takeIf(String::isNotBlank)
                    if (group == null) hasUnresolvedSubjects = true else groups += groupPrefix + group
                }
                "user" -> {
                    val email = subject.path("email").asText().ifBlank { subject.path("emailAddress").asText() }
                        .takeIf(String::isNotBlank)
                        ?: subject.path("userKey").asText().takeIf(String::isNotBlank)
                            ?.let { resolveUserKeyEmail(context, it) }
                    if (email == null) hasUnresolvedSubjects = true else emails += email
                }
            }
            row.path("subjects").path("group").path("results").forEach { group ->
                val name = group.path("name").asText().ifBlank { group.path("id").asText() }
                    .takeIf(String::isNotBlank)
                if (name == null) hasUnresolvedSubjects = true else groups += groupPrefix + name
            }
            row.path("subjects").path("user").path("results").forEach { user ->
                val email = resolveRestrictionEmail(context, user)
                if (email == null) hasUnresolvedSubjects = true else emails += email
            }
            if (row.path("anonymousAccess").asBoolean(false)) isPublic = true
        }
        return if (hasUnresolvedSubjects) {
            SpacePermissionResolution(PRIVATE_ACCESS, hasUnresolvedSubjects = true)
        } else {
            SpacePermissionResolution(ExternalAccess(emails, groups, isPublic))
        }
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

    private fun pageExpand(includePermissions: Boolean): String =
        if (includePermissions) "$PAGE_EXPAND,$RESTRICTIONS_EXPAND" else PAGE_EXPAND

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
        val usernameEmails = mutableMapOf<String, String?>()
        val userKeyEmails = mutableMapOf<String, String?>()
        val displayNames = mutableMapOf<String, String?>()
    }

    private data class PageFetch(val results: List<JsonNode>, val nextPath: String?, val effectiveLimit: Int)
    private data class ProcessResult(
        val documents: List<SourceDocument> = emptyList(),
        val failures: List<ConnectorFailure> = emptyList(),
    )
    private data class PermissionResolution(
        val access: ExternalAccess?,
        val failure: ConnectorFailure? = null,
    )
    private data class SpacePermissionResolution(
        val access: ExternalAccess,
        val hasUnresolvedSubjects: Boolean = false,
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
        const val CONFLUENCE_GROUP_PREFIX = "confluence_"
        const val PROBLEMATIC_BODY_EXPAND = "body.storage.value"
        const val REPLACEMENT_BODY_EXPAND = "body.view.value"
        const val ANCESTOR_RESTRICTIONS_EXPAND = "ancestors.restrictions.read.restrictions."
        const val COMMENT_EXPAND = "body.storage.value"
        const val PAGE_EXPAND = "body.storage.value,version,space,metadata.labels,history.lastUpdated,ancestors"
        const val ATTACHMENT_EXPAND = "version,space,metadata.labels,history"
        const val RESTRICTIONS_EXPAND =
            "space,restrictions.read.restrictions.user,restrictions.read.restrictions.group," +
                "ancestors.restrictions.read.restrictions.user,ancestors.restrictions.read.restrictions.group,history"
        const val PER_PAGE_RESTRICTIONS_EXPAND =
            "space,restrictions.read.restrictions.user,restrictions.read.restrictions.group,ancestors,history"
        const val PRUNING_EXPAND = "space,ancestors,history"
        const val WEBSUDO_KB_URL =
            "https://support.atlassian.com/confluence/kb/json-rpc-api-request-returns-websudorequiredexception-on-confluence/"

        val PRIVATE_ACCESS = ExternalAccess(isPublic = false)
        val SERVER_ERROR_CODES = setOf(500, 502, 503, 504)
        val CONFCLOUD_77618_SIGNATURES = listOf(
            "No content with id",
            "Cannot find content. Outdated version/old_draft/trashed",
        )
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
