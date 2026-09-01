package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.onyx.foss.kotlin.domain.ConnectorSource
import org.springframework.core.codec.DecodingException
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.util.UriUtils
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Base64

data class JiraCheckpoint(
    val hasMore: Boolean = true,
    val allIssueIds: List<List<String>> = emptyList(),
    val idsDone: Boolean = false,
    val cursor: String? = null,
    val offset: Int? = null,
    val seenHierarchyNodeIds: Set<String> = emptySet(),
)

@Service
class JiraConnectorLoader(
    private val http: RemoteJsonClient,
    private val mapper: ObjectMapper,
) {
    private companion object {
        const val DEFAULT_PAGE_SIZE = 50
        const val CLOUD_ID_PAGE_SIZE = 5_000
        const val DEFAULT_MAX_TICKET_BYTES = 100 * 1024
        const val ISSUE_FIELDS =
            "summary,description,comment,updated,created,labels,reporter,assignee,priority,status,resolution,resolutiondate,duedate,issuetype,parent,project"
        const val SLIM_ISSUE_FIELDS = "key,created,issuetype,parent,project"
    }

    fun load(
        config: JsonNode?,
        credentials: JsonNode,
        checkpointNode: JsonNode?,
        permissionSync: Boolean = false,
        start: Instant? = null,
        end: Instant? = null,
    ): Sequence<ConnectorBatch> = loadInternal(
        config,
        credentials,
        checkpointNode,
        permissionSync,
        start,
        end,
        slim = false,
    )

    private fun loadInternal(
        config: JsonNode?,
        credentials: JsonNode,
        checkpointNode: JsonNode?,
        permissionSync: Boolean,
        start: Instant?,
        end: Instant?,
        slim: Boolean,
    ): Sequence<ConnectorBatch> {
        val connection = connection(config, credentials)
        val savedCheckpoint = checkpointNode?.let { mapper.treeToValue(it, JiraCheckpoint::class.java) } ?: JiraCheckpoint()
        val checkpoint = if (savedCheckpoint.hasMore) savedCheckpoint else {
            JiraCheckpoint(seenHierarchyNodeIds = savedCheckpoint.seenHierarchyNodeIds)
        }
        val context = Context(
            config = config,
            jiraBase = connection.jiraBase,
            apiBase = connection.apiBase,
            apiVersion = connection.apiVersion,
            headers = connection.headers,
            jql = jql(config, start, end),
            pageSize = config?.path("batch_size")?.asInt(DEFAULT_PAGE_SIZE)?.coerceIn(1, 100) ?: DEFAULT_PAGE_SIZE,
            permissionSync = permissionSync,
            slim = slim,
        )
        return if (connection.apiVersion == 3) loadCloud(context, checkpoint) else loadServer(context, checkpoint)
    }

    fun validate(config: JsonNode?, credentials: JsonNode) {
        val connection = connection(config, credentials)
        val project = config?.text("project_key")
        val customJql = config?.text("jql_query")
        val path = when {
            customJql != null && connection.apiVersion == 3 ->
                "/rest/api/3/search/jql?jql=${query(customJql)}&maxResults=1&fields=id"
            customJql != null -> "/rest/api/2/search" +
                "?jql=${query(customJql)}&startAt=0&maxResults=1&fields=key"
            project != null -> "/rest/api/${connection.apiVersion}/project/${segment(project)}"
            else -> "/rest/api/${connection.apiVersion}/project?maxResults=1"
        }
        try {
            http.get(connection.apiBase, path, connection.headers)
        } catch (error: WebClientResponseException) {
            throw validationError(error)
        }
    }

    fun retrieveAllSlimDocuments(
        config: JsonNode?,
        credentials: JsonNode,
        start: Instant? = null,
        end: Instant? = null,
        includePermissions: Boolean = false,
    ): Sequence<ConnectorBatch> {
        val effectiveConfig = (config?.deepCopy<ObjectNode>() ?: mapper.createObjectNode())
            .put("include_permissions", includePermissions)
        return loadInternal(
            effectiveConfig,
            credentials,
            checkpointNode = null,
            permissionSync = includePermissions,
            start = start,
            end = end,
            slim = true,
        )
    }

    private fun loadServer(context: Context, initial: JiraCheckpoint): Sequence<ConnectorBatch> = sequence {
        var offset = initial.offset ?: 0
        val seenHierarchyNodeIds = initial.seenHierarchyNodeIds.toMutableSet()
        while (true) {
            val response = getSearch(
                context,
                "/rest/api/2/search?jql=${query(context.jql)}&startAt=$offset" +
                    "&maxResults=${context.pageSize}&fields=${query(context.issueFields)}",
            )
            val issues = response.path("issues").toList()
            val result = processIssues(context, issues, seenHierarchyNodeIds)
            val nextOffset = offset + issues.size
            val total = response.path("total").takeIf { it.isNumber }?.asInt()
                ?: nextOffset + if (issues.size == context.pageSize) 1 else 0
            val hasMore = issues.isNotEmpty() && nextOffset < total
            val checkpoint = JiraCheckpoint(
                hasMore = hasMore,
                offset = nextOffset,
                seenHierarchyNodeIds = seenHierarchyNodeIds.toSet(),
            )
            yield(result.toBatch(checkpoint))
            if (!hasMore) break
            offset = nextOffset
        }
    }

    private fun loadCloud(context: Context, initial: JiraCheckpoint): Sequence<ConnectorBatch> = sequence {
        val pendingIds = initial.allIssueIds.toMutableList()
        val seenHierarchyNodeIds = initial.seenHierarchyNodeIds.toMutableSet()
        var cursor = initial.cursor
        var idsDone = initial.idsDone
        while (pendingIds.isNotEmpty() || !idsDone) {
            if (pendingIds.isEmpty()) {
                val cursorQuery = cursor?.let { "&nextPageToken=${query(it)}" }.orEmpty()
                val response = getSearch(
                    context,
                    "/rest/api/3/search/jql?jql=${query(context.jql)}&maxResults=$CLOUD_ID_PAGE_SIZE" +
                        "&fields=id$cursorQuery",
                )
                response.path("issues").mapNotNull { issue -> issue.path("id").asText().takeIf(String::isNotBlank) }
                    .chunked(context.pageSize)
                    .forEach(pendingIds::add)
                cursor = response.path("nextPageToken").asText().takeIf(String::isNotBlank)
                idsDone = cursor == null
                if (pendingIds.isEmpty()) {
                    val checkpoint = JiraCheckpoint(
                        hasMore = !idsDone,
                        idsDone = idsDone,
                        cursor = cursor,
                        seenHierarchyNodeIds = seenHierarchyNodeIds.toSet(),
                    )
                    yield(ProcessResult().toBatch(checkpoint))
                    if (idsDone) break
                    continue
                }
            }

            val bulkResult = bulkFetch(context, pendingIds.removeAt(0))
            val processed = processIssues(context, bulkResult.issues, seenHierarchyNodeIds)
            val result = processed.copy(
                failures = processed.failures + bulkResult.failures,
                enumerationComplete = processed.enumerationComplete && bulkResult.failures.isEmpty(),
            )
            val checkpoint = JiraCheckpoint(
                hasMore = pendingIds.isNotEmpty() || !idsDone,
                allIssueIds = pendingIds.map { it.toList() },
                idsDone = idsDone,
                cursor = cursor,
                seenHierarchyNodeIds = seenHierarchyNodeIds.toSet(),
            )
            yield(result.toBatch(checkpoint))
        }
    }

    private fun bulkFetch(context: Context, issueIds: List<String>): BulkFetchResult = try {
        val response = http.post(
            context.apiBase,
            "/rest/api/3/issue/bulkfetch",
            context.headers,
            mapOf("issueIdsOrKeys" to issueIds, "fields" to context.issueFields.split(',')),
        )
        val issues = response.path("issues").toList()
        val returnedIds = issues.flatMapTo(mutableSetOf()) { issue ->
            listOf(issue.path("id").asText(), issue.path("key").asText()).filter(String::isNotBlank)
        }
        val issueErrors = response.path("issueErrors").toList()
        val errorById = issueErrors.associateBy { error ->
            listOf("issueId", "issueIdOrKey", "id", "key")
                .firstNotNullOfOrNull { field -> error.path(field).asText().takeIf(String::isNotBlank) }
                .orEmpty()
        }
        val missingIds = issueIds.filterNot(returnedIds::contains)
        val failures = missingIds.map { issueId ->
            ConnectorFailure(
                target = FailureTarget.Entity("jira_issue:$issueId"),
                message = issueErrorMessage(errorById[issueId], issueId),
                errorType = "jira_bulk_fetch",
            )
        }.toMutableList()
        issueErrors.filter { error ->
            val id = listOf("issueId", "issueIdOrKey", "id", "key")
                .firstNotNullOfOrNull { field -> error.path(field).asText().takeIf(String::isNotBlank) }
            id == null || id !in missingIds
        }.forEach { error ->
            val issueId = listOf("issueId", "issueIdOrKey", "id", "key")
                .firstNotNullOfOrNull { field -> error.path(field).asText().takeIf(String::isNotBlank) }
            failures += ConnectorFailure(
                target = FailureTarget.Entity(issueId?.let { "jira_issue:$it" } ?: "jira_issue:unknown"),
                message = issueErrorMessage(error, issueId ?: "unknown"),
                errorType = "jira_bulk_fetch",
            )
        }
        BulkFetchResult(issues, failures)
    } catch (error: WebClientResponseException) {
        throw searchError(error, context.jql)
    } catch (error: RuntimeException) {
        if (issueIds.size <= 1 || !error.isJsonDecodeFailure()) throw error
        val middle = issueIds.size / 2
        val left = bulkFetch(context, issueIds.subList(0, middle))
        val right = bulkFetch(context, issueIds.subList(middle, issueIds.size))
        BulkFetchResult(left.issues + right.issues, left.failures + right.failures)
    }

    private fun issueErrorMessage(error: JsonNode?, issueId: String): String {
        val details = error?.path("errorMessages")?.map(JsonNode::asText).orEmpty() +
            listOfNotNull(error?.path("errorMessage")?.asText()?.takeIf(String::isNotBlank))
        return details.filter(String::isNotBlank).joinToString("; ")
            .ifBlank { "Jira bulk fetch did not return requested issue $issueId" }
    }

    private fun processIssues(
        context: Context,
        issues: List<JsonNode>,
        seenHierarchyNodeIds: MutableSet<String>,
    ): ProcessResult {
        if (context.slim) return processSlimIssues(context, issues, seenHierarchyNodeIds)
        val documents = mutableListOf<SourceDocument>()
        val failures = mutableListOf<ConnectorFailure>()
        var enumerationComplete = true
        val permissionCache = mutableMapOf<String, ExternalAccess>()
        issues.forEach { issue ->
            val key = issue.path("key").asText().ifBlank { issue.path("id").asText().ifBlank { "unknown" } }
            try {
                val document = convertIssue(context, issue, seenHierarchyNodeIds)
                if (document == null) {
                    enumerationComplete = false
                    return@forEach
                }
                val projectKey = document.metadata["project"]?.toString()
                val access = if (context.config?.path("include_permissions")?.asBoolean(false) == true && projectKey != null) {
                    permissionCache.getOrPut(projectKey) { projectAccess(context, projectKey) }
                } else {
                    null
                }
                documents += document.copy(externalAccess = access)
            } catch (error: Exception) {
                val link = context.jiraBase + "/browse/" + segment(key)
                failures += ConnectorFailure(
                    target = if (key == "unknown") FailureTarget.Entity("jira-issue:unknown") else FailureTarget.Document(link, link),
                    message = "Failed to process Jira issue: ${error.message ?: error::class.simpleName}",
                    errorType = "jira_issue_processing",
                )
            }
        }
        return ProcessResult(documents, failures, enumerationComplete)
    }

    private fun processSlimIssues(
        context: Context,
        issues: List<JsonNode>,
        seenHierarchyNodeIds: MutableSet<String>,
    ): ProcessResult {
        val documents = mutableListOf<SourceDocument>()
        val failures = mutableListOf<ConnectorFailure>()
        val permissionCache = mutableMapOf<String, ExternalAccess>()
        issues.forEach { issue ->
            val key = issue.path("key").asText().ifBlank { issue.path("id").asText().ifBlank { "unknown" } }
            try {
                require(key != "unknown") { "Jira issue key is missing" }
                val fields = issue.path("fields")
                require(fields.isObject) { "Jira issue $key has no fields" }
                val projectKey = fields.path("project").path("key").asText()
                val parent = fields.path("parent")
                val parentKey = parent.path("key").asText().takeIf(String::isNotBlank)
                val parentIsEpic = parent.path("fields").path("issuetype").path("name").asText().equals("epic", true)
                if (projectKey.isNotBlank()) seenHierarchyNodeIds += projectKey
                if (parentIsEpic && parentKey != null) seenHierarchyNodeIds += parentKey
                if (fields.path("issuetype").path("name").asText().equals("epic", true)) seenHierarchyNodeIds += key
                val link = context.jiraBase + "/browse/" + segment(key)
                val access = if (context.config?.path("include_permissions")?.asBoolean(false) == true && projectKey.isNotBlank()) {
                    permissionCache.getOrPut(projectKey) { projectAccess(context, projectKey) }
                } else {
                    null
                }
                documents += SourceDocument(
                    id = link,
                    title = link,
                    content = "",
                    link = link,
                    metadata = mapOf(
                        "source" to "jira",
                        "project" to projectKey,
                        "parent_hierarchy_raw_node_id" to if (parentIsEpic) parentKey else projectKey,
                    ),
                    externalAccess = access,
                    source = ConnectorSource.JIRA,
                )
            } catch (error: Exception) {
                val link = context.jiraBase + "/browse/" + segment(key)
                failures += ConnectorFailure(
                    target = if (key == "unknown") FailureTarget.Entity("jira-issue:unknown") else FailureTarget.Document(link, link),
                    message = "Failed to process Jira issue: ${error.message ?: error::class.simpleName}",
                    errorType = "jira_issue_processing",
                )
            }
        }
        return ProcessResult(documents, failures)
    }

    private fun convertIssue(
        context: Context,
        issue: JsonNode,
        seenHierarchyNodeIds: MutableSet<String>,
    ): SourceDocument? {
        val key = issue.path("key").asText()
        require(key.isNotBlank()) { "Jira issue key is missing" }
        val fields = issue.path("fields")
        require(fields.isObject) { "Jira issue $key has no fields" }
        val labels = fields.path("labels").map(JsonNode::asText)
        if (labels.any(context.labelsToSkip::contains)) return null

        val description = adfText(fields.path("description"))
        val comments = fields.path("comment").path("comments").mapNotNull { comment ->
            val authorEmail = comment.path("author").path("emailAddress").asText()
            if (authorEmail in context.commentEmailBlacklist) null else adfText(comment.path("body")).takeIf(String::isNotBlank)
        }
        val content = buildString {
            append(description)
            append('\n')
            comments.forEach { append("Comment: ").append(it).append('\n') }
        }
        val maxBytes = context.config?.path("max_ticket_size_bytes")?.asInt(DEFAULT_MAX_TICKET_BYTES)
            ?: DEFAULT_MAX_TICKET_BYTES
        if (content.toByteArray(StandardCharsets.UTF_8).size > maxBytes) return null

        val project = fields.path("project")
        val projectKey = project.path("key").asText()
        val parent = fields.path("parent")
        val parentKey = parent.path("key").asText().takeIf(String::isNotBlank)
        val parentIsEpic = parent.path("fields").path("issuetype").path("name").asText().equals("epic", true)
        if (projectKey.isNotBlank()) seenHierarchyNodeIds += projectKey
        if (parentIsEpic && parentKey != null) seenHierarchyNodeIds += parentKey
        if (fields.path("issuetype").path("name").asText().equals("epic", true)) seenHierarchyNodeIds += key

        val metadata = linkedMapOf<String, Any?>(
            "source" to "jira",
            "key" to key,
            "updated" to fields.path("updated").asText(),
            "labels" to labels,
        )
        addText(metadata, "created", fields.path("created"))
        addText(metadata, "duedate", fields.path("duedate"))
        addText(metadata, "resolution_date", fields.path("resolutiondate"))
        addNamed(metadata, "priority", fields.path("priority"))
        addNamed(metadata, "status", fields.path("status"))
        addNamed(metadata, "resolution", fields.path("resolution"))
        addNamed(metadata, "issuetype", fields.path("issuetype"))
        if (projectKey.isNotBlank()) metadata["project"] = projectKey
        addText(metadata, "project_name", project.path("name"))
        if (parentKey != null) metadata["parent"] = parentKey
        metadata["parent_hierarchy_raw_node_id"] = if (parentIsEpic) parentKey else projectKey.takeIf(String::isNotBlank)

        val reporter = fields.path("reporter")
        val assignee = fields.path("assignee")
        addPerson(metadata, "reporter", reporter)
        addPerson(metadata, "assignee", assignee)
        val summary = fields.path("summary").asText(key)
        val link = context.jiraBase + "/browse/" + segment(key)
        return SourceDocument(
            id = link,
            title = "$key $summary",
            content = content.ifBlank { summary },
            link = link,
            metadata = metadata,
            source = ConnectorSource.JIRA,
            updatedAt = parseInstant(fields.path("updated").asText()),
            primaryOwners = listOf(reporter, assignee).mapNotNull { person ->
                person.path("displayName").asText().takeIf(String::isNotBlank)
                    ?: person.path("emailAddress").asText().takeIf(String::isNotBlank)
            }.distinct(),
        )
    }

    private fun projectAccess(context: Context, projectKey: String): ExternalAccess {
        val response = http.get(
            context.apiBase,
            "/rest/api/${context.apiVersion}/project/${segment(projectKey)}/permissionscheme" +
                "?expand=permissions,user,group,projectRole,applicationRole",
            context.headers,
        )
        val emails = mutableSetOf<String>()
        val groups = mutableSetOf<String>()
        var isPublic = false
        response.path("permissions").filter { it.path("permission").asText() == "BROWSE_PROJECTS" }.forEach { permission ->
            val holder = permission.path("holder")
            when (holder.path("type").asText().lowercase()) {
                "applicationrole", "anyone" -> isPublic = true
                "group" -> holder.path("parameter").asText().ifBlank { holder.path("group").path("name").asText() }
                    .takeIf(String::isNotBlank)?.let(groups::add)
                "user" -> holder.path("user").path("emailAddress").asText()
                    .ifBlank { holder.path("parameter").asText().takeIf { it.contains('@') }.orEmpty() }
                    .takeIf(String::isNotBlank)?.let(emails::add)
                "projectrole" -> {
                    val roleId = holder.path("parameter").asText()
                    if (roleId.isNotBlank()) {
                        val role = http.get(
                            context.apiBase,
                            "/rest/api/${context.apiVersion}/project/${segment(projectKey)}/role/${segment(roleId)}",
                            context.headers,
                        )
                        role.path("actors").forEach { actor ->
                            when {
                                actor.path("type").asText().contains("group", true) ->
                                    actor.path("name").asText().ifBlank { actor.path("actorGroup").path("name").asText() }
                                        .takeIf(String::isNotBlank)?.let(groups::add)
                                actor.path("type").asText().contains("user", true) ->
                                    actor.path("actorUser").path("emailAddress").asText()
                                        .ifBlank { actor.path("user").path("emailAddress").asText() }
                                        .takeIf(String::isNotBlank)?.let(emails::add)
                            }
                        }
                    }
                }
            }
        }
        val outputGroups = if (context.permissionSync) groups else groups.mapTo(mutableSetOf()) { "jira_${it.lowercase()}" }
        return ExternalAccess(emails, outputGroups, isPublic)
    }

    private fun getSearch(context: Context, path: String): JsonNode = try {
        http.get(context.apiBase, path, context.headers)
    } catch (error: WebClientResponseException) {
        throw searchError(error, context.jql)
    }

    private fun searchError(error: WebClientResponseException, jql: String): RuntimeException {
        val detail = error.responseBodyAsString
        return when (error.statusCode.value()) {
            400 -> if (detail.contains("does not exist for the field 'project'")) {
                IllegalArgumentException("The specified Jira project does not exist or you don't have access to it. JQL query: $jql. Error: $detail")
            } else {
                IllegalArgumentException("Invalid JQL query. JQL: $jql. Error: $detail")
            }
            401 -> IllegalArgumentException("Jira credentials are expired or invalid (HTTP 401).")
            403 -> IllegalArgumentException("Insufficient permissions to execute JQL query. JQL: $jql")
            429 -> IllegalArgumentException("Jira rate-limits were exceeded. Please try again later.")
            else -> error
        }
    }

    private fun validationError(error: WebClientResponseException): IllegalArgumentException = when (error.statusCode.value()) {
        401 -> IllegalArgumentException("Jira credential appears to be expired or invalid (HTTP 401).")
        403 -> IllegalArgumentException("Your Jira token does not have sufficient permissions for this configuration (HTTP 403).")
        429 -> IllegalArgumentException("Validation failed due to Jira rate-limits being exceeded. Please try again later.")
        404 -> IllegalArgumentException("Unexpected Jira error during validation (HTTP 404).")
        else -> IllegalArgumentException("Validation failed due to Jira error: ${error.responseBodyAsString}")
    }

    private fun jql(config: JsonNode?, start: Instant?, end: Instant?): String {
        val timeJql = listOfNotNull(
            start?.let { "updated >= ${it.toEpochMilli()}" },
            end?.let { "updated <= ${it.toEpochMilli()}" },
        ).joinToString(" AND ")
        val customJql = config?.text("jql_query")
        val projectJql = config?.text("project_key")?.let { "project = \"$it\"" }
        return when {
            customJql != null && timeJql.isNotEmpty() -> "($customJql) AND $timeJql"
            customJql != null -> customJql
            projectJql != null && timeJql.isNotEmpty() -> "$projectJql AND $timeJql"
            projectJql != null -> projectJql
            timeJql.isNotEmpty() -> timeJql
            else -> "order by updated asc"
        }
    }

    private fun connection(config: JsonNode?, credentials: JsonNode): JiraConnection {
        val jiraBase = required(config, "jira_base_url", "base_url").trimEnd('/')
        val headers = auth(credentials)
        val cloud = credentials.firstText("jira_user_email", "jira_email", "email") != null ||
            config?.path("scoped_token")?.asBoolean(false) == true || config?.path("is_cloud")?.asBoolean(false) == true
        val apiBase = if (config?.path("scoped_token")?.asBoolean(false) == true) {
            val cloudId = http.get(jiraBase, "/_edge/tenant_info", headers).path("cloudId").asText()
            require(cloudId.isNotBlank()) { "Jira scoped token discovery did not return a cloudId" }
            "https://api.atlassian.com/ex/jira/" + segment(cloudId)
        } else {
            jiraBase
        }
        return JiraConnection(jiraBase, apiBase, if (cloud) 3 else 2, headers)
    }

    private fun auth(credentials: JsonNode): Map<String, String> {
        val token = credentials.firstText("jira_api_token", "api_token", "access_token", "token")
            ?: throw IllegalArgumentException("Connector credential does not contain a Jira API token")
        val email = credentials.firstText("jira_user_email", "jira_email", "email")
        val value = if (email == null) {
            "Bearer $token"
        } else {
            "Basic " + Base64.getEncoder().encodeToString("$email:$token".toByteArray(StandardCharsets.UTF_8))
        }
        return mapOf(HttpHeaders.AUTHORIZATION to value)
    }

    private val Context.labelsToSkip: Set<String> get() = config.stringSet("labels_to_skip")

    private val Context.commentEmailBlacklist: Set<String> get() = config.stringSet("comment_email_blacklist")

    private val Context.issueFields: String get() = if (slim) SLIM_ISSUE_FIELDS else ISSUE_FIELDS

    private fun JsonNode?.stringSet(name: String): Set<String> {
        val value = this?.path(name) ?: return emptySet()
        return if (value.isArray) value.mapNotNull { it.asText().trim().takeIf(String::isNotBlank) }.toSet()
        else value.asText().split(',').map(String::trim).filter(String::isNotBlank).toSet()
    }

    private fun required(node: JsonNode?, vararg names: String): String = node?.firstText(*names)
        ?: throw IllegalArgumentException("Connector configuration is missing ${names.first()}")

    private fun JsonNode.text(name: String): String? = path(name).asText().trim().takeIf(String::isNotBlank)

    private fun JsonNode.firstText(vararg names: String): String? = names.asSequence().mapNotNull { text(it) }.firstOrNull()

    private fun query(value: String): String = UriUtils.encodeQueryParam(value, StandardCharsets.UTF_8)

    private fun segment(value: String): String = UriUtils.encodePathSegment(value, StandardCharsets.UTF_8)

    private fun adfText(node: JsonNode): String = when {
        node.isTextual -> node.asText()
        node.isArray -> node.map(::adfText).filter(String::isNotBlank).joinToString(" ")
        node.isObject && node.path("type").asText() == "text" -> node.path("text").asText()
        node.isObject -> adfText(node.path("content"))
        else -> ""
    }

    private fun addText(metadata: MutableMap<String, Any?>, key: String, node: JsonNode) {
        node.asText().takeIf(String::isNotBlank)?.let { metadata[key] = it }
    }

    private fun addNamed(metadata: MutableMap<String, Any?>, key: String, node: JsonNode) =
        addText(metadata, key, node.path("name"))

    private fun addPerson(metadata: MutableMap<String, Any?>, key: String, person: JsonNode) {
        addText(metadata, key, person.path("displayName"))
        addText(metadata, "${key}_email", person.path("emailAddress"))
    }

    private fun parseInstant(value: String): Instant? = runCatching {
        val normalized = value.replace(Regex("([+-]\\d{2})(\\d{2})$"), "$1:$2")
        OffsetDateTime.parse(normalized).toInstant()
    }.getOrNull()

    private fun RuntimeException.isJsonDecodeFailure(): Boolean = generateSequence<Throwable>(this) { it.cause }
        .any { it is DecodingException || it is JsonProcessingException }

    private data class Context(
        val config: JsonNode?,
        val jiraBase: String,
        val apiBase: String,
        val apiVersion: Int,
        val headers: Map<String, String>,
        val jql: String,
        val pageSize: Int,
        val permissionSync: Boolean,
        val slim: Boolean,
    )

    private data class JiraConnection(
        val jiraBase: String,
        val apiBase: String,
        val apiVersion: Int,
        val headers: Map<String, String>,
    )

    private data class ProcessResult(
        val documents: List<SourceDocument> = emptyList(),
        val failures: List<ConnectorFailure> = emptyList(),
        val enumerationComplete: Boolean = true,
    )

    private data class BulkFetchResult(
        val issues: List<JsonNode>,
        val failures: List<ConnectorFailure>,
    )

    private fun ProcessResult.toBatch(checkpoint: JiraCheckpoint): ConnectorBatch = ConnectorBatch(
        documents = documents,
        failures = failures,
        checkpoint = ConnectorCheckpoint(
            value = mapper.valueToTree(checkpoint),
            hasMore = checkpoint.hasMore,
        ),
        enumerationComplete = enumerationComplete && failures.all { it.target is FailureTarget.Document },
    )
}
