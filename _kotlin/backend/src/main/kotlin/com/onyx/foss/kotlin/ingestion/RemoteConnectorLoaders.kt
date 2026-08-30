package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriUtils
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64

@Service
class RemoteConnectorLoaders(
    private val http: RemoteJsonClient,
) {
    fun load(source: String, config: JsonNode?, credentials: JsonNode, checkpoint: JsonNode?): List<SourceDocument> =
        when (source) {
            "jira" -> jira(config, credentials, checkpoint)
            "confluence" -> confluence(config, credentials, checkpoint)
            "github" -> github(config, credentials, checkpoint)
            else -> error("Unsupported remote connector: " + source)
        }

    private fun jira(config: JsonNode?, credentials: JsonNode, checkpoint: JsonNode?): List<SourceDocument> {
        val base = required(config, "jira_base_url", "base_url")
        val project = config?.text("project_key")
        val customJql = config?.text("jql_query")
        val jql = customJql ?: if (project.isNullOrBlank()) "order by updated asc" else "project = " + project + " order by updated asc"
        val headers = auth(credentials, basic = true)
        val result = mutableListOf<SourceDocument>()
        var start = 0
        while (true) {
            val path = "/rest/api/2/search?jql=" + query(jql) + "&startAt=" + start + "&maxResults=100&fields=summary,description,updated"
            val response = http.get(base, path, headers)
            val issues = response.path("issues")
            issues.forEach { issue ->
                val key = issue.path("key").asText()
                val fields = issue.path("fields")
                result += SourceDocument(
                    id = key,
                    title = key + ": " + fields.path("summary").asText(key),
                    content = adfText(fields.path("description")).ifBlank { fields.path("summary").asText() },
                    link = base.trimEnd('/') + "/browse/" + key,
                    metadata = mapOf("source" to "jira", "key" to key, "updated" to fields.path("updated").asText()),
                )
            }
            val pageSize = issues.size()
            val total = response.path("total").asInt(start + pageSize)
            start += pageSize
            if (pageSize == 0 || start >= total) break
        }
        return afterCheckpoint(result, checkpoint)
    }

    private fun confluence(config: JsonNode?, credentials: JsonNode, checkpoint: JsonNode?): List<SourceDocument> {
        val base = required(config, "wiki_base", "confluence_base_url", "base_url")
        val requested = config?.text("cql_query")
        val space = config?.text("space")
        val cql = requested ?: if (space.isNullOrBlank()) "type=page" else "type=page and space='" + space + "'"
        val headers = auth(credentials, basic = true)
        val result = mutableListOf<SourceDocument>()
        var start = 0
        while (true) {
            val path = "/rest/api/content/search?cql=" + query(cql) +
                "&expand=body.storage,version,space,_links&limit=50&start=" + start
            val response = http.get(base, path, headers)
            val pages = response.path("results")
            pages.forEach { page ->
                val pageId = page.path("id").asText()
                val webPath = page.path("_links").path("webui").asText()
                result += SourceDocument(
                    id = pageId,
                    title = page.path("title").asText(pageId),
                    content = stripHtml(page.path("body").path("storage").path("value").asText()),
                    link = if (webPath.isBlank()) null else base.trimEnd('/') + webPath,
                    metadata = mapOf(
                        "source" to "confluence",
                        "space" to page.path("space").path("key").asText(),
                        "updated" to page.path("version").path("when").asText(),
                    ),
                )
            }
            val pageSize = pages.size()
            val total = response.path("totalSize").asInt(start + pageSize)
            start += pageSize
            if (pageSize == 0 || start >= total) break
        }
        return afterCheckpoint(result, checkpoint)
    }

    private fun github(config: JsonNode?, credentials: JsonNode, checkpoint: JsonNode?): List<SourceDocument> {
        val base = config?.text("github_base_url") ?: "https://api.github.com"
        val owner = required(config, "repo_owner", "owner")
        val state = config?.text("state_filter") ?: "all"
        val headers = auth(credentials, basic = false) + mapOf("Accept" to "application/vnd.github+json")
        val configured = config?.text("repositories")?.split(",")?.map(String::trim)?.filter(String::isNotBlank).orEmpty()
        val repos = if (configured.isNotEmpty()) configured else paginate(base, "/users/" + segment(owner) + "/repos?per_page=100", headers)
            .map { it.path("name").asText() }.filter(String::isNotBlank)
        val result = mutableListOf<SourceDocument>()
        repos.forEach { repo ->
            if (config?.path("include_prs")?.asBoolean(true) != false) {
                githubItems(base, owner, repo, "pulls", state, headers, result, "pull_request")
            }
            if (config?.path("include_issues")?.asBoolean(false) == true) {
                githubItems(base, owner, repo, "issues", state, headers, result, "issue")
            }
            if (config?.path("include_files")?.asBoolean(false) == true) {
                githubFiles(base, owner, repo, config?.text("branch"), headers, result)
            }
        }
        return afterCheckpoint(result, checkpoint)
    }

    private fun githubItems(
        base: String,
        owner: String,
        repo: String,
        type: String,
        state: String,
        headers: Map<String, String>,
        output: MutableList<SourceDocument>,
        source: String,
    ) {
        paginate(
            base,
            "/repos/" + segment(owner) + "/" + segment(repo) + "/" + type + "?state=" + query(state) + "&per_page=100&sort=updated&direction=desc",
            headers,
        ).forEach { item ->
            if (source == "issue" && item.has("pull_request")) return@forEach
            val number = item.path("number").asInt()
            output += SourceDocument(
                id = owner + "/" + repo + "/" + source + "/" + number,
                title = item.path("title").asText(),
                content = item.path("body").asText().ifBlank { item.path("title").asText() },
                link = item.path("html_url").asText(null),
                metadata = mapOf("source" to "github", "repository" to owner + "/" + repo, "updated" to item.path("updated_at").asText()),
            )
        }
    }

    private fun githubFiles(
        base: String,
        owner: String,
        repo: String,
        branch: String?,
        headers: Map<String, String>,
        output: MutableList<SourceDocument>,
    ) {
        val ref = branch?.takeIf(String::isNotBlank) ?: "HEAD"
        val tree = http.get(
            base,
            "/repos/" + segment(owner) + "/" + segment(repo) + "/git/trees/" + query(ref) + "?recursive=1",
            headers,
        ).path("tree")
        tree.filter {
            it.path("type").asText() == "blob" &&
                (it.path("path").asText().endsWith(".md", true) ||
                    it.path("path").asText().endsWith(".txt", true) ||
                    it.path("path").asText().endsWith(".rst", true))
        }.take(500).forEach { entry ->
            val path = entry.path("path").asText()
            val encodedPath = path.split("/").joinToString("/") { segment(it) }
            val response = http.get(
                base,
                "/repos/" + segment(owner) + "/" + segment(repo) + "/contents/" + encodedPath + "?ref=" + query(ref),
                headers,
            )
            val raw = response.path("content").asText().replace("\n", "")
            if (raw.isBlank()) return@forEach
            val content = String(Base64.getDecoder().decode(raw), StandardCharsets.UTF_8)
            output += SourceDocument(
                id = owner + "/" + repo + "/file/" + path,
                title = repo + "/" + path,
                content = content,
                link = response.path("html_url").asText(null),
                metadata = mapOf("source" to "github", "repository" to owner + "/" + repo, "path" to path),
            )
        }
    }

    private fun paginate(base: String, initialPath: String, headers: Map<String, String>): List<JsonNode> {
        val output = mutableListOf<JsonNode>()
        var page = 1
        while (page <= 100) {
            val separator = if (initialPath.contains("?")) "&" else "?"
            val response = http.get(base, initialPath + separator + "page=" + page, headers)
            if (!response.isArray || response.isEmpty) break
            output.addAll(response.toList())
            if (response.size() < 100) break
            page += 1
        }
        return output
    }
    private fun afterCheckpoint(documents: List<SourceDocument>, checkpoint: JsonNode?): List<SourceDocument> {
        val since = checkpoint?.path("last_success_at")?.asText()?.takeIf(String::isNotBlank) ?: return documents
        val checkpointInstant = runCatching { Instant.parse(since) }.getOrNull() ?: return documents
        return documents.filter { document ->
            val updated = document.metadata["updated"]?.toString()?.takeIf(String::isNotBlank) ?: return@filter true
            runCatching { Instant.parse(updated).isAfter(checkpointInstant) }.getOrDefault(true)
        }
    }





    private fun auth(credentials: JsonNode, basic: Boolean): Map<String, String> {
        val token = credentials.firstText("jira_api_token", "api_token", "github_access_token", "access_token", "token")
            ?: error("Connector credential does not contain an access token")
        val username = credentials.firstText("jira_email", "email", "username")
        val value = if (basic && !username.isNullOrBlank()) {
            "Basic " + Base64.getEncoder().encodeToString((username + ":" + token).toByteArray(StandardCharsets.UTF_8))
        } else {
            "Bearer " + token
        }
        return mapOf(HttpHeaders.AUTHORIZATION to value)
    }

    private fun required(node: JsonNode?, vararg names: String): String =
        node?.firstText(*names) ?: error("Connector configuration is missing " + names.first())

    private fun JsonNode.text(name: String): String? = path(name).asText().takeIf(String::isNotBlank)

    private fun JsonNode.firstText(vararg names: String): String? =
        names.asSequence().mapNotNull { text(it) }.firstOrNull()

    private fun query(value: String): String = UriUtils.encodeQueryParam(value, StandardCharsets.UTF_8)

    private fun segment(value: String): String = UriUtils.encodePathSegment(value, StandardCharsets.UTF_8)

    private fun adfText(node: JsonNode): String = when {
        node.isTextual -> node.asText()
        node.isArray || node.isObject -> node.map(::adfText).joinToString(" ")
        else -> ""
    }

    private fun stripHtml(value: String): String = value.replace(Regex("<[^>]+>"), " ").replace(Regex("\\s+"), " ").trim()
}

@Service
class RemoteJsonClient(
    private val clientBuilder: WebClient.Builder,
) {
    fun get(base: String, path: String, headers: Map<String, String>): JsonNode =
        clientBuilder.build().get()
            .uri(base.trimEnd('/') + path)
            .accept(MediaType.APPLICATION_JSON)
            .headers { httpHeaders -> headers.forEach { (name, value) -> httpHeaders.set(name, value) } }
            .retrieve()
            .bodyToMono(JsonNode::class.java)
            .block() ?: error("Remote connector returned an empty response")
}
