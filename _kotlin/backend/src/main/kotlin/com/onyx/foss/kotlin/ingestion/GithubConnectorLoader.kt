package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorSource
import org.springframework.http.HttpHeaders
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.util.UriUtils
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Base64

enum class GithubStage {
    REPOSITORIES,
    PERMISSIONS,
    PULL_REQUESTS,
    ISSUES,
    FILES,
}

enum class GithubPermissionStage {
    COLLABORATORS,
    USERS,
    TEAMS,
}

data class GithubRepository(
    val id: Long,
    val name: String,
    val fullName: String,
    val isPrivate: Boolean,
    val htmlUrl: String,
    val defaultBranch: String,
    val pushedAt: String? = null,
)

data class GithubCheckpoint(
    val hasMore: Boolean = true,
    val stage: GithubStage = GithubStage.REPOSITORIES,
    val repositories: List<GithubRepository> = emptyList(),
    val repository: GithubRepository? = null,
    val repositoryIndex: Int = 0,
    val repositoryPage: Int = 1,
    val repositoryListingComplete: Boolean = false,
    val repositoryOwnerKind: String? = null,
    val permissionStage: GithubPermissionStage = GithubPermissionStage.COLLABORATORS,
    val permissionCollaboratorPage: Int = 1,
    val permissionUserLogins: List<String> = emptyList(),
    val permissionUserOffset: Int = 0,
    val permissionTeamPage: Int = 1,
    val permissionEmails: Set<String> = emptySet(),
    val permissionTeamIds: Set<String> = emptySet(),
    val pullRequestPage: Int = 1,
    val pullRequestCursor: String? = null,
    val pullRequestsRetrieved: Int = 0,
    val issuePage: Int = 1,
    val issueCursor: String? = null,
    val issuesRetrieved: Int = 0,
    val filePaths: List<String>? = null,
    val fileOffset: Int = 0,
    val branch: String? = null,
)

open class GithubConnectorValidationException(message: String, cause: Throwable? = null) :
    IllegalArgumentException(message, cause)

class GithubCredentialExpiredException(message: String, cause: Throwable? = null) :
    GithubConnectorValidationException(message, cause)

class GithubInsufficientPermissionsException(message: String, cause: Throwable? = null) :
    GithubConnectorValidationException(message, cause)

class GithubRateLimitValidationException(message: String, cause: Throwable? = null) :
    GithubConnectorValidationException(message, cause)

@Service
class GithubConnectorLoader(
    private val http: RemoteJsonClient,
    private val mapper: ObjectMapper,
    private val sleepMillis: (Long) -> Unit,
    private val nowSource: () -> Instant,
) {
    @Autowired
    constructor(http: RemoteJsonClient, mapper: ObjectMapper) : this(http, mapper, Thread::sleep, Instant::now)

    companion object {
        private const val PAGE_SIZE = 100
        private const val FILE_BATCH_SIZE = 100
        private const val MAX_FILE_SIZE_BYTES = 1_000_000
        private const val MAX_RATE_LIMIT_RETRIES = 5
        private const val MAX_RATE_LIMIT_WAIT_SECONDS = 60 * 60L
        private const val MAX_REPOSITORIES = 10_000
        private const val MAX_FILE_PATHS = 100_000
        private const val MAX_PERMISSION_ENTRIES = 5_000
        private const val MAX_CHECKPOINT_BYTES = 8 * 1024 * 1024
        private val INDEXABLE_EXTENSIONS = setOf("md", "mdx", "markdown", "rst", "txt")
        private val INDEXABLE_NAMES = setOf(
            "readme", "license", "licence", "changelog", "contributing", "authors", "notice", "copying",
            "install", "maintainers", "codeowners", "security", "support",
        )
        private val PATH_DENYLIST = setOf(".git", "node_modules", "vendor", "dist", "build", ".venv", "__pycache__")

        internal fun isIndexablePath(path: String, size: Int?): Boolean {
            if (size != null && size > MAX_FILE_SIZE_BYTES) return false
            val segments = path.split('/').map(String::lowercase)
            if (segments.any(PATH_DENYLIST::contains)) return false
            val basename = segments.lastOrNull().orEmpty()
            val extension = basename.substringAfterLast('.', missingDelimiterValue = "")
            return extension in INDEXABLE_EXTENSIONS || (extension.isEmpty() && basename in INDEXABLE_NAMES)
        }
    }

    fun load(
        config: JsonNode?,
        credentials: JsonNode,
        checkpointNode: JsonNode?,
        start: Instant? = null,
        end: Instant? = null,
    ): Sequence<ConnectorBatch> = loadInternal(
        config,
        credentials,
        checkpointNode,
        adjustedStart(start),
        end?.plus(1, ChronoUnit.DAYS),
        slim = false,
        includePermissions = config.boolean("include_permissions", false),
    )

    fun retrieveAllSlimDocuments(
        config: JsonNode?,
        credentials: JsonNode,
        start: Instant? = null,
        end: Instant? = null,
        includePermissions: Boolean = false,
    ): Sequence<ConnectorBatch> = loadInternal(
        config,
        credentials,
        checkpointNode = null,
        start = start,
        end = end,
        slim = true,
        includePermissions = includePermissions,
    )

    fun validate(config: JsonNode?, credentials: JsonNode) {
        val context = context(config, credentials, includePermissions = false)
        if (!context.includePullRequests && !context.includeIssues && !context.includeFiles) {
            throw GithubConnectorValidationException(
                "Invalid GitHub settings: select pull requests, issues, or files.",
            )
        }
        val names = context.repositoryNames
        if (names.isNotEmpty()) {
            var lastError: WebClientResponseException? = null
            names.forEach { name ->
                try {
                    http.get(context.base, repositoryPath(context.owner, name), context.headers)
                    if (context.configuredBranch != null) {
                        http.get(
                            context.base,
                            "${repositoryPath(context.owner, name)}/branches/${segment(context.configuredBranch)}",
                            context.headers,
                        )
                    }
                    return
                } catch (error: WebClientResponseException) {
                    lastError = error
                }
            }
            throw validationError(requireNotNull(lastError), context, repository = names.singleOrNull())
        }

        val repositories = try {
            http.get(
                context.base,
                "/orgs/${segment(context.owner)}/repos?per_page=1&page=1",
                context.headers,
            )
        } catch (error: WebClientResponseException.NotFound) {
            try {
                http.get(
                    context.base,
                    "/users/${segment(context.owner)}/repos?per_page=1&page=1",
                    context.headers,
                )
            } catch (userError: WebClientResponseException) {
                throw validationError(userError, context)
            }
        } catch (error: WebClientResponseException) {
            throw validationError(error, context)
        }
        if (!repositories.isArray || repositories.isEmpty) {
            throw GithubConnectorValidationException(
                "Found no repos for user: ${context.owner}. Does the credential have the right scopes?",
            )
        }
    }

    private fun loadInternal(
        config: JsonNode?,
        credentials: JsonNode,
        checkpointNode: JsonNode?,
        start: Instant?,
        end: Instant?,
        slim: Boolean,
        includePermissions: Boolean,
    ): Sequence<ConnectorBatch> = sequence {
        val context = context(config, credentials, includePermissions)
        val savedCheckpoint = parseCheckpoint(checkpointNode)
        validateRepositoryIndex(context, savedCheckpoint)
        var checkpoint = if (savedCheckpoint.hasMore) savedCheckpoint else GithubCheckpoint()
        while (checkpoint.hasMore) {
            when (checkpoint.stage) {
                GithubStage.REPOSITORIES -> {
                    val result = selectRepository(context, checkpoint)
                    checkpoint = result.checkpoint
                    yield(batch(checkpoint, failures = result.failures))
                }
                GithubStage.PERMISSIONS -> {
                    val result = processPermissions(context, checkpoint)
                    checkpoint = result.checkpoint
                    yield(batch(checkpoint, result.documents, result.failures))
                }
                GithubStage.PULL_REQUESTS -> {
                    if (!context.includePullRequests) {
                        checkpoint = checkpoint.copy(stage = GithubStage.ISSUES).resetPageState()
                        continue
                    }
                    val result = processCollection(
                        context,
                        checkpoint,
                        type = CollectionType.PULL_REQUEST,
                        start,
                        end,
                        slim,
                    )
                    checkpoint = result.checkpoint
                    yield(batch(checkpoint, result.documents, result.failures))
                }
                GithubStage.ISSUES -> {
                    if (!context.includeIssues) {
                        checkpoint = checkpoint.copy(stage = GithubStage.FILES).resetPageState()
                        continue
                    }
                    val result = processCollection(
                        context,
                        checkpoint,
                        type = CollectionType.ISSUE,
                        start,
                        end,
                        slim,
                    )
                    checkpoint = result.checkpoint
                    yield(batch(checkpoint, result.documents, result.failures))
                }
                GithubStage.FILES -> {
                    val result = if (context.includeFiles) {
                        processFiles(context, checkpoint, start, slim)
                    } else {
                        ProcessResult(checkpoint = advanceRepository(context, checkpoint))
                    }
                    checkpoint = result.checkpoint
                    yield(batch(checkpoint, result.documents, result.failures))
                }
            }
        }
    }

    private fun selectRepository(context: Context, checkpoint: GithubCheckpoint): ProcessResult {
        if (checkpoint.repository != null) {
            return ProcessResult(
                checkpoint = checkpoint.copy(stage = firstStage(context, checkpoint.repository)).resetPageState(),
            )
        }
        if (context.repositoryNames.isNotEmpty()) {
            if (checkpoint.repositoryIndex >= context.repositoryNames.size) {
                return ProcessResult(checkpoint = terminal())
            }
            val repositoryName = context.repositoryNames[checkpoint.repositoryIndex]
            val repository = try {
                parseRepository(get(context, repositoryPath(context.owner, repositoryName)).body, context)
            } catch (_: WebClientResponseException.NotFound) {
                return ProcessResult(
                    failures = listOf(
                        ConnectorFailure(
                            FailureTarget.Entity("${context.owner}/$repositoryName"),
                            "GitHub repository not found: ${context.owner}/$repositoryName",
                            "github_repository_not_found",
                        ),
                    ),
                    checkpoint = advanceRepository(context, checkpoint),
                )
            }
            return ProcessResult(
                checkpoint = checkpoint.copy(
                    repository = repository,
                    repositories = listOf(repository),
                    stage = firstStage(context, repository),
                    repositoryListingComplete = true,
                ).resetPageState(),
            )
        }

        if (checkpoint.repositoryListingComplete) {
            val repository = checkpoint.repositories.getOrNull(checkpoint.repositoryIndex)
                ?: return ProcessResult(checkpoint = terminal())
            return ProcessResult(
                checkpoint = checkpoint.copy(repository = repository, stage = firstStage(context, repository)).resetPageState(),
            )
        }
        val ownerKind = checkpoint.repositoryOwnerKind ?: "orgs"
        var resolvedOwnerKind = ownerKind
        val page = try {
            get(
                context,
                "/$ownerKind/${segment(context.owner)}/repos?per_page=$PAGE_SIZE&page=${checkpoint.repositoryPage}",
            ).body
        } catch (error: WebClientResponseException.NotFound) {
            if (ownerKind != "orgs") throw validationError(error, context)
            resolvedOwnerKind = "users"
            get(
                context,
                "/users/${segment(context.owner)}/repos?per_page=$PAGE_SIZE&page=${checkpoint.repositoryPage}",
            ).body
        }
        require(page.isArray) { "GitHub repository listing was not an array" }
        val parsed = page.map { parseRepository(it, context) }
        if (checkpoint.repositories.size + parsed.size > MAX_REPOSITORIES) {
            checkpointLimit("repository count exceeds $MAX_REPOSITORIES")
        }
        val repositories = checkpoint.repositories + parsed
        val listingComplete = parsed.size < PAGE_SIZE
        if (!listingComplete) {
            return ProcessResult(
                checkpoint = checkpoint.copy(
                    repositories = repositories,
                    repositoryPage = checkpoint.repositoryPage + 1,
                    repositoryOwnerKind = resolvedOwnerKind,
                ),
            )
        }
        val repository = repositories.firstOrNull() ?: return ProcessResult(
            checkpoint = terminal(),
        )
        return ProcessResult(
            checkpoint = checkpoint.copy(
                repositories = repositories,
                repository = repository,
                repositoryListingComplete = true,
                repositoryOwnerKind = resolvedOwnerKind,
                stage = firstStage(context, repository),
            ).resetPageState(),
        )
    }

    private fun processPermissions(context: Context, checkpoint: GithubCheckpoint): ProcessResult {
        val repository = requireNotNull(checkpoint.repository) { "GitHub checkpoint has no current repository" }
        val next = when (checkpoint.permissionStage) {
            GithubPermissionStage.COLLABORATORS -> {
                val response = get(
                    context,
                    "${repositoryPath(context.owner, repository.name)}/collaborators" +
                        "?affiliation=all&per_page=$PAGE_SIZE&page=${checkpoint.permissionCollaboratorPage}",
                ).body
                require(response.isArray) { "GitHub collaborator response was not an array" }
                val emails = checkpoint.permissionEmails + response.mapNotNull { it.text("email") }
                val logins = checkpoint.permissionUserLogins + response.mapNotNull { collaborator ->
                    if (collaborator.text("email") == null) collaborator.text("login") else null
                }
                if (response.size() == PAGE_SIZE) {
                    checkpoint.copy(
                        permissionCollaboratorPage = checkpoint.permissionCollaboratorPage + 1,
                        permissionEmails = emails,
                        permissionUserLogins = logins.distinct(),
                    )
                } else {
                    checkpoint.copy(
                        permissionStage = if (logins.isEmpty()) GithubPermissionStage.TEAMS else GithubPermissionStage.USERS,
                        permissionEmails = emails,
                        permissionUserLogins = logins.distinct(),
                    )
                }
            }
            GithubPermissionStage.USERS -> {
                val login = checkpoint.permissionUserLogins.getOrNull(checkpoint.permissionUserOffset)
                    ?: return ProcessResult(checkpoint = checkpoint.copy(permissionStage = GithubPermissionStage.TEAMS))
                val email = try {
                    get(context, "/users/${segment(login)}").body.text("email")
                } catch (_: WebClientResponseException.NotFound) {
                    null
                }
                val offset = checkpoint.permissionUserOffset + 1
                checkpoint.copy(
                    permissionStage = if (offset == checkpoint.permissionUserLogins.size) {
                        GithubPermissionStage.TEAMS
                    } else {
                        GithubPermissionStage.USERS
                    },
                    permissionUserOffset = offset,
                    permissionEmails = checkpoint.permissionEmails + listOfNotNull(email),
                )
            }
            GithubPermissionStage.TEAMS -> {
                val response = get(
                    context,
                    "${repositoryPath(context.owner, repository.name)}/teams" +
                        "?per_page=$PAGE_SIZE&page=${checkpoint.permissionTeamPage}",
                ).body
                require(response.isArray) { "GitHub team response was not an array" }
                val teamIds = checkpoint.permissionTeamIds + response.mapNotNull {
                    it.path("id").asText().takeIf(String::isNotBlank)
                }
                if (response.size() == PAGE_SIZE) {
                    checkpoint.copy(
                        permissionTeamPage = checkpoint.permissionTeamPage + 1,
                        permissionTeamIds = teamIds,
                    )
                } else {
                    checkpoint.copy(
                        stage = firstContentStage(context),
                        permissionTeamIds = teamIds,
                    )
                }
            }
        }
        return ProcessResult(checkpoint = next)
    }

    private fun processCollection(
        context: Context,
        checkpoint: GithubCheckpoint,
        type: CollectionType,
        start: Instant?,
        end: Instant?,
        slim: Boolean,
    ): ProcessResult {
        val repository = requireNotNull(checkpoint.repository) { "GitHub checkpoint has no current repository" }
        val pageNumber = if (type == CollectionType.PULL_REQUEST) checkpoint.pullRequestPage else checkpoint.issuePage
        val cursor = if (type == CollectionType.PULL_REQUEST) checkpoint.pullRequestCursor else checkpoint.issueCursor
        val retrieved = if (type == CollectionType.PULL_REQUEST) {
            checkpoint.pullRequestsRetrieved
        } else {
            checkpoint.issuesRetrieved
        }
        val page = try {
            fetchCollectionPage(context, repository, type, pageNumber, cursor, retrieved)
        } catch (error: WebClientResponseException.NotFound) {
            CollectionPage(emptyList(), rawSize = 0, nextCursor = null, cursorMode = cursor != null)
        }
        val access = checkpointAccess(context, checkpoint, repository)
        val documents = mutableListOf<SourceDocument>()
        val failures = mutableListOf<ConnectorFailure>()
        var reachedStart = false
        page.items.forEach { item ->
            if (type == CollectionType.ISSUE && item.has("pull_request")) return@forEach
            val updated = item.instant("updated_at")
            if (!slim && start != null && updated != null && updated.isBefore(start)) {
                reachedStart = true
                return@forEach
            }
            if (!slim && end != null && updated != null && updated.isAfter(end)) return@forEach
            try {
                val documentItem = if (!slim && type == CollectionType.PULL_REQUEST) {
                    val number = item.path("number").asInt()
                    require(number > 0) { "GitHub pull request number is missing" }
                    get(
                        context,
                        "${repositoryPath(context.owner, repository.name)}/pulls/$number",
                    ).body
                } else {
                    item
                }
                documents += if (slim) {
                    slimDocument(documentItem, access)
                } else {
                    collectionDocument(repository, documentItem, type, access)
                }
            } catch (error: Exception) {
                val link = item.text("html_url")
                failures += ConnectorFailure(
                    link?.let { FailureTarget.Document(it, it) }
                        ?: FailureTarget.Entity("${repository.fullName}:${type.label}:${item.path("id").asText("unknown")}"),
                    "Failed to convert GitHub ${type.label}: ${error.message ?: error::class.simpleName}",
                    "github_${type.label}_processing",
                )
            }
        }

        val hasAnotherPage = !reachedStart && when {
            page.cursorMode -> page.nextCursor != null
            page.nextCursor != null -> true
            else -> page.rawSize == PAGE_SIZE
        }
        val nextCheckpoint = if (hasAnotherPage) {
            if (type == CollectionType.PULL_REQUEST) {
                checkpoint.copy(
                    pullRequestPage = page.nextOffsetPage ?: checkpoint.pullRequestPage + if (page.cursorMode) 0 else 1,
                    pullRequestCursor = page.nextCursor,
                    pullRequestsRetrieved = checkpoint.pullRequestsRetrieved + page.items.size,
                )
            } else {
                checkpoint.copy(
                    issuePage = page.nextOffsetPage ?: checkpoint.issuePage + if (page.cursorMode) 0 else 1,
                    issueCursor = page.nextCursor,
                    issuesRetrieved = checkpoint.issuesRetrieved + page.items.size,
                )
            }
        } else if (type == CollectionType.PULL_REQUEST) {
            checkpoint.copy(stage = GithubStage.ISSUES).resetPageState()
        } else {
            checkpoint.copy(stage = GithubStage.FILES).resetPageState()
        }
        return ProcessResult(documents, failures, nextCheckpoint)
    }

    private fun fetchCollectionPage(
        context: Context,
        repository: GithubRepository,
        type: CollectionType,
        pageNumber: Int,
        cursor: String?,
        retrieved: Int,
    ): CollectionPage {
        val endpoint = "${repositoryPath(context.owner, repository.name)}/${type.endpoint}"
        val query = "state=${query(context.stateFilter)}&per_page=$PAGE_SIZE&sort=updated&direction=desc"
        if (cursor != null) {
            return try {
                val response = get(context, safeCursorPath(context.base, cursor))
                CollectionPage(
                    response.body.toList(),
                    response.body.size(),
                    nextCursor(response.headers, context.base),
                    cursorMode = true,
                )
            } catch (error: WebClientResponseException.UnprocessableEntity) {
                if (!error.responseBodyAsString.contains("cursor", ignoreCase = true)) throw error
                val restartPage = (retrieved / PAGE_SIZE) + 1
                val response = get(context, "$endpoint?$query&page=$restartPage")
                val skip = retrieved % PAGE_SIZE
                CollectionPage(
                    response.body.toList().drop(skip),
                    response.body.size(),
                    nextCursor(response.headers, context.base),
                    cursorMode = false,
                    nextOffsetPage = restartPage + 1,
                )
            }
        }
        return try {
            val response = get(context, "$endpoint?$query&page=$pageNumber")
            CollectionPage(
                response.body.toList(),
                response.body.size(),
                nextCursor(response.headers, context.base),
                cursorMode = false,
            )
        } catch (error: WebClientResponseException.UnprocessableEntity) {
            if (!error.responseBodyAsString.contains("cursor", ignoreCase = true)) throw error
            val response = get(context, "$endpoint?$query")
            CollectionPage(
                response.body.toList(),
                response.body.size(),
                nextCursor(response.headers, context.base),
                cursorMode = true,
            )
        }
    }

    private fun processFiles(
        context: Context,
        checkpoint: GithubCheckpoint,
        start: Instant?,
        slim: Boolean,
    ): ProcessResult {
        val repository = requireNotNull(checkpoint.repository) { "GitHub checkpoint has no current repository" }
        val branch = context.configuredBranch ?: repository.defaultBranch
        val branchChanged = checkpoint.filePaths != null && checkpoint.branch != branch
        var filePaths = if (branchChanged) null else checkpoint.filePaths
        var fileOffset = if (branchChanged) 0 else checkpoint.fileOffset
        val failures = mutableListOf<ConnectorFailure>()
        if (filePaths == null) {
            if (!branchChanged && start != null && repository.pushedAt?.let(Instant::parse)?.isBefore(start) == true) {
                filePaths = emptyList()
            } else {
                val tree = try {
                    get(
                        context,
                        "${repositoryPath(context.owner, repository.name)}/git/trees/${segment(branch)}?recursive=1",
                    ).body
                } catch (error: WebClientResponseException) {
                    if (error.statusCode.value() == 409 && error.responseBodyAsString.contains("empty", ignoreCase = true)) {
                        mapper.createObjectNode().putArray("tree")
                    } else if (error.statusCode.value() == 404 && context.configuredBranch != null) {
                        throw GithubConnectorValidationException(
                            "Branch '${context.configuredBranch}' not found in repository ${repository.fullName}.",
                            error,
                        )
                    } else {
                        throw error
                    }
                }
                filePaths = mutableListOf<String>().also { paths ->
                    tree.path("tree").forEach { entry ->
                        if (
                            entry.path("type").asText() == "blob" &&
                            isIndexablePath(
                                entry.path("path").asText(),
                                entry.path("size").takeIf(JsonNode::isNumber)?.asInt(),
                            )
                        ) {
                            if (paths.size == MAX_FILE_PATHS) checkpointLimit("file path count exceeds $MAX_FILE_PATHS")
                            paths += entry.path("path").asText()
                        }
                    }
                    paths.sort()
                }
                if (tree.path("truncated").asBoolean(false)) {
                    failures += ConnectorFailure(
                        FailureTarget.Entity("${repository.fullName}:files"),
                        "GitHub truncated the file tree for ${repository.fullName}; some files were not indexed.",
                        "github_tree_truncated",
                    )
                }
            }
        }
        val access = checkpointAccess(context, checkpoint, repository)
        val documents = mutableListOf<SourceDocument>()
        val selected = filePaths.drop(fileOffset).take(FILE_BATCH_SIZE)
        selected.forEach { path ->
            val link = "${repository.htmlUrl}/blob/$branch/$path"
            if (slim) {
                documents += SourceDocument(
                    id = link,
                    title = link,
                    content = "",
                    link = link,
                    externalAccess = access,
                    source = ConnectorSource.GITHUB,
                    updatedAt = repository.pushedAt?.let(Instant::parse),
                )
                return@forEach
            }
            try {
                val contentResponse = get(
                    context,
                    "${repositoryPath(context.owner, repository.name)}/contents/${pathSegments(path)}?ref=${query(branch)}",
                ).body
                val encoding = contentResponse.path("encoding").asText()
                val encoded = contentResponse.path("content").asText().replace("\n", "")
                require(encoding == "base64" && encoded.isNotBlank()) {
                    "Could not decode content for $path (encoding=$encoding)"
                }
                val content = decodeText(Base64.getDecoder().decode(encoded))
                    ?: throw IllegalArgumentException("Could not decode non-text file $path")
                documents += fileDocument(repository, path, branch, content, access)
            } catch (error: Exception) {
                failures += ConnectorFailure(
                    FailureTarget.Document(link, link),
                    "Error converting GitHub file $path to a document: ${error.message ?: error::class.simpleName}",
                    "github_file_decode",
                )
            }
        }
        fileOffset += selected.size
        val next = if (fileOffset < filePaths.size) {
            checkpoint.copy(filePaths = filePaths, fileOffset = fileOffset, branch = branch)
        } else {
            advanceRepository(
                context,
                checkpoint.copy(filePaths = filePaths, fileOffset = fileOffset, branch = branch),
            )
        }
        return ProcessResult(documents, failures, next)
    }

    private fun collectionDocument(
        repository: GithubRepository,
        item: JsonNode,
        type: CollectionType,
        access: ExternalAccess?,
    ): SourceDocument {
        val number = item.path("number").asInt()
        require(number > 0) { "GitHub ${type.label} number is missing" }
        val link = item.text("html_url") ?: error("GitHub ${type.label} URL is missing")
        val title = item.path("title").asText()
        val author = item.path("user").path("login").asText().takeIf(String::isNotBlank)
        val assignees = item.path("assignees").mapNotNull { it.path("login").asText().takeIf(String::isNotBlank) }
        val labels = item.path("labels").mapNotNull { it.path("name").asText().takeIf(String::isNotBlank) }
        val metadata = linkedMapOf<String, Any?>(
            "source" to "github",
            "repository" to repository.fullName,
            "repo" to repository.fullName,
            "object_type" to type.objectType,
            "id" to number,
            "state" to item.path("state").asText(),
            "user" to userInfo(item.path("user")),
            "assignees" to item.path("assignees").map(::userInfo),
            "labels" to labels,
            "created_at" to item.text("created_at"),
            "updated_at" to item.text("updated_at"),
            "source_path" to listOf(repository.fullName.substringBefore('/'), repository.name, type.hierarchy),
        )
        if (type == CollectionType.PULL_REQUEST) {
            metadata["merged"] = item.path("merged").asBoolean(false)
            metadata["num_commits"] = item.path("commits").asInt()
            metadata["num_files_changed"] = item.path("changed_files").asInt()
        }
        return SourceDocument(
            id = link,
            title = "$number: $title",
            content = item.path("body").asText(),
            link = link,
            metadata = metadata,
            externalAccess = access,
            source = ConnectorSource.GITHUB,
            updatedAt = item.instant("updated_at"),
            primaryOwners = listOfNotNull(author),
            secondaryOwners = assignees,
        )
    }

    private fun slimDocument(item: JsonNode, access: ExternalAccess?): SourceDocument {
        val link = item.text("html_url") ?: error("GitHub document URL is missing")
        return SourceDocument(
            id = link,
            title = link,
            content = "",
            link = link,
            externalAccess = access,
            source = ConnectorSource.GITHUB,
            updatedAt = item.instant("created_at"),
        )
    }

    private fun fileDocument(
        repository: GithubRepository,
        path: String,
        branch: String,
        content: String,
        access: ExternalAccess?,
    ): SourceDocument {
        val link = "${repository.htmlUrl}/blob/$branch/$path"
        return SourceDocument(
            id = link,
            title = path,
            content = content,
            link = link,
            metadata = mapOf(
                "source" to "github",
                "repository" to repository.fullName,
                "repo" to repository.fullName,
                "object_type" to "File",
                "path" to path,
                "file_extension" to path.substringAfterLast('.', missingDelimiterValue = "").lowercase(),
                "branch" to branch,
                "source_path" to listOf(
                    repository.fullName.substringBefore('/'),
                    repository.name,
                    "files",
                ) + path.split('/'),
            ),
            externalAccess = access,
            source = ConnectorSource.GITHUB,
            updatedAt = repository.pushedAt?.let(Instant::parse),
        )
    }

    private fun checkpointAccess(
        context: Context,
        checkpoint: GithubCheckpoint,
        repository: GithubRepository,
    ): ExternalAccess? = if (context.includePermissions) {
        ExternalAccess(checkpoint.permissionEmails, checkpoint.permissionTeamIds, isPublic = !repository.isPrivate)
    } else {
        null
    }

    private fun advanceRepository(context: Context, checkpoint: GithubCheckpoint): GithubCheckpoint {
        val nextIndex = checkpoint.repositoryIndex + 1
        if (context.repositoryNames.isNotEmpty()) {
            return if (nextIndex < context.repositoryNames.size) {
                GithubCheckpoint(
                    repositories = checkpoint.repositories,
                    repositoryIndex = nextIndex,
                    repositoryListingComplete = true,
                )
            } else {
                terminal()
            }
        }
        val nextRepository = checkpoint.repositories.getOrNull(nextIndex) ?: return terminal()
        return GithubCheckpoint(
            repositories = checkpoint.repositories,
            repository = nextRepository,
            repositoryIndex = nextIndex,
            repositoryPage = checkpoint.repositoryPage,
            repositoryListingComplete = true,
            repositoryOwnerKind = checkpoint.repositoryOwnerKind,
            stage = firstStage(context, nextRepository),
        )
    }

    private fun terminal(): GithubCheckpoint =
        GithubCheckpoint(hasMore = false, stage = GithubStage.FILES)

    private fun GithubCheckpoint.resetPageState(): GithubCheckpoint = copy(
        pullRequestPage = if (stage == GithubStage.PULL_REQUESTS) pullRequestPage else 1,
        pullRequestCursor = if (stage == GithubStage.PULL_REQUESTS) pullRequestCursor else null,
        pullRequestsRetrieved = if (stage == GithubStage.PULL_REQUESTS) pullRequestsRetrieved else 0,
        issuePage = if (stage == GithubStage.ISSUES) issuePage else 1,
        issueCursor = if (stage == GithubStage.ISSUES) issueCursor else null,
        issuesRetrieved = if (stage == GithubStage.ISSUES) issuesRetrieved else 0,
        filePaths = if (stage == GithubStage.FILES) filePaths else null,
        fileOffset = if (stage == GithubStage.FILES) fileOffset else 0,
        branch = if (stage == GithubStage.FILES) branch else null,
    )

    private fun firstStage(context: Context, repository: GithubRepository): GithubStage =
        if (context.includePermissions && repository.isPrivate) GithubStage.PERMISSIONS else firstContentStage(context)

    private fun firstContentStage(context: Context): GithubStage = when {
        context.includePullRequests -> GithubStage.PULL_REQUESTS
        context.includeIssues -> GithubStage.ISSUES
        else -> GithubStage.FILES
    }

    private fun batch(
        checkpoint: GithubCheckpoint,
        documents: List<SourceDocument> = emptyList(),
        failures: List<ConnectorFailure> = emptyList(),
    ): ConnectorBatch {
        ensureCheckpointBounds(checkpoint)
        val value = mapper.valueToTree<JsonNode>(checkpoint)
        if (mapper.writeValueAsBytes(value).size > MAX_CHECKPOINT_BYTES) {
            checkpointLimit("serialized size exceeds $MAX_CHECKPOINT_BYTES bytes")
        }
        return ConnectorBatch(
            documents = documents,
            failures = failures,
            checkpoint = ConnectorCheckpoint(value, checkpoint.hasMore),
            enumerationComplete = failures.all { it.target is FailureTarget.Document },
        )
    }

    private fun parseCheckpoint(node: JsonNode?): GithubCheckpoint {
        if (node == null || node.isNull) return GithubCheckpoint()
        if (mapper.writeValueAsBytes(node).size > MAX_CHECKPOINT_BYTES) {
            checkpointLimit("serialized size exceeds $MAX_CHECKPOINT_BYTES bytes")
        }
        validateCheckpointTypes(node)
        val checkpoint = try {
            mapper.treeToValue(node, GithubCheckpoint::class.java)
        } catch (error: Exception) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: ${error.message}", error)
        }
        ensureCheckpointBounds(checkpoint)
        if (checkpoint.hasMore && checkpoint.stage != GithubStage.REPOSITORIES && checkpoint.repository == null) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: active stage has no repository")
        }
        return checkpoint
    }

    private fun ensureCheckpointBounds(checkpoint: GithubCheckpoint) {
        if (
            checkpoint.repositoryIndex < 0 || checkpoint.repositoryPage < 1 || checkpoint.pullRequestPage < 1 ||
            checkpoint.issuePage < 1 || checkpoint.pullRequestsRetrieved < 0 || checkpoint.issuesRetrieved < 0 ||
            checkpoint.fileOffset < 0 || checkpoint.permissionCollaboratorPage < 1 || checkpoint.permissionTeamPage < 1 ||
            checkpoint.permissionUserOffset < 0
        ) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: page and offset values must be positive")
        }
        if (checkpoint.repositories.size > MAX_REPOSITORIES) checkpointLimit("repository count exceeds $MAX_REPOSITORIES")
        if ((checkpoint.filePaths?.size ?: 0) > MAX_FILE_PATHS) checkpointLimit("file path count exceeds $MAX_FILE_PATHS")
        if (checkpoint.filePaths == null && checkpoint.fileOffset != 0) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: file offset requires saved paths")
        }
        if (checkpoint.filePaths != null && checkpoint.fileOffset > checkpoint.filePaths.size) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: file offset exceeds saved paths")
        }
        if (checkpoint.permissionUserOffset > checkpoint.permissionUserLogins.size) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: user offset exceeds saved users")
        }
        val unresolvedUsers = checkpoint.permissionUserLogins.size - checkpoint.permissionUserOffset
        if (checkpoint.permissionEmails.size + checkpoint.permissionTeamIds.size + unresolvedUsers > MAX_PERMISSION_ENTRIES) {
            checkpointLimit("permission entries exceed $MAX_PERMISSION_ENTRIES")
        }
    }

    private fun validateRepositoryIndex(context: Context, checkpoint: GithubCheckpoint) {
        if (!checkpoint.hasMore) return
        val available = if (context.repositoryNames.isNotEmpty()) {
            context.repositoryNames.indices
        } else if (checkpoint.repositoryOwnerKind != null || checkpoint.repositoryListingComplete) {
            checkpoint.repositories.indices
        } else {
            return
        }
        if (checkpoint.repositoryIndex !in available) {
            throw GithubConnectorValidationException("Invalid GitHub checkpoint: repository index exceeds available repositories")
        }
    }

    private fun checkpointLimit(message: String): Nothing =
        throw GithubConnectorValidationException("GitHub checkpoint limit exceeded: $message")

    private fun validateCheckpointTypes(node: JsonNode) {
        if (!node.isObject) throw GithubConnectorValidationException("Invalid GitHub checkpoint: expected an object")
        listOf("hasMore", "repositoryListingComplete").forEach { name ->
            node.get(name)?.let { if (!it.isBoolean) invalidCheckpointType(name) }
        }
        listOf(
            "repositoryIndex", "repositoryPage", "pullRequestPage", "pullRequestsRetrieved", "issuePage",
            "issuesRetrieved", "fileOffset", "permissionCollaboratorPage", "permissionUserOffset", "permissionTeamPage",
        ).forEach { name ->
            node.get(name)?.let { if (!it.isIntegralNumber) invalidCheckpointType(name) }
        }
        listOf("stage", "permissionStage").forEach { name ->
            node.get(name)?.let { if (!it.isTextual) invalidCheckpointType(name) }
        }
        listOf("repositoryOwnerKind", "pullRequestCursor", "issueCursor", "branch").forEach { name ->
            node.get(name)?.let { if (!it.isNull && !it.isTextual) invalidCheckpointType(name) }
        }
        node.get("repositories")?.let { repositories ->
            if (!repositories.isArray) invalidCheckpointType("repositories")
            repositories.forEach { validateRepositoryTypes(it, "repositories") }
        }
        node.get("repository")?.let { if (!it.isNull) validateRepositoryTypes(it, "repository") }
        node.get("filePaths")?.let { paths ->
            if (!paths.isNull && (!paths.isArray || paths.any { !it.isTextual })) invalidCheckpointType("filePaths")
        }
        node.get("permissionUserLogins")?.let { users ->
            if (!users.isArray || users.any { !it.isTextual }) invalidCheckpointType("permissionUserLogins")
        }
        listOf("permissionEmails", "permissionTeamIds").forEach { name ->
            node.get(name)?.let { values ->
                if (!values.isArray || values.any { !it.isTextual }) invalidCheckpointType(name)
            }
        }
    }

    private fun validateRepositoryTypes(node: JsonNode, field: String) {
        if (!node.isObject) invalidCheckpointType(field)
        node.get("id")?.let { if (!it.isIntegralNumber) invalidCheckpointType("$field.id") }
        node.get("isPrivate")?.let { if (!it.isBoolean) invalidCheckpointType("$field.isPrivate") }
        listOf("name", "fullName", "htmlUrl", "defaultBranch").forEach { name ->
            node.get(name)?.let { if (!it.isTextual) invalidCheckpointType("$field.$name") }
        }
        node.get("pushedAt")?.let { if (!it.isNull && !it.isTextual) invalidCheckpointType("$field.pushedAt") }
    }

    private fun invalidCheckpointType(field: String): Nothing =
        throw GithubConnectorValidationException("Invalid GitHub checkpoint: $field has the wrong type")

    private fun context(config: JsonNode?, credentials: JsonNode, includePermissions: Boolean): Context {
        val owner = config.text("repo_owner") ?: config.text("owner")
            ?: throw GithubConnectorValidationException("Invalid GitHub settings: repo_owner is required.")
        val token = credentials.firstText("github_access_token", "access_token", "token")
            ?: throw GithubCredentialExpiredException("GitHub credentials do not contain an access token.")
        val names = config.text("repositories")?.split(',')?.map(String::trim)?.filter(String::isNotEmpty).orEmpty()
        if (names.size > MAX_REPOSITORIES) checkpointLimit("repository count exceeds $MAX_REPOSITORIES")
        return Context(
            base = config.text("github_base_url") ?: "https://api.github.com",
            owner = owner,
            repositoryNames = names,
            stateFilter = config.text("state_filter") ?: "all",
            includePullRequests = config.boolean("include_prs", true),
            includeIssues = config.boolean("include_issues", false),
            includeFiles = config.boolean("include_files", false),
            configuredBranch = config.text("branch")?.trim()?.takeIf(String::isNotEmpty),
            includePermissions = includePermissions,
            headers = mapOf(
                HttpHeaders.AUTHORIZATION to "Bearer $token",
                HttpHeaders.ACCEPT to "application/vnd.github+json",
                "X-GitHub-Api-Version" to "2022-11-28",
            ),
        )
    }

    private fun get(context: Context, path: String, attempt: Int = 0): RemoteJsonResponse {
        if (attempt > MAX_RATE_LIMIT_RETRIES) {
            throw GithubRateLimitValidationException("GitHub rate limit retry limit was exceeded")
        }
        try {
            return http.getResponse(context.base, path, context.headers)
        } catch (error: WebClientResponseException) {
            if (!isRateLimited(error)) throw error
            val reset = error.headers.getFirst("X-RateLimit-Reset")?.toLongOrNull()
                ?: throw GithubRateLimitValidationException("GitHub rate limit response did not include a reset time.", error)
            val waitSeconds = try {
                Math.subtractExact(reset, nowSource().epochSecond)
            } catch (_: ArithmeticException) {
                throw GithubRateLimitValidationException("GitHub rate limit reset time is outside the supported range.", error)
            }
            if (waitSeconds > MAX_RATE_LIMIT_WAIT_SECONDS) {
                throw GithubRateLimitValidationException("GitHub rate limit reset wait exceeds one hour.", error)
            }
            val wait = waitSeconds.coerceAtLeast(0) * 1000
            sleepMillis(wait)
            return get(context, path, attempt + 1)
        }
    }

    private fun isRateLimited(error: WebClientResponseException): Boolean =
        error.statusCode.value() in setOf(403, 429) && (
            error.headers.getFirst("X-RateLimit-Remaining") == "0" ||
                error.responseBodyAsString.contains("rate limit", ignoreCase = true)
            )

    private fun validationError(
        error: WebClientResponseException,
        context: Context,
        repository: String? = null,
    ): GithubConnectorValidationException = when (error.statusCode.value()) {
        401 -> GithubCredentialExpiredException(
            "GitHub credential appears to be invalid or expired (HTTP 401).",
            error,
        )
        403 -> if (isRateLimited(error)) {
            GithubRateLimitValidationException("Validation failed because GitHub rate limits were exceeded.", error)
        } else {
            GithubInsufficientPermissionsException(
                "Your GitHub token does not have sufficient permissions (HTTP 403).",
                error,
            )
        }
        404 -> GithubConnectorValidationException(
            repository?.let { "GitHub repository not found: ${context.owner}/$it" }
                ?: "GitHub user or organization not found: ${context.owner}",
            error,
        )
        else -> GithubConnectorValidationException(
            "Unexpected GitHub error (HTTP ${error.statusCode.value()}): ${error.responseBodyAsString}",
            error,
        )
    }

    private fun parseRepository(node: JsonNode, context: Context): GithubRepository {
        val name = node.text("name") ?: error("GitHub repository name is missing")
        return GithubRepository(
            id = node.path("id").asLong(),
            name = name,
            fullName = node.text("full_name") ?: "${context.owner}/$name",
            isPrivate = node.path("private").asBoolean(false),
            htmlUrl = node.text("html_url") ?: "https://github.com/${context.owner}/$name",
            defaultBranch = node.text("default_branch") ?: "main",
            pushedAt = node.text("pushed_at"),
        )
    }

    private fun adjustedStart(start: Instant?): Instant? = start?.minus(3, ChronoUnit.HOURS)?.let {
        if (it.isBefore(Instant.EPOCH)) Instant.EPOCH else it
    }

    private fun decodeText(bytes: ByteArray): String? {
        if (bytes.any { it == 0.toByte() }) return null
        return runCatching {
            StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString()
        }.getOrNull()
    }

    private fun nextCursor(headers: HttpHeaders, base: String): String? {
        val link = headers.getFirst(HttpHeaders.LINK) ?: return null
        val next = link.split(',').firstOrNull { it.substringAfter('>').contains("rel=\"next\"") }
            ?.substringAfter('<')?.substringBefore('>') ?: return null
        return safeCursorPath(base, next)
    }

    private fun safeCursorPath(base: String, cursor: String): String {
        if (cursor.startsWith('/')) return cursor
        val baseUri = URI.create(base)
        val cursorUri = URI.create(cursor)
        require(baseUri.scheme.equals(cursorUri.scheme, true) && baseUri.authority.equals(cursorUri.authority, true)) {
            "GitHub cursor points outside the configured server"
        }
        return cursorUri.rawPath + cursorUri.rawQuery?.let { "?$it" }.orEmpty()
    }

    private fun userInfo(node: JsonNode): Map<String, String> = listOf("login", "name", "email")
        .mapNotNull { key -> node.path(key).asText().takeIf(String::isNotBlank)?.let { key to it } }
        .toMap()

    private fun repositoryPath(owner: String, repository: String): String =
        "/repos/${segment(owner)}/${segment(repository)}"

    private fun pathSegments(path: String): String = path.split('/').joinToString("/") { segment(it) }

    private fun segment(value: String): String = UriUtils.encodePathSegment(value, StandardCharsets.UTF_8)

    private fun query(value: String): String = UriUtils.encodeQueryParam(value, StandardCharsets.UTF_8)

    private fun JsonNode?.text(name: String): String? =
        this?.path(name)?.asText()?.takeIf(String::isNotBlank)

    private fun JsonNode?.boolean(name: String, default: Boolean): Boolean =
        this?.path(name)?.takeIf(JsonNode::isBoolean)?.asBoolean() ?: default

    private fun JsonNode.firstText(vararg names: String): String? = names.firstNotNullOfOrNull { text(it) }

    private fun JsonNode.instant(name: String): Instant? = text(name)?.let { runCatching { Instant.parse(it) }.getOrNull() }

    private data class Context(
        val base: String,
        val owner: String,
        val repositoryNames: List<String>,
        val stateFilter: String,
        val includePullRequests: Boolean,
        val includeIssues: Boolean,
        val includeFiles: Boolean,
        val configuredBranch: String?,
        val includePermissions: Boolean,
        val headers: Map<String, String>,
    )

    private data class ProcessResult(
        val documents: List<SourceDocument> = emptyList(),
        val failures: List<ConnectorFailure> = emptyList(),
        val checkpoint: GithubCheckpoint,
    )

    private data class CollectionPage(
        val items: List<JsonNode>,
        val rawSize: Int,
        val nextCursor: String?,
        val cursorMode: Boolean,
        val nextOffsetPage: Int? = null,
    )

    private enum class CollectionType(
        val endpoint: String,
        val objectType: String,
        val label: String,
        val hierarchy: String,
    ) {
        PULL_REQUEST("pulls", "PullRequest", "pull_request", "pull_requests"),
        ISSUE("issues", "Issue", "issue", "issues"),
    }
}
