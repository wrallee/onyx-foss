package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import tools.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorSource
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.WebClient
import java.time.Instant
import java.util.Base64
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

class GithubConnectorLoaderTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun githubCheckpointRoundTripsThroughJson() {
        val checkpoint = GithubCheckpoint(
            stage = GithubStage.FILES,
            repository = repository("docs", branch = "main"),
            repositoryIndex = 2,
            repositoryPage = 3,
            repositories = listOf(repository("one"), repository("two")),
            pullRequestPage = 4,
            pullRequestCursor = "/pulls?after=pr",
            pullRequestsRetrieved = 12,
            issuePage = 5,
            issueCursor = "/issues?after=issue",
            issuesRetrieved = 13,
            filePaths = listOf("README.md"),
            fileOffset = 1,
            branch = "main",
            hasMore = true,
        )

        assertEquals(checkpoint, mapper.treeToValue(mapper.valueToTree(checkpoint), GithubCheckpoint::class.java))
    }

    @Test
    fun githubCheckpointRejectsCoercedScalarTypes() {
        MockWebServer().use { server ->
            server.dispatcher = routes()
            val invalid = mapper.valueToTree<JsonNode>(
                mapOf(
                    "hasMore" to "true",
                    "stage" to "FILES",
                    "repository" to repository("test-repo"),
                    "pullRequestPage" to "2",
                ),
            )

            assertFailsWith<GithubConnectorValidationException> {
                loader().load(config(server), credentials(), invalid).first()
            }
        }
    }

    @Test
    fun githubCheckpointRejectsOffsetsBeyondSavedCollections() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val invalid = checkpoint(
            stage = GithubStage.FILES,
            filePaths = listOf("README.md"),
            fileOffset = 2,
        )

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(fileConfig(server), credentials(), mapper.valueToTree(invalid)).first()
        }
        Unit
    }

    @Test
    fun discoveredRepositoryCheckpointRejectsIndexPastSavedList() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val invalid = GithubCheckpoint(
            stage = GithubStage.REPOSITORIES,
            repositories = listOf(repository("only")),
            repositoryIndex = 1,
            repositoryListingComplete = true,
            repositoryOwnerKind = "orgs",
        )

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(
                config(server, "\"repositories\":\"\""),
                credentials(),
                mapper.valueToTree(invalid),
            ).first()
        }
        assertEquals(0, server.requestCount)
        Unit
    }

    @Test
    fun configuredRepositoryCheckpointRejectsIndexPastConfiguredList() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val invalid = GithubCheckpoint(
            stage = GithubStage.REPOSITORIES,
            repositoryIndex = 2,
            repositoryListingComplete = true,
        )

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(
                config(server, "\"repositories\":\"one,two\""),
                credentials(),
                mapper.valueToTree(invalid),
            ).first()
        }
        assertEquals(0, server.requestCount)
        Unit
    }

    @Test
    fun githubCheckpointRejectsOversizedRepositoryAndFileCollections() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val tooManyRepositories = GithubCheckpoint(
            repositories = List(10_001) { repository("repo$it", id = it.toLong()) },
        )
        val tooManyFiles = checkpoint(
            stage = GithubStage.FILES,
            filePaths = List(100_001) { "doc$it.md" },
        )

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(config(server), credentials(), mapper.valueToTree(tooManyRepositories)).first()
        }
        assertFailsWith<GithubConnectorValidationException> {
            loader().load(fileConfig(server), credentials(), mapper.valueToTree(tooManyFiles)).first()
        }
        Unit
    }

    @Test
    fun configuredRepositoryListRejectsAboveCheckpointCeiling() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val names = (1..10_001).joinToString(",") { "repo$it" }

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(config(server, "\"repositories\":${mapper.writeValueAsString(names)}"), credentials(), null).first()
        }
        Unit
    }

    @Test
    fun githubCheckpointRejectsOversizedSerializedState() = MockWebServer().use { server ->
        server.dispatcher = routes()
        val invalid = mapper.createObjectNode()
            .put("hasMore", true)
            .put("stage", "REPOSITORIES")
            .put("pullRequestCursor", "x".repeat(8 * 1024 * 1024 + 1))

        assertFailsWith<GithubConnectorValidationException> {
            loader().load(config(server), credentials(), invalid).first()
        }
        Unit
    }

    @Test
    fun completedCheckpointStartsANewPollWhileActiveCheckpointResumes() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = routes(
            requested = requested,
            pulls = listOf(pull(1)),
        )
        val completed = GithubCheckpoint(hasMore = false)

        val restarted = loader().load(config(server), credentials(), mapper.valueToTree(completed)).toList()

        assertTrue(restarted.flatMap { it.documents }.any { it.id.endsWith("/pull/1") })
        assertTrue(requested.any { it == "/repos/test-org/test-repo" })

        requested.clear()
        val active = checkpoint(stage = GithubStage.ISSUES, issuePage = 7)
        loader().load(config(server, "\"include_prs\":false,\"include_issues\":true"), credentials(), mapper.valueToTree(active)).toList()

        assertTrue(requested.any { it.contains("/issues") && it.contains("page=7") })
        assertFalse(requested.any { it == "/repos/test-org/test-repo" })
    }

    @Test
    fun githubConnectorBasic() = MockWebServer().use { server ->
        server.dispatcher = routes(
            pulls = listOf(pull(7, user = "octocat", assignee = "reviewer")),
            issues = listOf(issue(9, user = "reporter", assignee = "owner")),
        )

        val documents = loader().load(
            config(server, "\"include_issues\":true"),
            credentials(),
            null,
        ).flatMap { it.documents }.toList()

        assertEquals(2, documents.size)
        val pr = documents.first { "/pull/" in it.id }
        val issue = documents.first { "/issues/" in it.id }
        assertEquals(ConnectorSource.GITHUB, pr.source)
        assertEquals("PullRequest", pr.metadata["object_type"])
        assertEquals("test-org/test-repo", pr.metadata["repo"])
        assertEquals(listOf("octocat"), pr.primaryOwners)
        assertEquals(listOf("reviewer"), pr.secondaryOwners)
        assertEquals(Instant.parse("2026-01-02T00:00:00Z"), pr.updatedAt)
        assertEquals("Issue", issue.metadata["object_type"])
        assertEquals(listOf("reporter"), issue.primaryOwners)
        assertEquals(listOf("owner"), issue.secondaryOwners)
    }

    @Test
    fun pullRequestListFetchesDetailBeforeConversion() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                requested += request.path.orEmpty()
                return when (request.requestUrl!!.encodedPath) {
                    "/repos/test-org/test-repo" -> json(repoJson())
                    "/repos/test-org/test-repo/pulls" -> json(
                        """[{"id":70,"number":7,"title":"PR 7","html_url":"https://github.test/test-org/test-repo/pull/7","updated_at":"2026-01-02T00:00:00Z"}]""",
                    )
                    "/repos/test-org/test-repo/pulls/7" -> json(
                        pull(7, body = "detail body").replace("\"merged\":false", "\"merged\":true")
                            .replace("\"commits\":2", "\"commits\":8")
                            .replace("\"changed_files\":3", "\"changed_files\":9"),
                    )
                    else -> json("[]")
                }
            }
        }

        val document = loader().load(config(server), credentials(), null).flatMap { it.documents }.single()

        assertEquals("detail body", document.content)
        assertEquals(true, document.metadata["merged"])
        assertEquals(8, document.metadata["num_commits"])
        assertEquals(9, document.metadata["num_files_changed"])
        assertTrue(requested.any { it.startsWith("/repos/test-org/test-repo/pulls/7") })
    }

    @Test
    fun loadFromCheckpointHappyPath() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1), pull(2)), issues = listOf(issue(3), issue(4)))

        val batches = loader().load(
            config(server, "\"include_issues\":true"),
            credentials(),
            null,
        ).toList()

        assertEquals(4, batches.flatMap { it.documents }.size)
        assertEquals(listOf(2, 2), batches.filter { it.documents.isNotEmpty() }.map { it.documents.size })
        assertFalse(batches.last().checkpoint.hasMore)
        assertEquals(GithubStage.FILES, checkpointOf(batches.last()).stage)
    }

    @Test
    fun loadFromCheckpointWithRateLimit() = MockWebServer().use { server ->
        var pullsCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.requestUrl!!.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                request.requestUrl!!.encodedPath.endsWith("/pulls") && pullsCalls++ == 0 -> json("{}", 403)
                    .setHeader("X-RateLimit-Remaining", "0")
                    .setHeader("X-RateLimit-Reset", "1010")
                request.requestUrl!!.encodedPath.endsWith("/pulls") -> json("[${pull(1)}]")
                request.requestUrl!!.encodedPath.endsWith("/pulls/1") -> json(pull(1))
                else -> json("[]")
            }
        }
        val sleeps = mutableListOf<Long>()
        val loader = loader(now = { Instant.ofEpochSecond(1000) }, sleep = sleeps::add)

        val documents = loader.load(config(server), credentials(), null).flatMap { it.documents }.toList()

        assertEquals(1, documents.size)
        assertEquals(listOf(10_000L), sleeps)
        assertEquals(2, pullsCalls)
    }

    @Test
    fun rateLimitWaitAllowsExactOneHourBoundary() = MockWebServer().use { server ->
        var pullsCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.requestUrl!!.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                request.requestUrl!!.encodedPath.endsWith("/pulls") && pullsCalls++ == 0 -> json("{}", 403)
                    .setHeader("X-RateLimit-Remaining", "0")
                    .setHeader("X-RateLimit-Reset", "4600")
                request.requestUrl!!.encodedPath.endsWith("/pulls") -> json("[${pull(1)}]")
                request.requestUrl!!.encodedPath.endsWith("/pulls/1") -> json(pull(1))
                else -> json("[]")
            }
        }
        val sleeps = mutableListOf<Long>()

        loader(now = { Instant.ofEpochSecond(1000) }, sleep = sleeps::add)
            .load(config(server), credentials(), null).toList()

        assertEquals(3_600_000L, sleeps.sum())
        assertTrue(sleeps.all { it <= 15_000L })
    }

    @Test
    fun slimRateLimitWaitSendsHeartbeats() = MockWebServer().use { server ->
        var pullsCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.requestUrl!!.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                request.requestUrl!!.encodedPath.endsWith("/pulls") && pullsCalls++ == 0 -> json("{}", 403)
                    .setHeader("X-RateLimit-Remaining", "0")
                    .setHeader("X-RateLimit-Reset", "1070")
                request.requestUrl!!.encodedPath.endsWith("/pulls") -> json("[${pull(1)}]")
                request.requestUrl!!.encodedPath.endsWith("/pulls/1") -> json(pull(1))
                else -> json("[]")
            }
        }
        val sleeps = mutableListOf<Long>()
        var heartbeats = 0

        loader(now = { Instant.ofEpochSecond(1000) }, sleep = sleeps::add)
            .retrieveAllSlimDocuments(
                config(server),
                credentials(),
                heartbeat = { heartbeats += 1 },
            ).toList()

        assertEquals(70_000L, sleeps.sum())
        assertTrue(sleeps.all { it <= 15_000L })
        assertTrue(heartbeats >= sleeps.size)
    }

    @Test
    fun rateLimitWaitRejectsExcessiveResetEpoch() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.requestUrl!!.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                else -> json("{}", 403)
                    .setHeader("X-RateLimit-Remaining", "0")
                    .setHeader("X-RateLimit-Reset", Long.MAX_VALUE.toString())
            }
        }

        assertFailsWith<GithubRateLimitValidationException> {
            loader(now = { Instant.ofEpochSecond(1000) }, sleep = {}).load(config(server), credentials(), null).toList()
        }
        Unit
    }

    @Test
    fun loadFromCheckpointWithEmptyRepo() = MockWebServer().use { server ->
        server.dispatcher = routes()

        val batches = loader().load(
            config(server, "\"include_issues\":true"),
            credentials(),
            null,
        ).toList()

        assertTrue(batches.flatMap { it.documents }.isEmpty())
        assertFalse(batches.last().checkpoint.hasMore)
    }

    @Test
    fun missingConfiguredRepositoryYieldsFailureAndContinues() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
                "/repos/test-org/missing" -> json("{\"message\":\"Not Found\"}", 404)
                "/repos/test-org/valid" -> json(repoJson(name = "valid", id = 2))
                "/repos/test-org/valid/pulls" -> json("[${pull(1, repo = "valid")}]")
                "/repos/test-org/valid/pulls/1" -> json(pull(1, repo = "valid"))
                else -> json("[]")
            }
        }

        val batches = loader().load(
            config(server, "\"repositories\":\"missing,valid\""),
            credentials(),
            null,
        ).toList()

        assertEquals(listOf("github_repository_not_found"), batches.flatMap { it.failures }.map { it.errorType })
        assertEquals(listOf("https://github.test/test-org/valid/pull/1"), batches.flatMap { it.documents }.map { it.id })
        assertFalse(batches.last().checkpoint.hasMore)
    }

    @Test
    fun loadFromCheckpointWithPrsOnly() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1), pull(2)), issues = listOf(issue(3)))

        val documents = loader().load(config(server), credentials(), null).flatMap { it.documents }.toList()

        assertEquals(2, documents.size)
        assertTrue(documents.all { "/pull/" in it.id })
    }

    @Test
    fun loadFromCheckpointWithIssuesOnly() = MockWebServer().use { server ->
        server.dispatcher = routes(issues = listOf(issue(1), issue(2)))

        val documents = loader().load(
            config(server, "\"include_prs\":false,\"include_issues\":true"),
            credentials(),
            null,
        ).flatMap { it.documents }.toList()

        assertEquals(2, documents.size)
        assertTrue(documents.all { "/issues/" in it.id })
    }

    @Test
    fun validateConnectorSettingsErrors() {
        listOf(
            401 to GithubCredentialExpiredException::class,
            403 to GithubInsufficientPermissionsException::class,
            404 to GithubConnectorValidationException::class,
        ).forEach { (status, type) ->
            MockWebServer().use { server ->
                server.dispatcher = object : Dispatcher() {
                    override fun dispatch(request: RecordedRequest) = json("{}", status)
                }

                val error = assertFailsWith<GithubConnectorValidationException> {
                    loader().validate(config(server), credentials())
                }

                assertEquals(type, error::class)
            }
        }
    }

    @Test
    fun validateConnectorSettingsSurfacesTypedError() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
                "/orgs/test-org/repos" -> json("{}", 404)
                "/users/test-org/repos" -> json("[]")
                else -> json("{}", 404)
            }
        }

        val error = assertFailsWith<GithubConnectorValidationException> {
            loader().validate(config(server, "\"repositories\":\"\""), credentials())
        }

        assertContains(error.message.orEmpty(), "Found no repos for user")
    }

    @Test
    fun validateRequiresAtLeastOneEnabledStage() = MockWebServer().use { server ->
        server.dispatcher = routes()

        assertFailsWith<GithubConnectorValidationException> {
            loader().validate(
                config(server, "\"include_prs\":false,\"include_issues\":false,\"include_files\":false"),
                credentials(),
            )
        }
        Unit
    }

    @Test
    fun validateConnectorSettingsSuccess() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = routes(requested = requested)

        loader().validate(config(server), credentials())

        assertEquals("/repos/test-org/test-repo", requested.single())
    }

    @Test
    fun loadFromCheckpointWithCursorFallback() = MockWebServer().use { server ->
        var offsetFailed = false
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val url = request.requestUrl!!
                return when {
                    url.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                    url.encodedPath.endsWith("/pulls") && url.queryParameter("page") != null -> {
                        offsetFailed = true
                        json("{\"message\":\"use cursor pagination\"}", 422)
                    }
                    url.encodedPath.endsWith("/pulls") -> json("[${pull(1)},${pull(2)}]")
                    "/pulls/" in url.encodedPath -> json(
                        if (url.encodedPath.endsWith("/1")) pull(1) else pull(2),
                    )
                    else -> json("[]")
                }
            }
        }

        val documents = loader().load(config(server), credentials(), null).flatMap { it.documents }.toList()

        assertTrue(offsetFailed)
        assertEquals(listOf(1, 2), documents.map { it.metadata["id"] })
    }

    @Test
    fun loadFromCheckpointResumeCursorPagination() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = routes(requested = requested, cursorPulls = listOf(pull(3), pull(4)))
        val active = checkpoint(
            stage = GithubStage.PULL_REQUESTS,
            pullRequestCursor = "/repos/test-org/test-repo/pulls?after=abc&per_page=100",
            pullRequestsRetrieved = 2,
        )

        val documents = loader().load(config(server), credentials(), mapper.valueToTree(active))
            .flatMap { it.documents }.toList()

        assertEquals(listOf(3, 4), documents.map { it.metadata["id"] })
        assertTrue(requested.first().contains("after=abc"))
        assertFalse(requested.any { it == "/repos/test-org/test-repo" })
    }

    @Test
    fun loadFromCheckpointCursorExpiration() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val url = request.requestUrl!!
                return when {
                    url.queryParameter("after") == "expired" -> json("{\"message\":\"Cursor expired\"}", 422)
                    url.encodedPath.endsWith("/pulls") -> json("[${pull(1)},${pull(2)},${pull(3)}]")
                    "/pulls/" in url.encodedPath -> json(pull(url.encodedPath.substringAfterLast('/').toInt()))
                    else -> json("[]")
                }
            }
        }
        val active = checkpoint(
            stage = GithubStage.PULL_REQUESTS,
            pullRequestCursor = "/repos/test-org/test-repo/pulls?after=expired&per_page=100",
            pullRequestsRetrieved = 2,
        )

        val documents = loader().load(config(server), credentials(), mapper.valueToTree(active))
            .flatMap { it.documents }.toList()

        assertEquals(listOf(3), documents.map { it.metadata["id"] })
    }

    @Test
    fun repeatedCursorExpirationCountsOnlyUnseenItems() = MockWebServer().use { server ->
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        var restartCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val url = request.requestUrl!!
                return when {
                    url.queryParameter("after") != null -> json("{\"message\":\"Cursor expired\"}", 422)
                    url.encodedPath.endsWith("/pulls") && restartCalls++ == 0 ->
                        json("[${pull(1)},${pull(2)},${pull(3)}]")
                            .setHeader("Link", "<$base/repos/test-org/test-repo/pulls?after=expired2>; rel=\"next\"")
                    url.encodedPath.endsWith("/pulls") -> json("[${pull(1)},${pull(2)},${pull(3)},${pull(4)}]")
                    "/pulls/" in url.encodedPath -> json(pull(url.encodedPath.substringAfterLast('/').toInt()))
                    else -> json("[]")
                }
            }
        }
        val active = checkpoint(
            stage = GithubStage.PULL_REQUESTS,
            pullRequestCursor = "/repos/test-org/test-repo/pulls?after=expired1",
            pullRequestsRetrieved = 2,
        )

        val batches = loader().load(config(server), credentials(), mapper.valueToTree(active)).toList()

        val documentBatches = batches.filter { it.documents.isNotEmpty() }
        assertEquals(listOf(3), documentBatches.first().documents.map { it.metadata["id"] })
        assertEquals(3, checkpointOf(documentBatches.first()).pullRequestsRetrieved)
        assertEquals(listOf(4), documentBatches.last().documents.map { it.metadata["id"] })
    }

    @Test
    fun loadFromCheckpointCursorPaginationCompletion() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val url = request.requestUrl!!
                return when {
                    url.encodedPath.endsWith("/repo1/pulls") -> json("[${pull(1, repo = "repo1")}]")
                    url.encodedPath.endsWith("/repo1/pulls/1") -> json(pull(1, repo = "repo1"))
                    url.encodedPath == "/repos/test-org/repo2" -> json(repoJson("repo2", id = 2))
                    url.encodedPath.endsWith("/repo2/pulls") -> json("[${pull(2, repo = "repo2")}]")
                    url.encodedPath.endsWith("/repo2/pulls/2") -> json(pull(2, repo = "repo2"))
                    else -> json("[]")
                }
            }
        }
        val active = checkpoint(
            repo = "repo1",
            stage = GithubStage.PULL_REQUESTS,
            repositories = listOf(repository("repo1"), repository("repo2", id = 2)),
            pullRequestCursor = "/repos/test-org/repo1/pulls?after=abc&per_page=100",
        )

        val documents = loader().load(
            config(server, "\"repositories\":\"repo1,repo2\""),
            credentials(),
            mapper.valueToTree(active),
        ).flatMap { it.documents }.toList()

        assertEquals(
            setOf("test-org/repo1", "test-org/repo2"),
            documents.map { it.metadata["repository"] }.toSet(),
        )
    }

    @Test
    fun filesNotIndexedWhenDisabled() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = routes(requested = requested)

        loader().load(
            config(server, "\"include_prs\":false,\"include_files\":false"),
            credentials(),
            null,
        ).toList()

        assertFalse(requested.any { "/git/trees/" in it })
    }

    @Test
    fun filesIndexedWhenEnabled() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(
            mapOf(
                "README.md" to "# Hello world".toByteArray(),
                "docs/guide.md" to "a guide".toByteArray(),
                "src/main.py" to "print('hi')".toByteArray(),
                "logo.png" to byteArrayOf(1, 2),
            ),
        )

        val documents = fileLoad(server).flatMap { it.documents }

        assertEquals(listOf("README.md", "docs/guide.md"), documents.map { it.metadata["path"] }.sortedBy(Any?::toString))
        assertEquals(listOf("test-org", "test-repo", "files", "README.md"), documents.first().metadata["source_path"])
    }

    @Test
    fun binaryFileYieldsFailure() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(mapOf("corrupt.md" to byteArrayOf(-1, -2, 0, 1, -128, -127)))

        val batch = fileLoad(server).first { it.failures.isNotEmpty() }

        assertEquals("github_file_decode", batch.failures.single().errorType)
    }

    @Test
    fun undecodableContentYieldsFailure() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(
            mapOf("big.md" to "placeholder".toByteArray()),
            undecodable = setOf("big.md"),
        )

        val failure = fileLoad(server).flatMap { it.failures }.single()

        assertContains(failure.message, "Could not decode")
    }

    @Test
    fun pushedAtGateSkipsFileStage() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(
            mapOf("README.md" to "hello".toByteArray()),
            pushedAt = "2020-01-01T00:00:00Z",
            requested = requested,
        )

        val batches = loader().load(
            fileConfig(server),
            credentials(),
            null,
            start = Instant.parse("2023-01-01T00:00:00Z"),
        ).toList()

        assertTrue(batches.flatMap { it.documents }.isEmpty())
        assertFalse(requested.any { "/git/trees/" in it })
    }

    @Test
    fun filesPaginatedAcrossCheckpoints() = MockWebServer().use { server ->
        val files = (0 until 250).associate { "doc%03d.md".format(it) to "content $it".toByteArray() }
        server.dispatcher = fileRoutes(files)

        val batches = fileLoad(server)

        assertEquals(250, batches.sumOf { it.documents.size })
        assertEquals(listOf(100, 100, 50), batches.filter { it.documents.isNotEmpty() }.map { it.documents.size })
        assertFalse(batches.last().checkpoint.hasMore)
    }

    @Test
    fun terminalCheckpointDropsMaterializedRepositoryAndFileLists() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(mapOf("README.md" to "hello".toByteArray()))

        val terminal = fileLoad(server).last().checkpoint.value

        assertEquals(0, terminal.path("repositories").size())
        assertEquals(0, terminal.path("filePaths").size())
    }

    @Test
    fun extensionlessDocsIndexed() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(
            mapOf(
                "README" to "readme".toByteArray(),
                "LICENSE" to "MIT".toByteArray(),
                "Makefile" to "all:".toByteArray(),
            ),
        )

        val paths = fileLoad(server).flatMap { it.documents }.map { it.metadata["path"] }

        assertEquals(listOf("LICENSE", "README"), paths.sortedBy(Any?::toString))
    }

    @Test
    fun truncatedTreeYieldsFailure() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(mapOf("README.md" to "# Hi".toByteArray()), truncated = true)

        val batches = fileLoad(server)

        assertEquals(1, batches.sumOf { it.documents.size })
        val failure = batches.flatMap { it.failures }.single()
        assertIs<FailureTarget.Entity>(failure.target)
        assertContains(failure.message.lowercase(), "truncated")
        assertFalse(batches.first { it.failures.isNotEmpty() }.enumerationComplete)
    }

    @Test
    fun truncatedTreeAlsoFailsSlimEnumeration() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(mapOf("README.md" to "# Hi".toByteArray()), truncated = true)

        val failures = loader().retrieveAllSlimDocuments(fileConfig(server), credentials()).flatMap { it.failures }.toList()

        assertEquals(listOf("github_tree_truncated"), failures.map { it.errorType })
        assertIs<FailureTarget.Entity>(failures.single().target)
        Unit
    }

    @Test
    fun collectionFailureUsesCanonicalDocumentUrl() = MockWebServer().use { server ->
        val url = "https://github.test/test-org/test-repo/pull/99"
        server.dispatcher = routes(
            pulls = listOf("""{"id":99,"number":0,"html_url":"$url","updated_at":"2026-01-01T00:00:00Z"}"""),
        )

        val failure = loader().load(config(server), credentials(), null).flatMap { it.failures }.single()

        assertEquals(url, (failure.target as FailureTarget.Document).id)
    }

    @Test
    fun emptyRepositoryTreeSkipsFileStage() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(emptyMap(), treeStatus = 409)

        val batches = fileLoad(server)

        assertTrue(batches.flatMap { it.documents }.isEmpty())
        assertFalse(batches.last().checkpoint.hasMore)
    }

    @Test
    fun branchOverrideThreadsThroughListingFetchingAndUrls() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(
            mapOf("index.md" to "home".toByteArray(), "docs/setup.md" to "setup".toByteArray()),
            requested = requested,
        )

        val documents = fileLoad(server, branch = "gh-pages").flatMap { it.documents }

        assertTrue(documents.all { "/blob/gh-pages/" in it.id && it.metadata["branch"] == "gh-pages" })
        assertTrue(requested.any { it.contains("/git/trees/gh-pages") })
        assertEquals(2, requested.count { it.contains("ref=gh-pages") })
    }

    @Test
    fun defaultBranchUsedWhenBranchUnset() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(mapOf("README.md" to "hello".toByteArray()), requested = requested)

        fileLoad(server)

        assertTrue(requested.any { it.contains("/git/trees/main") })
        assertTrue(requested.any { it.contains("ref=main") })
    }

    @Test
    fun blankBranchNormalizedToNone() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(mapOf("README.md" to "hello".toByteArray()), requested = requested)

        fileLoad(server, branch = "   ")

        assertTrue(requested.any { it.contains("/git/trees/main") })
    }

    @Test
    fun resumedCheckpointFromOtherBranchRelists() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(mapOf("new-only.md" to "new".toByteArray()), requested = requested)
        val active = checkpoint(
            stage = GithubStage.FILES,
            filePaths = listOf("old-only.md"),
            fileOffset = 1,
            branch = "main",
        )

        val documents = loader().load(
            fileConfig(server, "gh-pages"),
            credentials(),
            mapper.valueToTree(active),
        ).flatMap { it.documents }.toList()

        assertEquals(listOf("new-only.md"), documents.map { it.metadata["path"] })
        assertTrue(requested.any { it.contains("/git/trees/gh-pages") })
    }

    @Test
    fun resumedBranchChangeBypassesPushedAtGate() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        server.dispatcher = fileRoutes(
            mapOf("new-only.md" to "new".toByteArray()),
            pushedAt = "2020-01-01T00:00:00Z",
            requested = requested,
        )
        val active = checkpoint(
            stage = GithubStage.FILES,
            filePaths = listOf("old-only.md"),
            fileOffset = 1,
            branch = "main",
        )

        val documents = loader().load(
            fileConfig(server, "gh-pages"),
            credentials(),
            mapper.valueToTree(active),
            start = Instant.parse("2023-01-01T00:00:00Z"),
        ).flatMap { it.documents }.toList()

        assertEquals(1, documents.size)
        assertTrue(requested.any { it.contains("/git/trees/gh-pages") })
    }

    @Test
    fun nonexistentBranchRaisesClearError() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(emptyMap(), treeStatus = 404)

        val error = assertFailsWith<GithubConnectorValidationException> {
            fileLoad(server, branch = "no-such-branch")
        }

        assertContains(error.message.orEmpty(), "no-such-branch")
    }

    @Test
    fun prsDisabled404DoesNotCrashFiles() = MockWebServer().use { server ->
        server.dispatcher = fileRoutes(
            mapOf("README.md" to "# Hi".toByteArray()),
            pullsStatus = 404,
        )

        val documents = loader().load(
            config(server, "\"include_prs\":true,\"include_files\":true"),
            credentials(),
            null,
        ).flatMap { it.documents }.toList()

        assertEquals(listOf("README.md"), documents.map { it.metadata["path"] })
    }

    @Test
    fun filesPaginatedWithIssuesEnabledNoStageRegression() = MockWebServer().use { server ->
        val requested = mutableListOf<String>()
        val files = (0 until 250).associate { "doc%03d.md".format(it) to "content $it".toByteArray() }
        server.dispatcher = fileRoutes(files, requested = requested)

        val documents = loader().load(
            config(server, "\"include_prs\":false,\"include_issues\":true,\"include_files\":true"),
            credentials(),
            null,
        ).flatMap { it.documents }.toList()

        assertEquals(250, documents.size)
        assertEquals(250, documents.map { it.id }.toSet().size)
        assertEquals(1, requested.count { it.contains("/git/trees/") })
    }

    @Test
    fun isIndexablePath() {
        val accepted = listOf("README.md", "docs/guide.mdx", "notes.txt", "manual.rst", "README", "LICENSE")
        val rejected = listOf(
            "main.py", "logo.png", "data.json", "table.csv", "config.yaml", "schema.sql", "output.log",
            "node_modules/pkg/README.md", ".git/config.md", "Makefile", "Dockerfile",
        )

        assertTrue(accepted.all { GithubConnectorLoader.isIndexablePath(it, 100) })
        assertTrue(rejected.all { !GithubConnectorLoader.isIndexablePath(it, 100) })
        assertFalse(GithubConnectorLoader.isIndexablePath("BIG.md", 5_000_000))
        assertTrue(GithubConnectorLoader.isIndexablePath("README.md", null))
    }

    @Test
    fun retrieveAllSlimDocsSkipsPrIssues() = MockWebServer().use { server ->
        server.dispatcher = routes(
            pulls = emptyList(),
            issues = listOf(issue(99, isPullRequest = true), issue(1)),
        )

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"include_prs\":false,\"include_issues\":true"),
            credentials(),
        ).flatMap { it.documents }.toList()

        assertEquals(listOf("https://github.test/test-org/test-repo/issues/1"), documents.map { it.id })
    }

    @Test
    fun slimRetrievalDoesNotCopyPullRequestBody() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1, body = "must not be copied")))

        val document = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents }.single()

        assertEquals("", document.content)
        assertEquals(document.id, document.link)
    }

    @Test
    fun githubConnectorImplementsSlimConnector() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1)))

        val ids = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents }.map { it.id }.toList()

        assertEquals(listOf("https://github.test/test-org/test-repo/pull/1"), ids)
    }

    @Test
    fun retrieveAllSlimDocsReturnsPrUrls() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1), pull(2), pull(3)))

        val ids = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents }.map { it.id }.toSet()

        assertEquals(
            setOf(
                "https://github.test/test-org/test-repo/pull/1",
                "https://github.test/test-org/test-repo/pull/2",
                "https://github.test/test-org/test-repo/pull/3",
            ),
            ids,
        )
    }

    @Test
    fun retrieveAllSlimDocsHasPublicExternalAccess() = MockWebServer().use { server ->
        server.dispatcher = routes(pulls = listOf(pull(1)))

        val document = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents }.single()

        assertEquals(ExternalAccess(isPublic = true), document.externalAccess)
    }

    private fun loader(
        now: () -> Instant = Instant::now,
        sleep: (Long) -> Unit = {},
    ) = GithubConnectorLoader(RemoteJsonClient(WebClient.builder()), mapper, sleep, now)

    private fun credentials(): JsonNode = mapper.readTree("""{"github_access_token":"token"}""")

    private fun config(server: MockWebServer, extra: String = ""): JsonNode = mapper.readTree(
        """{"github_base_url":"${server.startAndBase()}","repo_owner":"test-org","repositories":"test-repo","include_prs":true,"include_issues":false,"include_files":false${if (extra.isBlank()) "" else ",$extra"}}""",
    )

    private fun fileConfig(server: MockWebServer, branch: String? = null): JsonNode = config(
        server,
        "\"include_prs\":false,\"include_files\":true${branch?.let { ",\"branch\":${mapper.writeValueAsString(it)}" }.orEmpty()}",
    )

    private fun fileLoad(server: MockWebServer, branch: String? = null): List<ConnectorBatch> =
        loader().load(fileConfig(server, branch), credentials(), null).toList()

    private fun checkpointOf(batch: ConnectorBatch): GithubCheckpoint =
        mapper.treeToValue(batch.checkpoint.value, GithubCheckpoint::class.java)

    private fun checkpoint(
        repo: String = "test-repo",
        stage: GithubStage,
        repositories: List<GithubRepository> = listOf(repository(repo)),
        repositoryIndex: Int = 0,
        issuePage: Int = 1,
        pullRequestCursor: String? = null,
        pullRequestsRetrieved: Int = 0,
        filePaths: List<String>? = null,
        fileOffset: Int = 0,
        branch: String? = null,
    ) = GithubCheckpoint(
        stage = stage,
        repositories = repositories,
        repository = repository(repo),
        repositoryIndex = repositoryIndex,
        issuePage = issuePage,
        pullRequestCursor = pullRequestCursor,
        pullRequestsRetrieved = pullRequestsRetrieved,
        filePaths = filePaths,
        fileOffset = fileOffset,
        branch = branch,
    )

    private fun repository(name: String, id: Long = 1, branch: String = "main") = GithubRepository(
        id = id,
        name = name,
        fullName = "test-org/$name",
        isPrivate = false,
        htmlUrl = "https://github.test/test-org/$name",
        defaultBranch = branch,
        pushedAt = "2026-01-01T00:00:00Z",
    )

    private fun routes(
        pulls: List<String> = emptyList(),
        issues: List<String> = emptyList(),
        cursorPulls: List<String> = emptyList(),
        requested: MutableList<String> = mutableListOf(),
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            requested += request.path.orEmpty()
            val url = request.requestUrl!!
            return when {
                url.encodedPath == "/repos/test-org/test-repo" -> json(repoJson())
                "/pulls/" in url.encodedPath -> {
                    val number = url.encodedPath.substringAfterLast('/').toInt()
                    val detail = (pulls + cursorPulls).firstOrNull { mapper.readTree(it).path("number").asInt() == number }
                    json(detail ?: "{}")
                }
                url.encodedPath.endsWith("/pulls") && url.queryParameter("after") != null ->
                    json(cursorPulls.joinToString(",", "[", "]"))
                url.encodedPath.endsWith("/pulls") -> json(pulls.joinToString(",", "[", "]"))
                url.encodedPath.endsWith("/issues") -> json(issues.joinToString(",", "[", "]"))
                else -> json("[]")
            }
        }
    }

    private fun fileRoutes(
        files: Map<String, ByteArray>,
        pushedAt: String = "2026-01-01T00:00:00Z",
        truncated: Boolean = false,
        undecodable: Set<String> = emptySet(),
        treeStatus: Int = 200,
        pullsStatus: Int = 200,
        requested: MutableList<String> = mutableListOf(),
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            requested += request.path.orEmpty()
            val url = request.requestUrl!!
            return when {
                url.encodedPath == "/repos/test-org/test-repo" -> json(repoJson(pushedAt = pushedAt))
                url.encodedPath.endsWith("/pulls") -> json("[]", pullsStatus)
                url.encodedPath.endsWith("/issues") -> json("[]")
                "/git/trees/" in url.encodedPath -> {
                    if (treeStatus != 200) {
                        json(
                            if (treeStatus == 409) "{\"message\":\"Git Repository is empty.\"}" else "{\"message\":\"Not Found\"}",
                            treeStatus,
                        )
                    } else {
                        val tree = files.map { (path, bytes) ->
                            mapOf("path" to path, "type" to "blob", "size" to bytes.size)
                        }
                        json(mapper.writeValueAsString(mapOf("truncated" to truncated, "tree" to tree)))
                    }
                }
                "/contents/" in url.encodedPath -> {
                    val path = url.encodedPath.substringAfter("/contents/")
                    if (path in undecodable) {
                        json("""{"encoding":"none","content":null}""")
                    } else {
                        json(
                            mapper.writeValueAsString(
                                mapOf(
                                    "encoding" to "base64",
                                    "content" to Base64.getEncoder().encodeToString(files.getValue(path)),
                                ),
                            ),
                        )
                    }
                }
                else -> json("[]")
            }
        }
    }

    private fun repoJson(
        name: String = "test-repo",
        id: Long = 1,
        private: Boolean = false,
        pushedAt: String = "2026-01-01T00:00:00Z",
    ): String = mapper.writeValueAsString(
        mapOf(
            "id" to id,
            "name" to name,
            "full_name" to "test-org/$name",
            "private" to private,
            "html_url" to "https://github.test/test-org/$name",
            "default_branch" to "main",
            "pushed_at" to pushedAt,
        ),
    )

    private fun pull(
        number: Int,
        repo: String = "test-repo",
        body: String = "PR body",
        user: String = "author",
        assignee: String? = null,
    ): String = mapper.writeValueAsString(
        mapOf(
            "id" to number * 10,
            "number" to number,
            "title" to "PR $number",
            "body" to body,
            "html_url" to "https://github.test/test-org/$repo/pull/$number",
            "state" to "open",
            "merged" to false,
            "user" to mapOf("login" to user, "email" to "$user@example.com"),
            "assignees" to listOfNotNull(assignee?.let { mapOf("login" to it) }),
            "labels" to listOf(mapOf("name" to "docs")),
            "created_at" to "2026-01-01T00:00:00Z",
            "updated_at" to "2026-01-02T00:00:00Z",
            "commits" to 2,
            "changed_files" to 3,
            "base" to mapOf("repo" to mapOf("full_name" to "test-org/$repo")),
        ),
    )

    private fun issue(
        number: Int,
        user: String = "author",
        assignee: String? = null,
        isPullRequest: Boolean = false,
    ): String = mapper.writeValueAsString(
        buildMap<String, Any?> {
            put("id", number * 10)
            put("number", number)
            put("title", "Issue $number")
            put("body", "Issue body")
            put("html_url", "https://github.test/test-org/test-repo/issues/$number")
            put("state", "open")
            put("user", mapOf("login" to user, "email" to "$user@example.com"))
            put("assignees", listOfNotNull(assignee?.let { mapOf("login" to it) }))
            put("labels", listOf(mapOf("name" to "bug")))
            put("created_at", "2026-01-01T00:00:00Z")
            put("updated_at", "2026-01-02T00:00:00Z")
            put("repository_url", "https://api.github.test/repos/test-org/test-repo")
            if (isPullRequest) put("pull_request", mapOf("url" to "pull"))
        },
    )

    private fun json(body: String, status: Int = 200): MockResponse =
        MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body)

    private fun MockWebServer.startAndBase(): String {
        if (port == -1) start()
        return url("/").toString().trimEnd('/')
    }
}
