package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.onyx.foss.kotlin.domain.ConnectorSource
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.net.URI
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class JiraConnectorLoaderTest {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    @Test
    fun jiraCheckpointRoundTripsThroughJson() {
        val checkpoint = JiraCheckpoint(
            allIssueIds = listOf(listOf("10001", "10002")),
            cursor = "next-page",
            seenHierarchyNodeIds = setOf("ENG", "ENG-1"),
        )

        assertEquals(checkpoint, mapper.readValue<JiraCheckpoint>(mapper.writeValueAsString(checkpoint)))
    }

    @Test
    fun projectJqlUsesQuotedProject() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().load(
            config(server, "\"project_key\":\"ORDER\""),
            tokenCredentials(),
            null,
            start = Instant.ofEpochSecond(1),
            end = Instant.ofEpochSecond(2),
        ).single()

        assertEquals(
            "project = \"ORDER\" AND updated >= 1000 AND updated <= 2000",
            server.takeRequest().requestUrl!!.queryParameter("jql"),
        )
    }

    @Test
    fun customJqlOverridesProjectJql() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().load(
            config(server, "\"project_key\":\"ENG\",\"jql_query\":\"labels = customer\""),
            tokenCredentials(),
            null,
            start = Instant.ofEpochSecond(1),
            end = Instant.ofEpochSecond(2),
        ).single()

        assertEquals(
            "(labels = customer) AND updated >= 1000 AND updated <= 2000",
            server.takeRequest().requestUrl!!.queryParameter("jql"),
        )
    }

    @Test
    fun jqlWithoutProjectUsesOnlyPollWindow() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().load(
            config(server, "\"batch_size\":50"),
            tokenCredentials(),
            null,
            start = Instant.ofEpochSecond(3),
            end = Instant.ofEpochSecond(4),
        ).single()

        assertEquals(
            "updated >= 3000 AND updated <= 4000",
            server.takeRequest().requestUrl!!.queryParameter("jql"),
        )
    }

    @Test
    fun jiraDocSyncPassesIndexingStart() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().retrieveAllSlimDocuments(
            config(server, "\"project_key\":\"ENG\""),
            tokenCredentials(),
            start = Instant.parse("2025-06-01T00:00:00Z"),
            includePermissions = true,
        ).single()

        assertEquals(
            "project = \"ENG\" AND updated >= 1748736000000",
            server.takeRequest().requestUrl!!.queryParameter("jql"),
        )
    }

    @Test
    fun jiraDocSyncPassesNoneWhenNoIndexingStart() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().retrieveAllSlimDocuments(
            config(server, "\"project_key\":\"ENG\""),
            tokenCredentials(),
            includePermissions = true,
        ).single()

        assertEquals("project = \"ENG\"", server.takeRequest().requestUrl!!.queryParameter("jql"))
    }

    @Test
    fun serverPaginationResumesFromOffsetAndYieldsEachPage() = MockWebServer().use { server ->
        server.json(serverPage(total = 4, issue("ENG-2"), issue("ENG-3")))
        server.json(serverPage(total = 4, issue("ENG-4")))

        val batches = loader().load(
            config(server, "\"project_key\":\"ENG\",\"batch_size\":2"),
            tokenCredentials(),
            mapper.valueToTree(JiraCheckpoint(offset = 1)),
        ).toList()

        assertEquals(listOf(2, 1), batches.map { it.documents.size })
        assertEquals(listOf(3, 4), batches.map { mapper.treeToValue(it.checkpoint.value, JiraCheckpoint::class.java).offset })
        assertEquals(listOf(true, false), batches.map { it.checkpoint.hasMore })
        assertEquals("1", server.takeRequest().requestUrl!!.queryParameter("startAt"))
        assertEquals("3", server.takeRequest().requestUrl!!.queryParameter("startAt"))
    }

    @Test
    fun cloudCursorResumeDrainsPendingIdsBeforeFetchingTheNextCursor() = MockWebServer().use { server ->
        server.json(bulkPage(issue("ENG-2", id = "10002")))
        server.json("""{"issues":[{"id":"10003"}]}""")
        server.json(bulkPage(issue("ENG-3", id = "10003")))
        val checkpoint = JiraCheckpoint(
            allIssueIds = listOf(listOf("10002")),
            cursor = "cursor-2",
        )

        val batches = loader().load(
            config(server, "\"project_key\":\"ENG\""),
            cloudCredentials(),
            mapper.valueToTree(checkpoint),
        ).toList()

        assertEquals(listOf("ENG-2", "ENG-3"), batches.flatMap { it.documents }.map { it.metadata["key"] })
        assertEquals("/rest/api/3/issue/bulkfetch", server.takeRequest().requestUrl!!.encodedPath)
        val resumedSearch = server.takeRequest()
        assertEquals("cursor-2", resumedSearch.requestUrl!!.queryParameter("nextPageToken"))
        assertFalse(batches.last().checkpoint.hasMore)
    }

    @Test
    fun completedCloudCheckpointStartsANewPollWindow() = MockWebServer().use { server ->
        server.json("""{"issues":[{"id":"10004"}]}""")
        server.json(bulkPage(issue("ENG-4", id = "10004")))
        val completed = JiraCheckpoint(hasMore = false, idsDone = true, seenHierarchyNodeIds = setOf("ENG"))

        val batches = loader().load(
            config(server, "\"project_key\":\"ENG\""),
            cloudCredentials(),
            mapper.valueToTree(completed),
            start = Instant.ofEpochSecond(5),
            end = Instant.ofEpochSecond(6),
        ).toList()

        assertEquals(listOf("ENG-4"), batches.flatMap { it.documents }.map { it.metadata["key"] })
        assertTrue(server.takeRequest().requestUrl!!.queryParameter("jql")!!.contains("updated >= 5000"))
    }

    @Test
    fun scopedTokenUsesTenantCloudIdAndKeepsIssueLinksOnTheTenant() = MockWebServer().use { server ->
        server.json("""{"cloudId":"cloud-123"}""")
        server.json("""{"issues":[{"id":"10001"}]}""")
        server.json(bulkPage(issue("ENG-1", id = "10001")))
        val client = RemoteJsonClient(
            WebClient.builder().filter(rewriteAtlassianRequestsTo(server)),
        )

        val document = JiraConnectorLoader(client, mapper).load(
            config(server, "\"project_key\":\"ENG\",\"scoped_token\":true"),
            cloudCredentials(),
            null,
        ).single().documents.single()

        assertEquals("/_edge/tenant_info", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/ex/jira/cloud-123/rest/api/3/search/jql", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals(server.url("/").toString().trimEnd('/') + "/browse/ENG-1", document.link)
    }

    @Test
    fun skippedLabelDropsOnlyThatIssueAndAllowedCommentsRemain() = MockWebServer().use { server ->
        val good = issue(
            "ENG-1",
            description = "Issue body",
            labels = listOf("public"),
            extraFields = ""","comment":{"comments":[
                {"body":"Visible","author":{"emailAddress":"person@example.com"}},
                {"body":"Hidden","author":{"emailAddress":"bot@example.com"}}
            ]},"parent":{"key":"ENG-EPIC","fields":{"issuetype":{"name":"Epic"}}}""",
        )
        val skipped = issue("ENG-2", labels = listOf("secret"))
        server.json(serverPage(total = 2, good, skipped))

        val batch = loader().load(
            config(
                server,
                "\"project_key\":\"ENG\",\"labels_to_skip\":[\"secret\"],\"comment_email_blacklist\":[\"bot@example.com\"]",
            ),
            tokenCredentials(),
            null,
        ).single()

        val document = batch.documents.single()
        assertEquals("ENG-1 Summary ENG-1", document.title)
        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), document.updatedAt)
        assertEquals(ConnectorSource.JIRA, document.source)
        assertEquals(listOf("Reporter", "Assignee"), document.primaryOwners)
        assertTrue(document.content.contains("Issue body"))
        assertTrue(document.content.contains("Comment: Visible"))
        assertFalse(document.content.contains("Hidden"))
        assertEquals(
            mapOf(
                "source" to "jira",
                "key" to "ENG-1",
                "updated" to "2026-01-01T00:00:00.000+0000",
                "labels" to listOf("public"),
                "created" to "2026-01-01T00:00:00.000+0000",
                "priority" to "High",
                "status" to "In Progress",
                "resolution" to "Fixed",
                "issuetype" to "Story",
                "project" to "ENG",
                "project_name" to "Engineering",
                "parent" to "ENG-EPIC",
                "parent_hierarchy_raw_node_id" to "ENG-EPIC",
                "reporter" to "Reporter",
                "reporter_email" to "reporter@example.com",
                "assignee" to "Assignee",
                "assignee_email" to "assignee@example.com",
            ),
            document.metadata,
        )
        val checkpoint = mapper.treeToValue(batch.checkpoint.value, JiraCheckpoint::class.java)
        assertEquals(setOf("ENG", "ENG-EPIC"), checkpoint.seenHierarchyNodeIds)
        assertFalse(batch.enumerationComplete)
    }

    @Test
    fun cloudBulkFetchSplitsAResponseThatCannotBeDecoded() = MockWebServer().use { server ->
        server.json("""{"issues":[{"id":"1"},{"id":"2"},{"id":"3"},{"id":"4"}]}""")
        server.enqueue(MockResponse().setHeader("Content-Type", "application/json").setBody("{"))
        server.json(bulkPage(issue("ENG-1", id = "1"), issue("ENG-2", id = "2")))
        server.json(bulkPage(issue("ENG-3", id = "3"), issue("ENG-4", id = "4")))

        val documents = loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null)
            .single().documents

        assertEquals(4, documents.size)
        server.takeRequest()
        val payloads = (1..3).map { mapper.readTree(server.takeRequest().body.readUtf8()) }
        assertEquals(listOf(4, 2, 2), payloads.map { it.path("issueIdsOrKeys").size() })
        assertTrue(payloads.all { payload -> payload.path("fields").any { it.asText() == "summary" } })
    }

    @Test
    fun cloudBulkFetchRecursiveSplitRaisesForTheBadIssue() = MockWebServer().use { server ->
        server.dispatcher = badBulkIssueDispatcher(setOf("1", "BAD", "2"))

        assertFailsWith<RuntimeException> {
            loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null).toList()
        }
    }

    @Test
    fun cloudBulkFetchNeverExceedsTheApiLimit() = MockWebServer().use { server ->
        val ids = (1..101).map(Int::toString)
        val requestSizes = mutableListOf<Int>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                if (request.requestUrl!!.encodedPath.endsWith("/search/jql")) {
                    return jsonResponse(mapper.writeValueAsString(mapOf("issues" to ids.map { mapOf("id" to it) })))
                }
                val requested = mapper.readTree(request.body.readUtf8()).path("issueIdsOrKeys").map(JsonNode::asText)
                requestSizes += requested.size
                return jsonResponse(bulkPage(*requested.map { issue("ENG-$it", id = it) }.toTypedArray()))
            }
        }

        val documents = loader().load(
            config(server, "\"project_key\":\"ENG\",\"batch_size\":1000"),
            cloudCredentials(),
            null,
        ).flatMap { it.documents.asSequence() }.toList()

        assertEquals(101, documents.size)
        assertEquals(listOf(100, 1), requestSizes)
    }

    @Test
    fun cloudBulkFetchRaisesForOneUnfetchableIssue() = MockWebServer().use { server ->
        server.dispatcher = badBulkIssueDispatcher(setOf("BAD"))

        assertFailsWith<RuntimeException> {
            loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null).toList()
        }
    }

    @Test
    fun cloudBulkFetchPropagatesNonJsonHttpErrors() = MockWebServer().use { server ->
        server.json("""{"issues":[{"id":"1"}]}""")
        server.enqueue(MockResponse().setResponseCode(500).setBody("upstream failed"))

        assertFailsWith<WebClientResponseException.InternalServerError> {
            loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null).toList()
        }
    }

    @Test
    fun cloudBulkFetchMissingIssueMakesEnumerationIncomplete() = MockWebServer().use { server ->
        server.json("""{"issues":[{"id":"10001"},{"id":"10002"}]}""")
        server.json(bulkPage(issue("ENG-1", id = "10001")))

        val batch = loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null).single()

        assertEquals(listOf("ENG-1"), batch.documents.map { it.metadata["key"] })
        assertFalse(batch.enumerationComplete)
        assertEquals("jira_issue:10002", (batch.failures.single().target as FailureTarget.Entity).id)
    }

    @Test
    fun cloudBulkFetchIssueErrorsMakeEnumerationIncomplete() = MockWebServer().use { server ->
        server.json("""{"issues":[{"id":"10001"}]}""")
        server.json(
            """{"issues":[],"issueErrors":[{"issueId":"10001","errorMessages":["Issue is not visible"]}]}""",
        )

        val batch = loader().load(config(server, "\"project_key\":\"ENG\""), cloudCredentials(), null).single()

        assertTrue(batch.documents.isEmpty())
        assertFalse(batch.enumerationComplete)
        assertEquals("jira_issue:10001", (batch.failures.single().target as FailureTarget.Entity).id)
        assertTrue(batch.failures.single().message.contains("Issue is not visible"))
    }

    @Test
    fun largeIssueIsSkippedWithoutDroppingSmallIssues() = MockWebServer().use { server ->
        server.json(
            serverPage(
                total = 2,
                issue("ENG-1", description = "small"),
                issue("ENG-2", description = "x".repeat(200)),
            ),
        )

        val batch = loader().load(
            config(server, "\"project_key\":\"ENG\",\"max_ticket_size_bytes\":100"),
            tokenCredentials(),
            null,
        ).single()

        assertEquals(listOf("ENG-1"), batch.documents.map { it.metadata["key"] })
        assertTrue(batch.failures.isEmpty())
        assertFalse(batch.enumerationComplete)
    }

    @Test
    fun oneBadIssueBecomesFailureWhileGoodIssueSurvives() = MockWebServer().use { server ->
        server.json(serverPage(total = 2, issue("ENG-1"), """{"key":"ENG-2","fields":null}"""))

        val batch = loader().load(config(server, "\"project_key\":\"ENG\""), tokenCredentials(), null).single()

        assertEquals(listOf("ENG-1"), batch.documents.map { it.metadata["key"] })
        assertEquals(
            server.url("/").toString().trimEnd('/') + "/browse/ENG-2",
            (batch.failures.single().target as FailureTarget.Document).id,
        )
        assertEquals("jira_issue_processing", batch.failures.single().errorType)
    }

    @Test
    fun permissionsPrefixGroupsForIndexingAndKeepThemRawForPermissionSync() = MockWebServer().use { server ->
        repeat(2) {
            server.json(serverPage(total = 1, issue("ENG-1")))
            server.json(
                """{"permissions":[
                    {"permission":"BROWSE_PROJECTS","holder":{"type":"group","parameter":"jira-users"}},
                    {"permission":"BROWSE_PROJECTS","holder":{"type":"user","user":{"emailAddress":"reader@example.com"}}}
                ]}""",
            )
        }
        val config = config(server, "\"project_key\":\"ENG\",\"include_permissions\":true")

        val indexed = loader().load(config, tokenCredentials(), null).single().documents.single().externalAccess
        val permissionSync = loader().load(config, tokenCredentials(), null, permissionSync = true)
            .single().documents.single().externalAccess

        assertEquals(setOf("jira_jira-users"), assertNotNull(indexed).externalUserGroupIds)
        assertEquals(setOf("jira-users"), assertNotNull(permissionSync).externalUserGroupIds)
        assertEquals(setOf("reader@example.com"), indexed.externalUserEmails)
        assertFalse(indexed.isPublic)
    }

    @Test
    fun projectRolePermissionsResolveGroupAndUserActors() = MockWebServer().use { server ->
        server.json(serverPage(total = 1, issue("ENG-1")))
        server.json(
            """{"permissions":[
                {"permission":"BROWSE_PROJECTS","holder":{"type":"projectRole","parameter":"10002"}}
            ]}""",
        )
        server.json(
            """{"actors":[
                {"type":"atlassian-group-role-actor","name":"jira-developers"},
                {"type":"atlassian-user-role-actor","actorUser":{"emailAddress":"developer@example.com"}}
            ]}""",
        )

        val access = loader().load(
            config(server, "\"project_key\":\"ENG\",\"include_permissions\":true"),
            tokenCredentials(),
            null,
            permissionSync = true,
        ).single().documents.single().externalAccess

        assertEquals(setOf("jira-developers"), assertNotNull(access).externalUserGroupIds)
        assertEquals(setOf("developer@example.com"), access.externalUserEmails)
        assertTrue(server.takeRequest().requestUrl!!.encodedPath.endsWith("/search"))
        server.takeRequest()
        assertEquals("/rest/api/2/project/ENG/role/10002", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun validationSucceedsWithProject() = MockWebServer().use { server ->
        server.json("""{"key":"ENG"}""")

        loader().validate(config(server, "\"project_key\":\"ENG\""), tokenCredentials())

        assertEquals("/rest/api/2/project/ENG", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun validationSucceedsWithoutProject() = MockWebServer().use { server ->
        server.json("[]")

        loader().validate(config(server, "\"batch_size\":50"), tokenCredentials())

        val request = server.takeRequest()
        assertEquals("/rest/api/2/project", request.requestUrl!!.encodedPath)
        assertEquals("1", request.requestUrl!!.queryParameter("maxResults"))
    }

    @Test
    fun scopedValidationUsesCloudIdAndAtlassianApiBase() = MockWebServer().use { server ->
        server.json("""{"cloudId":"cloud-123"}""")
        server.json("""{"key":"ENG"}""")
        val client = RemoteJsonClient(WebClient.builder().filter(rewriteAtlassianRequestsTo(server)))

        JiraConnectorLoader(client, mapper).validate(
            config(server, "\"project_key\":\"ENG\",\"scoped_token\":true"),
            cloudCredentials(),
        )

        assertEquals("/_edge/tenant_info", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/ex/jira/cloud-123/rest/api/3/project/ENG", server.takeRequest().requestUrl!!.encodedPath)
    }

    @Test
    fun customCloudJqlValidationUsesEnhancedSearch() = MockWebServer().use { server ->
        server.json("""{"issues":[]}""")

        loader().validate(
            config(server, "\"jql_query\":\"labels = customer\""),
            cloudCredentials(),
        )

        val request = server.takeRequest()
        assertEquals("/rest/api/3/search/jql", request.requestUrl!!.encodedPath)
        assertEquals("labels = customer", request.requestUrl!!.queryParameter("jql"))
    }

    @Test
    fun indexingMaps403ToPermissionError() = assertIndexingError(
        credentials = tokenCredentials(),
        status = 403,
        body = "{}",
        expected = "Insufficient permissions",
    )

    @Test
    fun cloudIndexingMapsMissingProjectToValidationError() = assertIndexingError(
        credentials = cloudCredentials(),
        status = 400,
        body = missingProjectError(),
        expected = "does not exist",
    )

    @Test
    fun serverIndexingMapsMissingProjectToValidationError() = assertIndexingError(
        credentials = tokenCredentials(),
        status = 400,
        body = missingProjectError(),
        expected = "does not exist",
    )

    @Test
    fun indexingMapsInvalidJqlToValidationError() = assertIndexingError(
        credentials = tokenCredentials(),
        status = 400,
        body = """{"errorMessages":["Error in the JQL Query"]}""",
        expected = "Invalid JQL",
    )

    @Test
    fun slimRetrievalSkipsPermissionResolution() = MockWebServer().use { server ->
        server.json(serverPage(total = 1, issue("ENG-1", description = "large", labels = listOf("secret"))))

        val document = loader().retrieveAllSlimDocuments(
            config(
                server,
                "\"project_key\":\"ENG\",\"include_permissions\":true," +
                    "\"labels_to_skip\":[\"secret\"],\"max_ticket_size_bytes\":1",
            ),
            tokenCredentials(),
        ).single().documents.single()

        assertEquals(server.url("/").toString().trimEnd('/') + "/browse/ENG-1", document.id)
        assertEquals("", document.content)
        assertEquals(null, document.externalAccess)
        assertEquals(1, server.requestCount)
    }

    @Test
    fun validationMaps401ToStableCredentialError() = assertValidationError(401, "expired or invalid")

    @Test
    fun validationMaps403ToStablePermissionError() = assertValidationError(403, "sufficient permissions")

    @Test
    fun validationMaps404ToStableUnexpectedError() = assertValidationError(404, "Unexpected Jira error")

    @Test
    fun validationMaps429ToStableRateLimitError() = assertValidationError(429, "rate-limits")

    @Test
    fun resumedCloudBulkFetchMaps401ToStableCredentialError() = MockWebServer().use { server ->
        server.start()
        server.enqueue(MockResponse().setResponseCode(401).setBody("{}"))
        val checkpoint = mapper.valueToTree<JsonNode>(
            JiraCheckpoint(allIssueIds = listOf(listOf("10001")), idsDone = true),
        )

        val error = assertFailsWith<IllegalArgumentException> {
            loader().load(
                config(server, "\"project_key\":\"ENG\""),
                cloudCredentials(),
                checkpoint,
            ).toList()
        }

        assertTrue(error.message.orEmpty().contains("expired or invalid"))
    }

    private fun assertIndexingError(credentials: JsonNode, status: Int, body: String, expected: String) =
        MockWebServer().use { server ->
            server.start()
            server.enqueue(MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body))

            val error = assertFailsWith<IllegalArgumentException> {
                loader().load(config(server, "\"project_key\":\"MISSING\""), credentials, null).toList()
            }

            assertTrue(error.message.orEmpty().contains(expected))
        }

    private fun assertValidationError(status: Int, expected: String) = MockWebServer().use { server ->
        server.start()
        server.enqueue(
            MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json")
                .setBody("""{"errorMessages":["upstream detail"]}"""),
        )

        val error = assertFailsWith<IllegalArgumentException> {
            loader().validate(config(server, "\"project_key\":\"ENG\""), tokenCredentials())
        }

        assertTrue(error.message.orEmpty().contains(expected))
    }

    private fun loader(): JiraConnectorLoader = JiraConnectorLoader(RemoteJsonClient(WebClient.builder()), mapper)

    private fun config(server: MockWebServer, extra: String) = mapper.readTree(
        """{"jira_base_url":"${server.startAndBase()}",$extra}""",
    )

    private fun tokenCredentials() = mapper.readTree("""{"jira_api_token":"token"}""")

    private fun cloudCredentials() = mapper.readTree(
        """{"jira_user_email":"user@example.com","jira_api_token":"token"}""",
    )

    private fun issue(
        key: String,
        id: String = key,
        description: String = "Description",
        labels: List<String> = emptyList(),
        extraFields: String = "",
    ): String = """{
        "id":"$id","key":"$key","fields":{
            "summary":"Summary $key",
            "description":{"type":"doc","content":[{"type":"text","text":"$description"}]},
            "updated":"2026-01-01T00:00:00.000+0000",
            "created":"2026-01-01T00:00:00.000+0000",
            "labels":${mapper.writeValueAsString(labels)},
            "project":{"key":"ENG","name":"Engineering"},
            "issuetype":{"name":"Story"},
            "priority":{"name":"High"},
            "status":{"name":"In Progress"},
            "resolution":{"name":"Fixed"},
            "reporter":{"displayName":"Reporter","emailAddress":"reporter@example.com"},
            "assignee":{"displayName":"Assignee","emailAddress":"assignee@example.com"}
            $extraFields
        }
    }"""

    private fun serverPage(total: Int, vararg issues: String): String =
        """{"total":$total,"issues":[${issues.joinToString(",")}]}"""

    private fun bulkPage(vararg issues: String): String =
        """{"issues":[${issues.joinToString(",")}]}"""

    private fun missingProjectError(): String =
        """{"errorMessages":["The value 'MISSING' does not exist for the field 'project'."]}"""

    private fun badBulkIssueDispatcher(ids: Set<String>): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            if (request.requestUrl!!.encodedPath.endsWith("/search/jql")) {
                return jsonResponse(mapper.writeValueAsString(mapOf("issues" to ids.map { mapOf("id" to it) })))
            }
            val requested = mapper.readTree(request.body.readUtf8()).path("issueIdsOrKeys").map(JsonNode::asText)
            if ("BAD" in requested) return jsonResponse("{")
            return jsonResponse(bulkPage(*requested.map { issue("ENG-$it", id = it) }.toTypedArray()))
        }
    }

    private fun jsonResponse(body: String): MockResponse =
        MockResponse().setHeader("Content-Type", "application/json").setBody(body)

    private fun MockWebServer.json(body: String) {
        startAndBase()
        enqueue(MockResponse().setHeader("Content-Type", "application/json").setBody(body))
    }

    private fun MockWebServer.startAndBase(): String {
        if (port == -1) start()
        return url("/").toString().trimEnd('/')
    }

    private fun rewriteAtlassianRequestsTo(server: MockWebServer): ExchangeFilterFunction = ExchangeFilterFunction.ofRequestProcessor { request ->
        if (request.url().host != "api.atlassian.com") {
            reactor.core.publisher.Mono.just(request)
        } else {
            val local = server.url(request.url().rawPath + (request.url().rawQuery?.let { "?$it" } ?: ""))
            reactor.core.publisher.Mono.just(ClientRequest.from(request).url(URI.create(local.toString())).build())
        }
    }
}
