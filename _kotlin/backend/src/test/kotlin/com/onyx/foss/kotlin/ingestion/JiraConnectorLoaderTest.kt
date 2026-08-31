package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.onyx.foss.kotlin.domain.ConnectorSource
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ExchangeFilterFunction
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI
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

        loader().load(config(server, "\"project_key\":\"ORDER\""), tokenCredentials(), null).single()

        assertEquals("project = \"ORDER\"", server.takeRequest().requestUrl!!.queryParameter("jql"))
    }

    @Test
    fun customJqlOverridesProjectJql() = MockWebServer().use { server ->
        server.json("""{"total":0,"issues":[]}""")

        loader().load(
            config(server, "\"project_key\":\"ENG\",\"jql_query\":\"labels = customer\""),
            tokenCredentials(),
            null,
        ).single()

        assertEquals("labels = customer", server.takeRequest().requestUrl!!.queryParameter("jql"))
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
            extraFields = ""","comment":{"comments":[
                {"body":"Visible","author":{"emailAddress":"person@example.com"}},
                {"body":"Hidden","author":{"emailAddress":"bot@example.com"}}
            ]}""",
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

        assertEquals(listOf("ENG-1"), batch.documents.map { it.metadata["key"] })
        assertTrue(batch.documents.single().content.contains("Issue body"))
        assertTrue(batch.documents.single().content.contains("Comment: Visible"))
        assertFalse(batch.documents.single().content.contains("Hidden"))
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
    }

    @Test
    fun oneBadIssueBecomesFailureWhileGoodIssueSurvives() = MockWebServer().use { server ->
        server.json(serverPage(total = 2, issue("ENG-1"), """{"key":"ENG-2","fields":null}"""))

        val batch = loader().load(config(server, "\"project_key\":\"ENG\""), tokenCredentials(), null).single()

        assertEquals(listOf("ENG-1"), batch.documents.map { it.metadata["key"] })
        assertEquals("ENG-2", (batch.failures.single().target as FailureTarget.Document).id)
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
            "issuetype":{"name":"Story"}
            $extraFields
        }
    }"""

    private fun serverPage(total: Int, vararg issues: String): String =
        """{"total":$total,"issues":[${issues.joinToString(",")}]}"""

    private fun bulkPage(vararg issues: String): String =
        """{"issues":[${issues.joinToString(",")}]}"""

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
