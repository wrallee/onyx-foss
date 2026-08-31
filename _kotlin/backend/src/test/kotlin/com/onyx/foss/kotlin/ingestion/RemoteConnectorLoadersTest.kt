package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.domain.ConnectorSource
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient
import java.util.Base64
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RemoteConnectorLoadersTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun collectsJiraIssuesWithBasicAuthentication() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"key":"DOC"}""",
            ),
        )
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"issues":[{"id":"10001"}]}""",
            ),
        )
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"issues":[{"id":"10001","key":"DOC-1","fields":{"summary":"Jira summary","description":{"type":"doc","content":[{"type":"text","text":"Issue body"}]},"updated":"2026-01-01"}}]}""",
            ),
        )
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        val docs = loaders().load(
            ConnectorSource.JIRA,
            mapper.readTree("""{"jira_base_url":"""" + base + """","project_key":"DOC"}"""),
            mapper.readTree("""{"email":"a@example.com","api_token":"token"}"""),
            null,
        ).single().documents

        assertEquals(1, docs.size)
        assertEquals(base + "/browse/DOC-1", docs.single().id)
        assertTrue(docs.single().content.contains("Issue body"))
        val validationRequest = server.takeRequest()
        assertEquals("/rest/api/3/project/DOC", validationRequest.requestUrl!!.encodedPath)
        assertEquals(
            "Basic " + Base64.getEncoder().encodeToString("a@example.com:token".toByteArray()),
            validationRequest.getHeader("Authorization"),
        )
        assertTrue(server.takeRequest().path!!.startsWith("/rest/api/3/search/jql?"))
    }

    @Test
    fun collectsConfluencePagesWithPaginationFields() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"results":[{"key":"ENG"}]}""",
            ),
        )
        server.enqueue(MockResponse().setHeader("Content-Type", "application/json").setBody("""{"key":"ENG"}"""))
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"totalSize":1,"results":[{"id":"42","title":"Runbook","body":{"storage":{"value":"<p>Safe <b>content</b></p>"}},"_links":{"webui":"/pages/42"},"space":{"key":"ENG"},"version":{"when":"2026-01-01T00:00:00Z"}}]}""",
            ),
        )
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        val docs = loaders().load(
            ConnectorSource.CONFLUENCE,
            mapper.readTree("""{"wiki_base":"""" + base + """","space":"ENG","include_comments":false,"include_attachments":false}"""),
            mapper.readTree("""{"confluence_username":"a@example.com","confluence_access_token":"token"}"""),
            null,
        ).single().documents

        assertEquals(base + "/pages/42", docs.single().id)
        assertEquals("Safe content", docs.single().content)
        val validation = server.takeRequest()
        assertEquals("/rest/api/space", validation.requestUrl!!.encodedPath)
        assertEquals("Bearer token", validation.getHeader("Authorization"))
        val request = server.takeRequest()
        assertEquals("/rest/api/space/ENG", request.requestUrl!!.encodedPath)
        val search = server.takeRequest()
        assertEquals("/rest/api/content/search", search.requestUrl!!.encodedPath)
        assertTrue(search.requestUrl!!.queryParameter("cql")!!.startsWith("type=page and space='ENG'"))
        assertEquals("Bearer token", search.getHeader("Authorization"))
    }

    @Test
    fun collectsGithubPullRequestsWithBearerAuthentication() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """[{"number":7,"title":"Improve docs","body":"Pull request body","html_url":"https://github.test/pr/7","updated_at":"2026-01-01"}]""",
            ),
        )
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        val docs = loaders().load(
            ConnectorSource.GITHUB,
            mapper.readTree(
                """{"github_base_url":"""" + base + """","repo_owner":"onyx","repositories":"foss","include_prs":true,"include_issues":false,"include_files":false}""",
            ),
            mapper.readTree("""{"github_access_token":"token"}"""),
            null,
        ).single().documents

        assertEquals(1, docs.size)
        assertEquals("onyx/foss/pull_request/7", docs.single().id)
        assertEquals("Bearer token", server.takeRequest().getHeader("Authorization"))
    }

    @Test
    fun acceptsConfluenceResponsesLargerThanTheWebClientDefaultBuffer() = MockWebServer().use { server ->
        val content = "a".repeat(300_000)
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"results":[{"key":"ENG"}]}""",
            ),
        )
        server.enqueue(MockResponse().setHeader("Content-Type", "application/json").setBody("""{"key":"ENG"}"""))
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                mapper.writeValueAsString(
                    mapOf(
                        "totalSize" to 1,
                        "results" to listOf(
                            mapOf(
                                "id" to "42",
                                "title" to "Large page",
                                "body" to mapOf("storage" to mapOf("value" to content)),
                                "_links" to mapOf("webui" to "/pages/42"),
                                "version" to mapOf("when" to "2026-01-01T00:00:00Z"),
                            ),
                        ),
                    ),
                ),
            ),
        )
        server.start()

        val docs = loaders().load(
            ConnectorSource.CONFLUENCE,
            mapper.readTree(
                """{"wiki_base":"${server.url("/").toString().trimEnd('/')}","space":"ENG","include_comments":false,"include_attachments":false}""",
            ),
            mapper.readTree("""{"confluence_access_token":"token"}"""),
            null,
        ).single().documents

        assertEquals(content.length, docs.single().content.length)
    }

    private fun loaders(): RemoteConnectorLoaders =
        RemoteJsonClient(WebClient.builder()).let { http ->
            RemoteConnectorLoaders(
                http,
                JiraConnectorLoader(http, mapper),
                ConfluenceConnectorLoader(http, mapper).also { it.sleepMillis = {} },
            )
        }
}
