package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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
                """{"total":1,"issues":[{"key":"DOC-1","fields":{"summary":"Jira summary","description":{"type":"doc","content":[{"type":"text","text":"Issue body"}]},"updated":"2026-01-01"}}]}""",
            ),
        )
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        val docs = loaders().load(
            "jira",
            mapper.readTree("""{"jira_base_url":"""" + base + """","project_key":"DOC"}"""),
            mapper.readTree("""{"email":"a@example.com","api_token":"token"}"""),
            null,
        )

        assertEquals(1, docs.size)
        assertEquals("DOC-1", docs.single().id)
        assertTrue(docs.single().content.contains("Issue body"))
        val request = server.takeRequest()
        assertTrue(request.path!!.startsWith("/rest/api/2/search?"))
        assertEquals(
            "Basic " + Base64.getEncoder().encodeToString("a@example.com:token".toByteArray()),
            request.getHeader("Authorization"),
        )
    }

    @Test
    fun collectsConfluencePagesWithPaginationFields() = MockWebServer().use { server ->
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"totalSize":1,"results":[{"id":"42","title":"Runbook","body":{"storage":{"value":"<p>Safe <b>content</b></p>"}},"_links":{"webui":"/pages/42"},"space":{"key":"ENG"},"version":{"when":"2026-01-01"}}]}""",
            ),
        )
        server.start()
        val base = server.url("/").toString().trimEnd('/')
        val docs = loaders().load(
            "confluence",
            mapper.readTree("""{"wiki_base":"""" + base + """","space":"ENG"}"""),
            mapper.readTree("""{"username":"a@example.com","api_token":"token"}"""),
            null,
        )

        assertEquals("42", docs.single().id)
        assertEquals("Safe content", docs.single().content)
        assertEquals("/rest/api/content/search", server.takeRequest().requestUrl!!.encodedPath)
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
            "github",
            mapper.readTree(
                """{"github_base_url":"""" + base + """","repo_owner":"onyx","repositories":"foss","include_prs":true,"include_issues":false,"include_files":false}""",
            ),
            mapper.readTree("""{"github_access_token":"token"}"""),
            null,
        )

        assertEquals(1, docs.size)
        assertEquals("onyx/foss/pull_request/7", docs.single().id)
        assertEquals("Bearer token", server.takeRequest().getHeader("Authorization"))
    }

    private fun loaders(): RemoteConnectorLoaders =
        RemoteConnectorLoaders(RemoteJsonClient(WebClient.builder()))
}
