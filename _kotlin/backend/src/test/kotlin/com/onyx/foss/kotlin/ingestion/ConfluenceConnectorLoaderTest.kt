package com.onyx.foss.kotlin.ingestion

import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.kotlin.jacksonObjectMapper
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
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConfluenceConnectorLoaderTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun confluenceConnectorSkipImages() = MockWebServer().use { server ->
        server.dispatcher = attachmentDispatcher(
            pageResponse(),
            listOf(imageAttachment(), pdfAttachment()),
            server,
        )

        val documents = loader().load(
            config(server, "\"allow_images\":false,\"include_comments\":false"),
            credentials(),
            null,
        ).flatMap { it.documents.asSequence() }.toList()

        assertEquals(listOf("Runbook", "spec.pdf"), documents.map { it.title })
        assertFalse(documents.any { it.title == "diagram.png" })
    }

    @Test
    fun confluenceConnectorAllowImages() = MockWebServer().use { server ->
        server.dispatcher = attachmentDispatcher(
            pageResponse(),
            listOf(imageAttachment(), pdfAttachment()),
            server,
        )

        val documents = loader().load(
            config(server, "\"allow_images\":true,\"include_comments\":false"),
            credentials(),
            null,
        ).flatMap { it.documents.asSequence() }.toList()

        val image = documents.single { it.title == "diagram.png" }
        assertEquals("", image.content)
        assertEquals(true, image.metadata["image"])
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun confluenceConnectorBasic() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dcomment") -> json(
                    """{"results":[{"body":{"storage":{"value":"<p>Useful comment</p>"}}}]}""",
                )
                request.path!!.contains("type%3Dattachment") -> json(
                    mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))),
                )
                request.path!!.contains("/download/") -> MockResponse().setBody("attachment text")
                else -> json(pageResponse("<p>Run <b>safely</b></p>"))
            }
        }

        val documents = loader().load(config(server), credentials(), null)
            .flatMap { it.documents.asSequence() }.toList()

        val page = documents.single { it.title == "Runbook" }
        assertEquals("Run safely\nComment:\nUseful comment", page.content)
        assertEquals("ENG", page.metadata["space"])
        assertEquals(listOf("ops"), page.metadata["labels"])
        assertEquals(Instant.parse("2026-01-02T00:00:00Z"), page.updatedAt)
        assertEquals(listOf("owner@example.com"), page.primaryOwners)
        val attachment = documents.single { it.title == "spec.pdf" }
        assertEquals("attachment text", attachment.content)
        assertEquals(page.id, attachment.metadata["parent_page_id"])
    }

    @Test
    fun confluenceConnectorBasicScoped() = MockWebServer().use { server ->
        server.enqueue(json("""{"cloudId":"cloud-1"}"""))
        server.enqueue(json(pageResponse()))
        server.enqueue(json("""{"results":[]}"""))
        val http = RemoteJsonClient(WebClient.builder().filter(rewriteAtlassianRequestsTo(server)))

        val document = ConfluenceConnectorLoader(http, mapper).load(
            config(server, "\"scoped_token\":true,\"is_cloud\":true,\"include_comments\":false"),
            credentials(),
            null,
        ).single().documents.single()

        assertEquals("/_edge/tenant_info", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/ex/confluence/cloud-1/rest/api/content/search", server.takeRequest().requestUrl!!.encodedPath)
        assertTrue(document.id.startsWith(server.url("/").toString().trimEnd('/')))
    }

    @Test
    fun confluenceHtmlLinkScope() {
        val text = loader().parseHtml(
            """<p>LINK_BEFORE <a href="https://example.com/target">LINK_TARGET_ONLY</a> LINK_AFTER</p><p>NEXT</p>""",
        )

        assertEquals("LINK_BEFORE [LINK_TARGET_ONLY](https://example.com/target) LINK_AFTER\nNEXT", text)
    }

    @Test
    fun confluenceHtmlTableScope() {
        val text = loader().parseHtml(
            """<p>TABLE_BEFORE</p><table><tr><th>HEADER_ALPHA</th><th>HEADER_BETA</th></tr><tr><td>CELL_ALPHA</td><td>CELL_BETA_LINE_1<br>CELL_BETA_LINE_2</td></tr></table><h2>TABLE_AFTER_HEADING</h2><p>TABLE_AFTER_PARAGRAPH_MUST_BE_SEPARATE</p><ul><li>TABLE_AFTER_LIST_ONE</li><li>TABLE_AFTER_LIST_TWO</li></ul>""",
        )

        assertEquals(
            "TABLE_BEFORE\n\tHEADER_ALPHA\tHEADER_BETA\n\tCELL_ALPHA\tCELL_BETA_LINE_1 CELL_BETA_LINE_2\n" +
                "TABLE_AFTER_HEADING\nTABLE_AFTER_PARAGRAPH_MUST_BE_SEPARATE\n" +
                "- TABLE_AFTER_LIST_ONE\n- TABLE_AFTER_LIST_TWO",
            text,
        )
    }

    @Test
    fun reindexSinglePage() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(setOf("111"), server)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false,\"include_attachments\":false"), credentials(), listOf(failure(url))).single()

        assertEquals(listOf(url), batch.documents.map { it.id })
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun reindexMultiplePages() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(setOf("111", "222"), server)
        val urls = listOf(
            server.url("spaces/ENG/pages/111/Runbook").toString(),
            server.url("spaces/ENG/pages/222/Runbook").toString(),
        )

        val batch = loader().reindex(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            urls.map(::failure),
        ).single()

        assertEquals(urls.toSet(), batch.documents.map { it.id }.toSet())
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun reindexUnknownPageYieldsFailure() = MockWebServer().use { server ->
        server.dispatcher = reindexDispatcher(emptySet(), server)
        val url = server.url("spaces/ENG/pages/999/Missing").toString()

        val batch = loader().reindex(config(server), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.isEmpty())
        assertEquals(url, (batch.failures.single().target as FailureTarget.Document).id)
    }

    @Test
    fun reindexUnparseableUrlYieldsFailure() {
        val url = "https://example.com/wiki/display/SPACE/Page"
        val batch = loader().reindex(config("https://example.com"), credentials(), listOf(failure(url))).single()

        assertEquals(url, (batch.failures.single().target as FailureTarget.Document).id)
        assertContains(batch.failures.single().message, "Cannot extract page id")
    }

    @Test
    fun reindexEmptyErrors() {
        assertTrue(loader().reindex(config("https://example.com"), credentials(), emptyList()).none())
    }

    @Test
    fun reindexEntityFailuresAreSkipped() {
        val failures = listOf(ConnectorFailure(FailureTarget.Entity("stage"), "failed"))
        assertTrue(loader().reindex(config("https://example.com"), credentials(), failures).none())
    }

    @Test
    fun paginationRejectsCrossOriginNextWithoutSendingAuthorization() = MockWebServer().use { trusted ->
        MockWebServer().use { malicious ->
            malicious.enqueue(json("""{"results":[]}"""))
            trusted.enqueue(
                json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapper.readTree(page("111"))),
                            "_links" to mapOf("next" to malicious.url("steal").toString()),
                        ),
                    ),
                ),
            )

            val error = assertFailsWith<IllegalArgumentException> {
                loader().load(
                    config(trusted, "\"include_comments\":false,\"include_attachments\":false"),
                    credentials(),
                    null,
                ).toList()
            }

            assertContains(error.message.orEmpty(), "origin")
            assertEquals(0, malicious.requestCount)
        }
    }

    @Test
    fun paginationAcceptsConfiguredOriginAbsoluteNext() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("111"))),
                        "_links" to mapOf(
                            "next" to server.url("rest/api/content/search?cql=type%3Dpage&start=1&limit=50").toString(),
                        ),
                    ),
                ),
            ),
        )
        server.enqueue(json("""{"results":[]}"""))

        val batches = loader().load(
            config(server, "\"is_cloud\":true,\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            null,
        ).toList()

        assertEquals(2, batches.size)
        assertEquals("Basic ${Base64.getEncoder().encodeToString("user@example.com:token".toByteArray())}", server.takeRequest().getHeader("Authorization"))
        assertEquals("1", server.takeRequest().requestUrl!!.queryParameter("start"))
    }

    @Test
    fun attachmentDownloadRejectsCrossOriginUrlWithoutSendingAuthorization() = MockWebServer().use { trusted ->
        MockWebServer().use { malicious ->
            malicious.enqueue(MockResponse().setBody("stolen"))
            val attachment = pdfAttachment().toMutableMap().also { value ->
                value["_links"] = mapOf(
                    "download" to malicious.url("steal").toString(),
                    "webui" to "/pages/viewpageattachments.action?pageId=222&preview=att-222",
                )
            }
            trusted.dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse =
                    if (request.path!!.contains("type%3Dattachment")) {
                        json(mapper.writeValueAsString(mapOf("results" to listOf(attachment))))
                    } else {
                        json(pageResponse())
                    }
            }

            val batch = loader().load(
                config(trusted, "\"include_comments\":false"),
                credentials(),
                null,
            ).single()

            assertEquals(0, malicious.requestCount)
            assertEquals("confluence_attachment_processing", batch.failures.single().errorType)
            assertFalse(batch.documents.any { it.title == "spec.pdf" })
        }
    }

    @Test
    fun `400KeepsPartialAttachmentsAndRecordsFailure`() = MockWebServer().use { server ->
        var attachmentCalls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") && attachmentCalls++ == 0 -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(pdfAttachment()),
                            "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50"),
                        ),
                    ),
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(400).setBody("bad offset")
                request.path!!.contains("/download/") -> MockResponse().setBody("partial text")
                else -> json(pageResponse())
            }
        }

        val batch = loader().load(
            config(server, "\"include_comments\":false"),
            credentials(),
            null,
        ).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" && it.content == "partial text" })
        assertEquals("confluence_attachment_pagination", batch.failures.single().errorType)
        assertTrue((batch.failures.single().target as FailureTarget.Document).id.contains("/pages/111/"))
    }

    @Test
    fun `401403SkipsPageAttachments`() {
        listOf(401, 403).forEach { status ->
            MockWebServer().use { server ->
                server.enqueue(json(pageResponse()))
                server.enqueue(MockResponse().setResponseCode(status).setBody("denied"))

                val batch = loader().load(
                    config(server, "\"include_comments\":false"),
                    credentials(),
                    null,
                ).single()

                assertEquals(listOf("Runbook"), batch.documents.map { it.title })
                assertContains(batch.failures.single().message, status.toString())
            }
        }
    }

    @Test
    fun otherHttpErrorsPropagate(): Unit = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse =
                if (request.path!!.contains("type%3Dattachment")) MockResponse().setResponseCode(500) else json(pageResponse())
        }

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server, "\"include_comments\":false"), credentials(), null).toList()
        }
    }

    @Test
    fun `400DateErrorPropagates`(): Unit = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        repeat(2) {
            server.enqueue(MockResponse().setResponseCode(400).setBody("The field 'updated' is invalid"))
        }

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server, "\"include_comments\":false"), credentials(), null).toList()
        }
    }

    @Test
    fun reindexRefetchesAttachments() = MockWebServer().use { server ->
        server.dispatcher = reindexWithAttachmentDispatcher(server, attachmentStatus = 200)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false"), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" })
        assertTrue(batch.failures.isEmpty())
    }

    @Test
    fun slimAttachment400RetriesThenSucceeds() = MockWebServer().use { server ->
        var calls = 0
        server.dispatcher = slimAttachmentDispatcher(server) { if (calls++ < 2) 400 else 200 }

        val documents = loader().retrieveAllSlimDocuments(config(server), credentials())
            .flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "spec.pdf" })
        assertEquals(3, calls)
    }

    @Test
    fun slimAttachmentPersistent400Raises(): Unit = MockWebServer().use { server ->
        server.dispatcher = slimAttachmentDispatcher(server) { 400 }

        assertFailsWith<WebClientResponseException> {
            loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
        }
    }

    @Test
    fun slimAttachmentOtherErrorsRaiseImmediately() {
        listOf(403 to "denied", 400 to "The field 'updated' is invalid").forEach { (status, body) ->
            MockWebServer().use { server ->
                var calls = 0
                server.dispatcher = object : Dispatcher() {
                    override fun dispatch(request: RecordedRequest): MockResponse =
                        if (request.path!!.contains("type%3Dattachment")) {
                            calls += 1
                            MockResponse().setResponseCode(status).setBody(body)
                        } else {
                            json(pageResponse())
                        }
                }
                assertFailsWith<WebClientResponseException> {
                    loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
                }
                assertEquals(1, calls)
            }
        }

        MockWebServer().use { server ->
            var calls = 0
            server.dispatcher = slimAttachmentDispatcher(server) { if (calls++ < 20) 500 else 200 }
            assertFailsWith<WebClientResponseException> {
                loader().retrieveAllSlimDocuments(config(server), credentials()).toList()
            }
        }
    }

    @Test
    fun reindexRerecordsAttachmentFailure() = MockWebServer().use { server ->
        server.dispatcher = reindexWithAttachmentDispatcher(server, attachmentStatus = 400)
        val url = server.url("spaces/ENG/pages/111/Runbook").toString()

        val batch = loader().reindex(config(server, "\"include_comments\":false"), credentials(), listOf(failure(url))).single()

        assertTrue(batch.documents.any { it.title == "spec.pdf" })
        assertEquals("confluence_attachment_pagination", batch.failures.single().errorType)
    }

    @Test
    fun attachmentPaginationRejectsRepeatedNextLink() = MockWebServer().use { server ->
        var repeatedCalls = 0
        val repeatedPath = "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50"
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") && !request.path!!.contains("start=1") -> json(
                    """{"results":[{"title":"ignored.bin"}],"_links":{"next":"$repeatedPath"}}""",
                )
                request.path!!.contains("type%3Dattachment") && repeatedCalls++ < 2 -> json(
                    """{"results":[{"title":"ignored.bin"}],"_links":{"next":"$repeatedPath"}}""",
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(418)
                else -> json(pageResponse())
            }
        }

        val error = assertFailsWith<IllegalArgumentException> {
            loader().load(
                config(server, "\"is_cloud\":true,\"include_comments\":false"),
                credentials(),
                null,
            ).toList()
        }

        assertContains(error.message.orEmpty(), "cycle")
        assertTrue(repeatedCalls <= 1)
    }

    @Test
    fun attachmentPaginationRejectsMoreThanOneHundredThousandResults() = MockWebServer().use { server ->
        val attachments = buildString {
            append("{\"results\":[")
            repeat(100_001) { index ->
                if (index > 0) append(',')
                append("{\"title\":\"ignored.bin\"}")
            }
            append("]}")
        }
        server.enqueue(json(pageResponse()))
        server.enqueue(json(attachments))

        val error = assertFailsWith<IllegalArgumentException> {
            loader().load(
                config(server, "\"include_comments\":false"),
                credentials(),
                null,
            ).toList()
        }

        assertContains(error.message.orEmpty(), "100000")
    }

    @Test
    fun pruningExpandSkipsRestrictionsButKeepsHierarchy() = MockWebServer().use { server ->
        server.enqueue(json("""{"results":[]}"""))

        loader().retrieveAllSlimDocuments(config(server), credentials()).toList()

        val expand = server.takeRequest().requestUrl!!.queryParameter("expand").orEmpty()
        assertContains(expand, "space")
        assertContains(expand, "ancestors")
        assertFalse(expand.contains("restrictions.read.restrictions"))
    }

    @Test
    fun attachmentSectionLinkUsesPlatformSpecificUrl() {
        listOf(false, true).forEach { isCloud ->
            MockWebServer().use { server ->
                server.dispatcher = attachmentDispatcher(pageResponse(), listOf(pdfAttachment()), server)
                val attachment = loader().load(
                    config(server, "\"is_cloud\":$isCloud,\"include_comments\":false"),
                    credentials(),
                    null,
                ).single().documents.single { it.title == "spec.pdf" }

                val expected = if (isCloud) {
                    server.url("wiki/download/attachments/111/spec.pdf").toString()
                } else {
                    server.url("download/attachments/222/spec.pdf").toString()
                }
                assertEquals(expected, attachment.link)
            }
        }
    }

    @Test
    fun attachmentFailureUsesAttachmentDocumentId() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") -> json(
                    mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))),
                )
                request.path!!.contains("/download/") -> MockResponse().setResponseCode(500).setBody("failed")
                else -> json(pageResponse())
            }
        }

        val failure = loader().load(
            config(server, "\"include_comments\":false"),
            credentials(),
            null,
        ).single().failures.single()

        val target = failure.target as FailureTarget.Document
        assertEquals(server.url("pages/viewpageattachments.action?pageId=222&preview=att-222").toString(), target.id)
        assertEquals(server.url("download/attachments/222/spec.pdf").toString(), target.link)
    }

    @Test
    fun getCqlQueryWithSpace() {
        val query = loader().constructPageCql(
            mapper.readTree("""{"space":"TEST","timezone_offset":0}"""),
            Instant.parse("2023-01-01T00:00:00Z"),
            Instant.parse("2023-01-02T00:00:00Z"),
        )

        assertContains(query, "space='TEST'")
        assertContains(query, "lastmodified >= '2023-01-01 00:00'")
        assertContains(query, "lastmodified <= '2023-01-02 00:00'")
    }

    @Test
    fun getCqlQueryWithoutSpace() {
        val query = loader().constructPageCql(
            mapper.readTree("""{"timezone_offset":0}"""),
            Instant.parse("2023-01-01T00:00:00Z"),
            Instant.parse("2023-01-02T00:00:00Z"),
        )

        assertFalse(query.contains("space="))
        assertContains(query, "lastmodified >= '2023-01-01 00:00'")
        assertContains(query, "lastmodified <= '2023-01-02 00:00'")
    }

    @Test
    fun loadFromCheckpointHappyPath() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("1")), mapper.readTree(page("2"))),
                        "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&start=2&limit=2"),
                    ),
                ),
            ),
        )
        repeat(4) { server.enqueue(json("""{"results":[]}""")) }
        server.enqueue(json(mapper.writeValueAsString(mapOf("results" to listOf(mapper.readTree(page("3")))))))
        repeat(2) { server.enqueue(json("""{"results":[]}""")) }

        val batches = loader().load(
            config(server, "\"batch_size\":2"),
            credentials(),
            null,
        ).toList()

        assertEquals(listOf(2, 1), batches.map { it.documents.size })
        assertEquals(
            "/rest/api/content/search?cql=type%3Dpage&start=2&limit=2",
            mapper.treeToValue(batches[0].checkpoint.value, ConfluenceCheckpoint::class.java).nextPageUrl,
        )
        assertFalse(mapper.treeToValue(batches[1].checkpoint.value, ConfluenceCheckpoint::class.java).hasMore)
    }

    @Test
    fun loadFromCheckpointWithPageProcessingError() = MockWebServer().use { server ->
        val bad = (mapper.readTree(page("2")).deepCopy() as ObjectNode).also { it.remove("version") }
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf("results" to listOf(mapper.readTree(page("1")), bad)),
                ),
            ),
        )
        repeat(4) { server.enqueue(json("""{"results":[]}""")) }

        val batch = loader().load(config(server), credentials(), null).single()

        assertEquals(listOf("1"), batch.documents.map { it.metadata["confluence_page_id"] })
        assertEquals(
            server.url("/").toString().trimEnd('/') + "/spaces/ENG/pages/2/Runbook",
            (batch.failures.single().target as FailureTarget.Document).id,
        )
    }

    @Test
    fun validateConnectorSettingsErrors() {
        listOf(401 to "expired", 403 to "permissions", 404 to "Unexpected Confluence error").forEach { (status, message) ->
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(status).setBody("upstream"))
                val error = assertFailsWith<IllegalArgumentException> {
                    loader().validate(config(server, "\"is_cloud\":false"), credentials())
                }
                assertTrue(error.message.orEmpty().contains(message, ignoreCase = true))
            }
        }
    }

    @Test
    fun validateConnectorSettingsSuccess() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
                "/rest/api/space/TEST" -> json("""{"key":"TEST"}""")
                else -> if (request.requestUrl!!.queryParameter("start") == "0") json(
                    """{"results":[{"key":"TEST"}],"_links":{"next":"/rest/api/space?limit=1&start=1"}}""",
                ) else json("""{"results":[]}""")
            }
        }

        loader().validate(config(server, "\"is_cloud\":false,\"space\":\"TEST\""), credentials())

        assertEquals("/rest/api/space", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals("/rest/api/space/TEST", server.takeRequest().requestUrl!!.encodedPath)
        assertEquals(2, server.requestCount)
    }

    @Test
    fun checkpointProgress() = MockWebServer().use { server ->
        server.enqueue(
            json(
                mapper.writeValueAsString(
                    mapOf(
                        "results" to listOf(mapper.readTree(page("1"))),
                        "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&start=1&limit=1"),
                    ),
                ),
            ),
        )
        repeat(2) { server.enqueue(json("""{"results":[]}""")) }
        val iterator = loader().load(config(server, "\"batch_size\":1"), credentials(), null).iterator()
        val first = iterator.next()
        val checkpoint = mapper.treeToValue(first.checkpoint.value, ConfluenceCheckpoint::class.java)

        assertTrue(checkpoint.hasMore)
        assertContains(assertNotNull(checkpoint.nextPageUrl), "start=1")
    }

    @Test
    fun completedCheckpointRestartsForNewPollWindow() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        val completed = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(hasMore = false, nextPageUrl = "/must-not-resume?start=99"),
        )

        loader().load(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            completed,
            start = Instant.parse("2026-01-01T00:00:00Z"),
            end = Instant.parse("2026-01-02T00:00:00Z"),
        ).single()

        val request = server.takeRequest()
        assertEquals("/rest/api/content/search", request.requestUrl!!.encodedPath)
        val cql = request.requestUrl!!.queryParameter("cql").orEmpty()
        assertContains(cql, "lastmodified >= '2026-01-01 00:00'")
        assertContains(cql, "lastmodified <= '2026-01-02 00:00'")
        assertFalse(request.path!!.contains("must-not-resume"))
    }

    @Test
    fun inProgressCheckpointResumesNextPageUrl() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))
        val active = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(
                hasMore = true,
                nextPageUrl = "/rest/api/content/search?cql=type%3Dpage&start=7&limit=2",
            ),
        )

        loader().load(
            config(server, "\"include_comments\":false,\"include_attachments\":false"),
            credentials(),
            active,
            start = Instant.parse("2026-01-01T00:00:00Z"),
            end = Instant.parse("2026-01-02T00:00:00Z"),
        ).single()

        val request = server.takeRequest()
        assertEquals("7", request.requestUrl!!.queryParameter("start"))
        assertEquals("type=page", request.requestUrl!!.queryParameter("cql"))
    }

    @Test
    fun displayNameCacheIsInstanceIsolated() = MockWebServer().use { server ->
        val loader = loader()
        val taggedPages = listOf("1", "2").map { id ->
            mapper.readTree(page(id, """<p><ri:user ri:userkey="user-1"/></p>"""))
        }
        val calls = mutableMapOf<String, Int>()
        server.dispatcher = userResolutionDispatcher(taggedPages) { request ->
            val authorization = request.getHeader("Authorization").orEmpty()
            calls[authorization] = calls.getOrDefault(authorization, 0) + 1
            json("""{"displayName":"${if (authorization.endsWith("token-a")) "Alice A" else "Bob B"}"}""")
        }

        val first = loadWithoutExtras(loader, server, "token-a")
        val second = loadWithoutExtras(loader, server, "token-b")

        assertTrue(first.all { it.content == "@Alice A" })
        assertTrue(second.all { it.content == "@Bob B" })
        assertEquals(mapOf("Bearer token-a" to 1, "Bearer token-b" to 1), calls)
    }

    @Test
    fun dateLozengeTextIsPreserved() {
        val text = loader().parseHtml("""<p>Meeting on <span class="date-lozenger-container">April 22, 2026</span> at noon.</p>""")
        assertContains(text, "April 22, 2026")
    }

    @Test
    fun pageWithoutDateLozengeUnaffected() {
        assertEquals("No dates here.", loader().parseHtml("<p>No dates here.</p>"))
    }

    @Test
    fun slimDocsIncludeAttachmentsByDefault() = MockWebServer().use { server ->
        server.dispatcher = slimAttachmentDispatcher(server) { 200 }

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":true"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun mainPassSkipsAttachmentFetchWhenDisabled() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))

        val documents = loader().load(
            config(server, "\"include_attachments\":false,\"include_comments\":false"),
            credentials(),
            null,
        ).single().documents

        assertEquals(listOf("Runbook"), documents.map { it.title })
        assertEquals(1, server.requestCount)
    }

    @Test
    fun slimDocsSkipAttachmentsWhenDisabled() = MockWebServer().use { server ->
        server.enqueue(json(pageResponse()))

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"include_attachments\":false"),
            credentials(),
        ).single().documents

        assertEquals(listOf("Runbook"), documents.map { it.title })
        assertEquals(1, server.requestCount)
    }

    @Test
    fun paginateUrlServerReDerivesStartWhenDcUnderCounts() = MockWebServer().use { server ->
        val ids = (1..8).toList()
        val starts = mutableListOf<Int>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val start = request.requestUrl!!.queryParameter("start")?.toInt() ?: 0
                starts += start
                val results = ids.drop(start).take(3).map { mapOf("id" to it) }
                val links = if (start + results.size < ids.size) {
                    mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=10&start=${start + 10}")
                } else {
                    emptyMap()
                }
                return json(mapper.writeValueAsString(mapOf("results" to results, "_links" to links)))
            }
        }

        val results = loader().paginate(config(server, "\"is_cloud\":false"), credentials(), "/rest/api/content/search?cql=type%3Dpage", 10)

        assertEquals(ids, results.map { it.path("id").asInt() })
        assertEquals(listOf(0, 3, 6), starts)
    }

    @Test
    fun paginatedCqlRetrievalHandlesPaginationError() = MockWebServer().use { server ->
        server.dispatcher = recoverablePaginationDispatcher(failAllRecoveryItems = false)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            3,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3, 4, 6, 7), ids)
    }

    @Test
    fun paginatedCqlRetrievalSkipsCompletelyFailingPage() = MockWebServer().use { server ->
        server.dispatcher = recoverablePaginationDispatcher(failAllRecoveryItems = true)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            3,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3, 7), ids)
    }

    @Test
    fun paginatedCqlRetrievalCloudReducesLimitOnError() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
                val start = request.requestUrl!!.queryParameter("start")
                return if (limit == 10 && start == null) {
                    json(
                        mapper.writeValueAsString(
                            mapOf(
                                "results" to (1..10).map { mapOf("id" to it) },
                                "_links" to mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=10&start=10"),
                            ),
                        ),
                    )
                } else {
                    MockResponse().setResponseCode(500)
                }
            }
        }

        assertFailsWith<WebClientResponseException> {
            loader().paginate(config(server, "\"is_cloud\":true"), credentials(), "/rest/api/content/search?cql=type%3Dpage", 10)
        }
        assertEquals(3, server.requestCount)
    }

    @Test
    fun paginateUrlReducesLimitOn504Cloud() = MockWebServer().use { server ->
        server.dispatcher = reducedLimitSuccessDispatcher(504)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":true"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            20,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2, 3), ids)
        assertEquals(3, server.requestCount)
    }

    @Test
    fun cqlPaginateAllExpansionsHandlesInternalPaginationError() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val path = request.path!!
                return when {
                    path.contains("content/search") -> json(
                        """{"results":[{"id":1,"children":{"results":[],"_links":{"next":"/rest/api/content/1/child?limit=3"}}}]}""",
                    )
                    path.contains("limit=3") && !path.contains("start=") -> MockResponse().setResponseCode(500)
                    path.contains("start=0") -> json("""{"results":[{"id":101}]}""")
                    path.contains("start=1") -> MockResponse().setResponseCode(500)
                    path.contains("start=2") -> json("""{"results":[{"id":103}]}""")
                    else -> json("""{"results":[]}""")
                }
            }
        }

        val item = loader().cqlPaginateAllExpansions(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "type=page",
            "children",
            3,
        ).single()

        assertEquals(listOf(101, 103), item.path("children").path("results").toList().map{ it.path("id").asInt() })
    }

    @Test
    fun paginateUrlReducesLimitOn500Server() = MockWebServer().use { server ->
        server.dispatcher = reducedLimitSuccessDispatcher(500, includeNext = false)

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            20,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2), ids)
        assertEquals(2, server.requestCount)
    }

    @Test
    fun paginateUrlServerFallsBackToOneByOneAfterLimitFloor() = MockWebServer().use { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val start = request.requestUrl!!.queryParameter("start")
                val limit = request.requestUrl!!.queryParameter("limit")
                return when {
                    limit == "5" -> MockResponse().setResponseCode(500)
                    start == "0" -> json("""{"results":[{"id":1}]}""")
                    start == "1" -> json("""{"results":[{"id":2}]}""")
                    else -> json("""{"results":[]}""")
                }
            }
        }

        val ids = loader().paginate(
            config(server, "\"is_cloud\":false"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            5,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(1, 2), ids)
    }

    @Test
    fun paginateUrl504HalvesMultipleTimes() = MockWebServer().use { server ->
        val limits = mutableListOf<Int>()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
                limits += limit
                return if (limit > 5) MockResponse().setResponseCode(504) else json("""{"results":[{"id":99}]}""")
            }
        }

        val ids = loader().paginate(
            config(server, "\"is_cloud\":true"),
            credentials(),
            "/rest/api/content/search?cql=type%3Dpage",
            40,
        ).map { it.path("id").asInt() }

        assertEquals(listOf(99), ids)
        assertEquals(listOf(40, 20, 10, 5), limits)
    }

    @Test
    fun nonRateLimitError(): Unit = MockWebServer().use { server ->
        server.enqueue(MockResponse().setResponseCode(418).setBody("not rate limited"))
        val checkpoint = mapper.valueToTree<JsonNode>(
            ConfluenceCheckpoint(
                hasMore = true,
                nextPageUrl = "/rest/api/content/search?cql=type%3Dpage&start=2&limit=50",
            ),
        )

        assertFailsWith<WebClientResponseException> {
            loader().load(config(server), credentials(), checkpoint).toList()
        }
        assertEquals(1, server.requestCount)
    }

    @Test
    fun rateLimitHonorsRetryAfter() {
        val farFuture = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).plusYears(1))
        listOf(
            "0" to 2_000L,
            "5" to 5_000L,
            "999" to 60_000L,
            farFuture to 60_000L,
        ).forEach { (retryAfter, expectedDelay) ->
            MockWebServer().use { server ->
                server.enqueue(MockResponse().setResponseCode(429).setHeader("Retry-After", retryAfter))
                server.enqueue(json("""{"results":[{"id":1}]}"""))
                val delays = mutableListOf<Long>()
                val loader = loader().also { it.sleepMillis = delays::add }

                val result = loader.paginate(config(server), credentials(), "/rest/api/content/search", 50)

                assertEquals(1, result.single().path("id").asInt())
                assertEquals(listOf(expectedDelay), delays)
            }
        }
    }

    @Test
    fun slimDocsSkipImagesWhenAllowImagesFalse() = MockWebServer().use { server ->
        server.dispatcher = slimMixedAttachmentDispatcher(server)

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":false"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertFalse(documents.any { it.title == "diagram.png" })
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    @Test
    fun slimDocsIncludeImagesWhenAllowImagesTrue() = MockWebServer().use { server ->
        server.dispatcher = slimMixedAttachmentDispatcher(server)

        val documents = loader().retrieveAllSlimDocuments(
            config(server, "\"allow_images\":true"),
            credentials(),
        ).flatMap { it.documents.asSequence() }.toList()

        assertTrue(documents.any { it.title == "diagram.png" })
        assertTrue(documents.any { it.title == "spec.pdf" })
    }

    private fun loader(): ConfluenceConnectorLoader =
        ConfluenceConnectorLoader(RemoteJsonClient(WebClient.builder()), mapper).also { it.sleepMillis = {} }

    private fun config(server: MockWebServer, extra: String = "") = config(
        server.startAndBase(),
        extra,
    )

    private fun config(base: String, extra: String = ""): JsonNode = mapper.readTree(
        """{"wiki_base":"$base"${if (extra.isBlank()) "" else ",$extra"}}""",
    )

    private fun credentials(token: String = "token"): JsonNode = mapper.readTree(
        """{"confluence_username":"user@example.com","confluence_access_token":"$token"}""",
    )

    private fun failure(url: String) = ConnectorFailure(FailureTarget.Document(url, url), "retry")

    private fun pageResponse(content: String = "<p>Run safely</p>"): String =
        mapper.writeValueAsString(mapOf("results" to listOf(mapper.readTree(page("111", content)))))

    private fun page(id: String, content: String = "<p>Run safely</p>"): String = """{
        "id":"$id",
        "title":"Runbook",
        "body":{"storage":{"value":${mapper.writeValueAsString(content)}}},
        "_links":{"webui":"/spaces/ENG/pages/$id/Runbook"},
        "space":{"key":"ENG","name":"Engineering"},
        "version":{"when":"2026-01-02T00:00:00Z","by":{"displayName":"Owner","email":"owner@example.com"}},
        "history":{"createdDate":"2026-01-01T00:00:00Z"},
        "metadata":{"labels":{"results":[{"name":"ops"}]}},
        "ancestors":[],
        "restrictions":{}
    }"""

    private fun pdfAttachment(): Map<String, Any> = mapOf(
        "id" to "att-222",
        "title" to "spec.pdf",
        "metadata" to mapOf("mediaType" to "application/pdf"),
        "extensions" to mapOf("fileSize" to 100),
        "_links" to mapOf(
            "download" to "/download/attachments/222/spec.pdf",
            "webui" to "/pages/viewpageattachments.action?pageId=222&preview=att-222",
        ),
        "space" to mapOf("key" to "ENG", "name" to "Engineering"),
        "version" to mapOf("when" to "2026-01-02T00:00:00Z"),
        "history" to mapOf("createdDate" to "2026-01-01T00:00:00Z"),
        "restrictions" to emptyMap<String, Any>(),
    )

    private fun imageAttachment(): Map<String, Any> = mapOf(
        "id" to "att-image",
        "title" to "diagram.png",
        "metadata" to mapOf("mediaType" to "image/png"),
        "extensions" to mapOf("fileSize" to 100),
        "_links" to mapOf(
            "download" to "/download/attachments/111/diagram.png",
            "webui" to "/pages/viewpageattachments.action?pageId=111&preview=att-image",
        ),
        "space" to mapOf("key" to "ENG", "name" to "Engineering"),
        "version" to mapOf("when" to "2026-01-02T00:00:00Z"),
        "history" to mapOf("createdDate" to "2026-01-01T00:00:00Z"),
        "restrictions" to emptyMap<String, Any>(),
    )

    private fun restrictions(users: List<String> = emptyList(), groups: List<String> = emptyList()): JsonNode = mapper.valueToTree(
        mapOf(
            "read" to mapOf(
                "restrictions" to mapOf(
                    "user" to mapOf("results" to users.map { mapOf("email" to it) }),
                    "group" to mapOf("results" to groups.map { mapOf("name" to it) }),
                ),
            ),
        ),
    )

    private fun restrictedPages(field: String, value: String): List<JsonNode> = listOf("1", "2").map { id ->
        (mapper.readTree(page(id)).deepCopy() as ObjectNode).also { page ->
            page.set(
                "restrictions",
                mapper.readTree(
                    """{"read":{"restrictions":{"user":{"results":[{"$field":"$value"}]},"group":{"results":[]}}}}""",
                ),
            )
        }
    }

    private fun userResolutionDispatcher(
        pages: List<JsonNode>,
        resolveUser: (RecordedRequest) -> MockResponse,
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when (request.requestUrl!!.encodedPath) {
            "/rest/api/space" -> json("""{"results":[]}""")
            "/rest/api/user" -> resolveUser(request)
            else -> json(mapper.writeValueAsString(mapOf("results" to pages)))
        }
    }

    private fun loadWithoutExtras(
        loader: ConfluenceConnectorLoader,
        server: MockWebServer,
        token: String,
    ): List<SourceDocument> = loader.load(
        config(server, "\"include_comments\":false,\"include_attachments\":false"),
        credentials(token),
        null,
    ).single().documents

    private fun attachmentDispatcher(
        pages: String,
        attachments: List<Map<String, Any>>,
        server: MockWebServer,
    ): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse = when {
            request.path!!.contains("type%3Dattachment") ->
                json(mapper.writeValueAsString(mapOf("results" to attachments)))
            request.path!!.endsWith("/download") || request.path!!.contains("/download/") -> {
                val body = if (request.path!!.contains("diagram")) "image bytes" else "attachment text"
                MockResponse().setBody(body)
            }
            request.path!!.contains("type%3Dcomment") -> json("""{"results":[]}""")
            else -> json(pages)
        }
    }

    private fun reindexDispatcher(ids: Set<String>, server: MockWebServer): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val pages = ids.map { mapper.readTree(page(it)) }
            return json(mapper.writeValueAsString(mapOf("results" to pages)))
        }
    }

    private fun reindexWithAttachmentDispatcher(server: MockWebServer, attachmentStatus: Int): Dispatcher =
        object : Dispatcher() {
            var attachmentPage = 0

            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dpage") -> json(pageResponse())
                request.path!!.contains("type%3Dattachment") && attachmentPage++ == 0 -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(pdfAttachment()),
                            "_links" to if (attachmentStatus == 400) {
                                mapOf("next" to "/rest/api/content/search?cql=type%3Dattachment&start=1&limit=50")
                            } else {
                                emptyMap()
                            },
                        ),
                    ),
                )
                request.path!!.contains("type%3Dattachment") -> MockResponse().setResponseCode(attachmentStatus)
                request.path!!.contains("/download/") -> MockResponse().setBody("attachment text")
                else -> json("""{"results":[]}""")
            }
        }

    private fun slimAttachmentDispatcher(server: MockWebServer, attachmentStatus: () -> Int): Dispatcher =
        object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path!!.contains("type%3Dattachment") -> {
                    val status = attachmentStatus()
                    if (status == 200) json(mapper.writeValueAsString(mapOf("results" to listOf(pdfAttachment()))))
                    else MockResponse().setResponseCode(status).setBody("attachment failure")
                }
                else -> json(pageResponse())
            }
        }

    private fun slimMixedAttachmentDispatcher(server: MockWebServer): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse =
            if (request.path!!.contains("type%3Dattachment")) {
                json(mapper.writeValueAsString(mapOf("results" to listOf(imageAttachment(), pdfAttachment()))))
            } else {
                json(pageResponse())
            }
    }

    private fun recoverablePaginationDispatcher(failAllRecoveryItems: Boolean): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val start = request.requestUrl!!.queryParameter("start")?.toInt() ?: 0
            val limit = request.requestUrl!!.queryParameter("limit")?.toInt() ?: 3
            return when {
                start == 0 && limit == 3 -> json(
                    """{"results":[{"id":1},{"id":2},{"id":3}],"_links":{"next":"/rest/api/content/search?cql=type%3Dpage&limit=3&start=3"}}""",
                )
                start == 3 && limit == 3 -> MockResponse().setResponseCode(500)
                start in 3..5 && limit == 1 && (failAllRecoveryItems || start == 4) -> MockResponse().setResponseCode(500)
                start == 3 && limit == 1 -> json("""{"results":[{"id":4}]}""")
                start == 5 && limit == 1 -> json("""{"results":[{"id":6}]}""")
                start == 6 && limit == 3 -> json("""{"results":[{"id":7}]}""")
                else -> json("""{"results":[]}""")
            }
        }
    }

    private fun reducedLimitSuccessDispatcher(status: Int, includeNext: Boolean = true): Dispatcher = object : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val limit = request.requestUrl!!.queryParameter("limit")!!.toInt()
            val start = request.requestUrl!!.queryParameter("start")
            return when {
                limit == 20 -> MockResponse().setResponseCode(status)
                start == null -> json(
                    mapper.writeValueAsString(
                        mapOf(
                            "results" to listOf(mapOf("id" to 1), mapOf("id" to 2)),
                            "_links" to if (includeNext) {
                                mapOf("next" to "/rest/api/content/search?cql=type%3Dpage&limit=20&start=2")
                            } else {
                                emptyMap()
                            },
                        ),
                    ),
                )
                start == "2" -> json("""{"results":[{"id":3}]}""")
                else -> error("Unexpected request ${request.path}")
            }
        }
    }

    private fun json(body: String): MockResponse =
        MockResponse().setHeader("Content-Type", "application/json").setBody(body)

    private fun MockWebServer.startAndBase(): String {
        if (port == -1) start()
        return url("/").toString().trimEnd('/')
    }

    private fun MockWebServer.takeRequests(count: Int): List<RecordedRequest> =
        (1..count).map { takeRequest() }

    private fun rewriteAtlassianRequestsTo(server: MockWebServer): ExchangeFilterFunction =
        ExchangeFilterFunction.ofRequestProcessor { request ->
            if (request.url().host != "api.atlassian.com") {
                reactor.core.publisher.Mono.just(request)
            } else {
                val local = server.url(request.url().rawPath + (request.url().rawQuery?.let { "?$it" } ?: ""))
                reactor.core.publisher.Mono.just(ClientRequest.from(request).url(URI.create(local.toString())).build())
            }
        }
}
