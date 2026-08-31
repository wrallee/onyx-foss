package com.onyx.foss.kotlin.ingestion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.onyx.foss.kotlin.service.FileStorageService
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.mock
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileConnectorLoaderTest {
    @TempDir
    lateinit var tempDir: Path

    private val mapper = jacksonObjectMapper()

    @Test
    fun singleTextFileWithMetadata() {
        val document = load(
            mapOf(
                "text-1" to write(
                    "notes.txt",
                    """
                    #ONYX_METADATA={"link":"https://onyx.app","file_display_name":"Display name","tag_of_your_choice":"test-tag"}
                    Test answer is 12345
                    """.trimIndent(),
                ),
            ),
            """{"file_locations":["text-1"],"file_names":["notes.txt"]}""",
        ).single()

        assertEquals("FILE_CONNECTOR__text-1", document.id)
        assertEquals("Display name", document.title)
        assertEquals("Test answer is 12345", document.content)
        assertEquals("https://onyx.app", document.link)
        assertEquals("file", document.metadata["source"])
        assertEquals("test-tag", document.metadata["tag_of_your_choice"])
        assertNull(document.metadata["file_id"])
    }

    @Test
    fun twoTextFilesWithZipMetadata() {
        val documents = load(
            mapOf(
                "file-1" to write("file1.txt", "File 1 content"),
                "file-2" to write("file2.txt", "File 2 content"),
                "metadata" to write(
                    ".onyx_metadata.json",
                    """[
                      {"filename":"file1.txt","file_display_name":"Display 1","link":"https://onyx.app/1","tag":"one"},
                      {"filename":"file2.txt","file_display_name":"Display 2","link":"https://onyx.app/2","tag":"two"}
                    ]""",
                ),
            ),
            """{"file_locations":["file-1","file-2"],"file_names":["file1.txt","file2.txt"],"zip_metadata_file_id":"metadata"}""",
        )

        assertEquals(listOf("FILE_CONNECTOR__file-1", "FILE_CONNECTOR__file-2"), documents.map { it.id })
        assertEquals(listOf("Display 1", "Display 2"), documents.map { it.title })
        assertEquals(listOf("File 1 content", "File 2 content"), documents.map { it.content })
        assertEquals(listOf("https://onyx.app/1", "https://onyx.app/2"), documents.map { it.link })
        assertEquals(listOf("one", "two"), documents.map { it.metadata["tag"] })
        assertTrue(documents.all { it.metadata["file_id"] == null })
    }

    @Test
    fun tabularFileSetsFileIdOnDocument() {
        val document = load(
            mapOf("csv-1" to write("data.csv", "name,value\nAlice,1\nBob,2\n")),
            """{"file_locations":["csv-1"],"file_names":["data.csv"]}""",
        ).single()

        assertEquals("FILE_CONNECTOR__csv-1", document.id)
        assertEquals("data.csv", document.title)
        assertTrue(document.content.contains("Alice"))
        assertEquals("csv-1", document.metadata["file_id"])
    }

    @Test
    fun nonTabularFileLeavesFileIdNone() {
        val document = load(
            mapOf("text-1" to write("notes.txt", "Some plain text content for the LLM.")),
            """{"file_locations":["text-1"],"file_names":["notes.txt"]}""",
        ).single()

        assertNull(document.metadata["file_id"])
        assertEquals("file", document.metadata["source"])
    }

    @Test
    fun mixedBatchOnlyTabularGetsFileId() {
        val documents = load(
            mapOf(
                "csv-1" to write("data.csv", "col,val\nfoo,1\nbar,2\n"),
                "text-1" to write("notes.txt", "Just some notes."),
            ),
            """{"file_locations":["csv-1","text-1"],"file_names":["data.csv","notes.txt"]}""",
        )

        assertEquals("csv-1", documents[0].metadata["file_id"])
        assertNull(documents[1].metadata["file_id"])
        assertFalse(documents[1].metadata.containsKey("file_id"))
    }

    private fun load(paths: Map<String, Path>, config: String): List<SourceDocument> {
        val files = mock(FileStorageService::class.java)
        paths.forEach { (id, path) -> doReturn(path).`when`(files).filePath(id) }
        return FileConnectorLoader(mapper, files).load(mapper.readTree(config)).single().documents
    }

    private fun write(name: String, contents: String): Path =
        Files.writeString(tempDir.resolve(name), contents)
}
