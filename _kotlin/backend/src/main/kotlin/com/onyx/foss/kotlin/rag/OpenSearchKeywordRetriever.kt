package com.onyx.foss.kotlin.rag

import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch._types.FieldValue
import org.springframework.ai.document.Document
import org.springframework.ai.rag.Query
import org.springframework.ai.rag.retrieval.search.DocumentRetriever

class OpenSearchKeywordRetriever(
    private val client: OpenSearchClient,
    private val indexName: String,
    private val count: Int = 50,
    private val documentSets: List<String> = emptyList(),
) : DocumentRetriever {

    override fun retrieve(query: Query): List<Document> {
        val queryText = query.text()
        if (queryText.isBlank()) return emptyList()

        val response = client.search({ s ->
            s.index(indexName)
                .size(count)
                .query { q ->
                    q.bool { b ->
                        b.must { m ->
                            m.multiMatch { mm ->
                                mm.query(queryText)
                                    .fields("title^2", "content")
                            }
                        }
                        val sets = documentSets.distinct().filter(String::isNotBlank)
                        if (sets.isNotEmpty()) {
                            b.filter { f ->
                                f.terms { t ->
                                    t.field("document_sets")
                                        .terms { fv ->
                                            fv.value(sets.map { FieldValue.of(it) })
                                        }
                                }
                            }
                        }
                        b
                    }
                }
        }, Map::class.java as Class<Map<String, Any?>>)

        return response.hits().hits().map { hit ->
            val source = hit.source() ?: emptyMap()
            val metadata = HashMap<String, Any>()
            source.forEach { (k, v) ->
                if (k != "content" && v != null) metadata[k] = v
            }
            Document.builder()
                .id(hit.id() ?: "")
                .text(source["content"]?.toString() ?: "")
                .metadata(metadata)
                .score(hit.score())
                .build()
        }
    }
}
