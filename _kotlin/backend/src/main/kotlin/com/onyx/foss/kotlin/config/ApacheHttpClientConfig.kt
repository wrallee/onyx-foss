package com.onyx.foss.kotlin.config

import com.onyx.foss.kotlin.ingestion.OpenSearchIndexer
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.core5.util.TimeValue
import org.opensearch.client.opensearch.OpenSearchClient
import org.springframework.ai.vectorstore.opensearch.autoconfigure.OpenSearchVectorStoreProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.RestClient

@Configuration
class ApacheHttpClientConfig {
    @Bean
    fun poolingHttpClientConnectionManager(): PoolingHttpClientConnectionManager =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setMaxConnTotal(100)
            .setMaxConnPerRoute(50)
            .setConnectionTimeToLive(TimeValue.ofMinutes(15))
            .setValidateAfterInactivity(TimeValue.ofSeconds(10))
            .build()

    @Bean
    fun closeableHttpClient(connectionManager: PoolingHttpClientConnectionManager): CloseableHttpClient =
        HttpClients.custom()
            .setConnectionManager(connectionManager)
            .evictIdleConnections(TimeValue.ofSeconds(30))
            .disableAutomaticRetries()
            .build()

    @Bean
    fun restClientBuilder(httpClient: CloseableHttpClient): RestClient.Builder =
        RestClient.builder()
            .requestFactory(HttpComponentsClientHttpRequestFactory(httpClient))

    @Bean
    fun openSearchClient(
        properties: OpenSearchVectorStoreProperties,
        @Value("\${OPENSEARCH_VERIFY_CERTS:false}") verifyCerts: Boolean = false,
    ): OpenSearchClient =
        OpenSearchIndexer.createOpenSearchClient(properties, verifyCerts)
}
