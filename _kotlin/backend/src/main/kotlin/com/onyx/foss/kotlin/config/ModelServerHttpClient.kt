package com.onyx.foss.kotlin.config

import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.core5.util.Timeout
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.web.client.RestClient

internal fun RestClient.Builder.buildModelServerClient(config: OnyxProperties.ModelServer): RestClient {
    val requestConfig = RequestConfig.custom()
        .setResponseTimeout(Timeout.ofMilliseconds(config.readTimeoutMs))
        .build()

    val connectionConfig = ConnectionConfig.custom()
        .setConnectTimeout(Timeout.ofMilliseconds(config.connectTimeoutMs))
        .setSocketTimeout(Timeout.ofMilliseconds(config.readTimeoutMs))
        .build()

    val connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
        .setDefaultConnectionConfig(connectionConfig)
        .build()

    val httpClient = HttpClients.custom()
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig)
        .build()

    return clone()
        .requestFactory(HttpComponentsClientHttpRequestFactory(httpClient))
        .baseUrl(config.baseUrl.trimEnd('/'))
        .build()
}
