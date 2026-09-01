package com.onyx.foss.kotlin.config

import io.netty.channel.ChannelOption
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient
import java.time.Duration

internal fun WebClient.Builder.buildModelServerClient(config: OnyxProperties.ModelServer): WebClient {
    val httpClient = HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(config.connectTimeoutMs))
        .responseTimeout(Duration.ofMillis(config.readTimeoutMs))
    return clone().clientConnector(ReactorClientHttpConnector(httpClient)).build()
}
