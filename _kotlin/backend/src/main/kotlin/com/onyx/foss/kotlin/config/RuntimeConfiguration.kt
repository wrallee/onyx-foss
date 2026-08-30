package com.onyx.foss.kotlin.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClient

@Configuration
@EnableConfigurationProperties(OnyxProperties::class)
class RuntimeConfiguration {
    @Bean
    fun webClientBuilder(): WebClient.Builder = WebClient.builder()
}
