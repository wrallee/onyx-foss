package com.onyx.foss.kotlin.config

import org.springframework.ai.vectorstore.opensearch.autoconfigure.OpenSearchVectorStoreProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
@EnableConfigurationProperties(OnyxProperties::class, OpenSearchVectorStoreProperties::class)
class RuntimeConfiguration {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
