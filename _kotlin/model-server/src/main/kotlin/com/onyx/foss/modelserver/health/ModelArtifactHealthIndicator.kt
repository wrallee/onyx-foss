package com.onyx.foss.modelserver.health

import com.onyx.foss.modelserver.runtime.GraniteOpenVinoEmbeddingRuntime
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component

@Component("modelArtifact")
class ModelArtifactHealthIndicator(
    private val embeddingRuntime: GraniteOpenVinoEmbeddingRuntime,
) : HealthIndicator {
    override fun health(): Health {
        val readiness = embeddingRuntime.readiness()
        val builder = if (readiness.ready) Health.up() else Health.down()
        builder.withDetail("code", readiness.code)
            .withDetail("message", readiness.message)
        readiness.modelName?.let { builder.withDetail("modelName", it) }
        readiness.device?.let { builder.withDetail("device", it) }
        return builder.build()
    }
}
