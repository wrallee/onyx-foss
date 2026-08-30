package com.onyx.foss.modelserver.health

import com.onyx.foss.modelserver.runtime.RERANKER_EXPORT_NOT_CONFIGURED
import com.onyx.foss.modelserver.runtime.RerankerRuntimeGate
import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.stereotype.Component

@Component("rerankerArtifact")
class RerankerHealthIndicator(
    private val rerankerRuntime: RerankerRuntimeGate,
) : HealthIndicator {
    override fun health(): Health = Health.down()
        .withDetail("code", RERANKER_EXPORT_NOT_CONFIGURED)
        .withDetail("message", rerankerRuntime.readinessMessage())
        .build()
}
