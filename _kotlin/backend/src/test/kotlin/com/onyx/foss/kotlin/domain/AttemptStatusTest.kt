package com.onyx.foss.kotlin.domain

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AttemptStatusTest {
    @Test
    fun statusValuesMatchTheFrontendContract() {
        assertThat(AttemptStatus.entries.map { it.value }).containsExactly(
            "not_started",
            "in_progress",
            "success",
            "failed",
            "completed_with_errors",
        )
        assertThat(jacksonObjectMapper().writeValueAsString(AttemptStatus.NOT_STARTED))
            .isEqualTo("\"not_started\"")
    }
}
