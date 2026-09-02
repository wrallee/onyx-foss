package com.onyx.foss.kotlin.ingestion

import com.onyx.foss.kotlin.config.OnyxProperties
import com.onyx.foss.kotlin.domain.ConnectorCredentialPairRepository
import com.onyx.foss.kotlin.domain.ConnectorRepository
import com.onyx.foss.kotlin.domain.IngestionAttemptRepository
import com.onyx.foss.kotlin.domain.JobState
import com.onyx.foss.kotlin.domain.PairStatus
import com.onyx.foss.kotlin.service.AdminService
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Component
class IngestionScheduler(
    private val properties: OnyxProperties,
    private val pairs: ConnectorCredentialPairRepository,
    private val connectors: ConnectorRepository,
    private val attempts: IngestionAttemptRepository,
    private val admin: AdminService,
) {
    @Scheduled(fixedDelayString = "\${onyx.scheduler.poll-delay-ms:15000}")
    @Transactional
    fun schedule() {
        if (properties.worker.enabled) scheduleDue(Instant.now())
    }

    @Transactional
    fun scheduleDue(now: Instant) {
        pairs.findSchedulable(
            listOf(PairStatus.SCHEDULED, PairStatus.INITIAL_INDEXING, PairStatus.ACTIVE),
            listOf(JobState.QUEUED, JobState.RUNNING),
        ).forEach { pair ->
            // ponytail: batch this projection if scheduler query volume becomes measurable.
            val pairId = requireNotNull(pair.id)
            val connector = connectors.findById(pair.connectorId).orElseThrow()
            val lastAttempt = attempts.findFirstByCcPairIdAndPruneOnlyFalseOrderByTimeUpdatedDescIdDesc(pairId)
            val lastAttemptAt = lastAttempt?.timeUpdated ?: lastAttempt?.timeStarted ?: now
            val pruneDue = connector.pruneFreq?.let { frequency ->
                pair.lastPrunedAt?.plusSeconds(frequency)?.isAfter(now) != true
            } == true
            val refreshDue = when {
                lastAttempt == null -> true
                connector.refreshFreq == null -> false
                pair.status == PairStatus.INITIAL_INDEXING && !pair.inRepeatedErrorState -> true
                else -> !lastAttemptAt.plusSeconds(connector.refreshFreq!!).isAfter(now)
            }
            when {
                pruneDue -> admin.enqueuePair(pairId, fromBeginning = false, pruneOnly = true)
                refreshDue -> admin.enqueuePair(pairId, fromBeginning = false)
            }
        }
    }
}
