package com.rmpader.eventsourcing.repository

import com.rmpader.eventsourcing.AggregateKey
import kotlinx.coroutines.flow.Flow
import java.time.OffsetDateTime

interface AggregateRepository<E, S> {
    data class EventRecord<E>(
        val aggregateKey: AggregateKey,
        val event: E,
        val sequenceNumber: Long,
        val timestamp: OffsetDateTime,
        val originCommandId: String,
    )

    data class OutboxRecord<E>(
        val eventId: String,
        val event: E,
    )

    data class SnapshotRecord<S>(
        val aggregateKey: AggregateKey,
        val state: S,
        val sequenceNumber: Long,
        val timestamp: OffsetDateTime,
    )

    fun loadEvents(
        aggregateKey: AggregateKey,
        fromSequenceNumber: Long,
    ): Flow<EventRecord<E>>

    suspend fun loadLatestSnapshot(aggregateKey: AggregateKey): SnapshotRecord<S>?

    suspend fun storeEvent(eventRecord: EventRecord<E>)

    suspend fun deleteFromOutbox(eventIds: Set<String>)

    suspend fun pollOutbox(limit: Int): Flow<OutboxRecord<E>>

    suspend fun cleanupStaleOutboxClaims(staleAfterMillis: Long): Long
}
