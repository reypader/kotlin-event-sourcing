package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.repository.AggregateRepository
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.lastOrNull
import java.time.OffsetDateTime

abstract class DefaultAggregateManager<C, E, S : AggregateEntity<C, E, S>>(
    val repository: AggregateRepository<E, S>,
) : AggregateManager<C, E, S> {
    override suspend fun acceptCommand(
        entityId: String,
        command: C,
    ) {
        try {
            val snapshotRecord = repository.loadLatestSnapshot(entityId)
            val startingState = snapshotRecord?.state ?: initializeAggregate(entityId)
            val currentSequence = snapshotRecord?.sequenceNumber ?: 0

            val events = repository.loadEvents(entityId, currentSequence + 1)
            val finalState =
                events.fold(startingState) { state, eventRecord ->
                    state.applyEvent(eventRecord.event)
                }

            val resultingEvent = finalState.handleCommand(command)
            val nextSequenceNumber = (events.lastOrNull()?.sequenceNumber ?: currentSequence) + 1
            repository.storeEvent(
                AggregateRepository.EventRecord(
                    entityId,
                    resultingEvent,
                    nextSequenceNumber,
                    OffsetDateTime.now(),
                ),
            )
        } catch (e: CommandRejectionException) {
            throw e
        } catch (e: EventSourcingRepositoryException) {
            throw e
        } catch (e: Exception) {
            // Wrap unexpected local exceptions
            throw CommandRejectionException(
                reason = "Local command execution failed: ${e.message ?: "Unknown error"}",
                errorCode = "LOCAL_EXECUTION_ERROR",
                rootCause = e,
            )
        }
    }
}
