package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.repository.AggregateRepository
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.lastOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime

class DefaultAggregateManager<C, E, S : AggregateEntity<C, E, S>>(
    val repository: AggregateRepository<E, S>,
    val aggregateInitializer: (String) -> S,
) : AggregateManager<C, E, S> {
    private sealed class ReplayResult<out S> {
        data class CommandAlreadyProcessed<S>(
            val state: S,
            val atSequenceNumber: Long,
        ) : ReplayResult<S>()

        data class ReadyForNewCommand<S>(
            val state: S,
            val nextSequence: Long,
        ) : ReplayResult<S>()
    }

    override suspend fun acceptCommand(
        entityId: String,
        commandId: String,
        command: C,
    ): S {
        try {
            val snapshotRecord = repository.loadLatestSnapshot(entityId)
            val currentState = snapshotRecord?.state ?: aggregateInitializer(entityId)
            val currentSequence = snapshotRecord?.sequenceNumber ?: 0
            val events = repository.loadEvents(entityId, currentSequence + 1)

            return when (val result = replayEventsWithIdempotency(currentState, currentSequence, commandId, events)) {
                is ReplayResult.CommandAlreadyProcessed -> {
                    logger.info("Command duplicate detected for command $commandId at sequence number ${result.atSequenceNumber}")
                    result.state
                }

                is ReplayResult.ReadyForNewCommand -> {
                    val finalState = result.state
                    val resultingEvent = finalState.handleCommand(command)
                    logger.info("Persisting event $resultingEvent for entity $entityId at sequence ${result.nextSequence}")
                    repository.storeEvent(
                        AggregateRepository.EventRecord(
                            entityId,
                            resultingEvent,
                            result.nextSequence,
                            OffsetDateTime.now(),
                            commandId,
                        ),
                    )
                    finalState.applyEvent(resultingEvent)
                }
            }
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

    private suspend fun replayEventsWithIdempotency(
        initialState: S,
        startingSequence: Long,
        commandId: String,
        events: Flow<AggregateRepository.EventRecord<E>>,
    ): ReplayResult<S> {
        var result: ReplayResult.CommandAlreadyProcessed<S>? = null
        var currentState = initialState
        logger.debug("Current state at {}", currentState)
        events.collect { eventRecord ->
            logger.debug("Applying event {}", eventRecord)
            currentState = currentState.applyEvent(eventRecord.event)

            if (eventRecord.originCommandId == commandId) {
                result = ReplayResult.CommandAlreadyProcessed(currentState, eventRecord.sequenceNumber)
                return@collect
            }
        }
        val nextSequenceNumber = (events.lastOrNull()?.sequenceNumber ?: startingSequence) + 1
        return result ?: ReplayResult.ReadyForNewCommand(currentState, nextSequenceNumber)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(DefaultAggregateManager::class.java)
    }
}
