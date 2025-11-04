package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CoordinatedAggregateManager<C, E, S : AggregateState<C, E, S>>(
    private val coordinator: AggregateCoordinator,
    private val commandTransport: CommandTransport,
    private val localDelegate: AggregateManager<C, E, S>,
    private val commandSerializer: Serializer<C>,
    private val stateDeserializer: Serializer<S>,
    private val localFallbackCondition: (Exception) -> Boolean = { false },
) : AggregateManager<C, E, S> {
    override suspend fun acceptCommand(
        aggregateKey: AggregateKey,
        commandId: String,
        command: C,
    ): S {
        try {
            return when (val location = coordinator.locateAggregate(aggregateKey)) {
                is AggregateCoordinator.AggregateLocation.Local -> {
                    logger.info("Executing command locally: $commandId")
                    executeLocally(aggregateKey, commandId, command)
                }

                is AggregateCoordinator.AggregateLocation.Remote -> {
                    logger.info("Executing command remotely to ${location.nodeId}: $commandId")
                    executeRemotely(
                        location.nodeId,
                        aggregateKey,
                        commandId,
                        command,
                    )
                }
            }
        } catch (e: EventSourcingRepositoryException) {
            throw e
        } catch (e: CommandRejectionException) {
            throw e
        } catch (e: Exception) {
            throw CommandRejectionException(
                reason = "Command processing failed: ${e.message ?: "Unknown error"}",
                errorCode = "REMOTE_EXECUTION_ERROR",
                rootCause = e,
            )
        }
    }

    suspend fun executeRemotely(
        nodeId: String,
        aggregateKey: AggregateKey,
        commandId: String,
        command: C,
    ): S {
        try {
            val requestData = commandSerializer.serialize(command)
            val responseData =
                commandTransport.sendToNode(
                    nodeId,
                    aggregateKey,
                    commandId,
                    requestData,
                )
            return stateDeserializer.deserialize(responseData)
        } catch (e: Exception) {
            if (localFallbackCondition(e)) {
                logger.info("Falling back to local processing...")
                return executeLocally(aggregateKey, commandId, command)
            } else {
                when (e) {
                    is CommandRejectionException -> throw e
                    else -> throw CommandRejectionException(
                        reason = "Command processing failed: ${e.message ?: "Unknown error"}",
                        errorCode = "REMOTE_EXECUTION_ERROR",
                        rootCause = e,
                    )
                }
            }
        }
    }

    suspend fun executeLocally(
        aggregateKey: AggregateKey,
        commandId: String,
        command: C,
    ): S {
        try {
            return localDelegate.acceptCommand(aggregateKey, commandId, command)
        } catch (e: EventSourcingRepositoryException) {
            throw e
        } catch (e: CommandRejectionException) {
            throw e
        } catch (e: Exception) {
            throw CommandRejectionException(
                reason = "Local command execution failed: ${e.message ?: "Unknown error"}",
                errorCode = "LOCAL_EXECUTION_ERROR",
                rootCause = e,
            )
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoordinatedAggregateManager::class.java)
    }
}
