package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class CoordinatedAggregateManager<C, E, S : AggregateEntity<C, E, S>>(
    private val coordinator: AggregateCoordinator,
    private val commandTransport: CommandTransport<C, S>,
    private val localDelegate: AggregateManager<C, E, S>,
    private val localFallbackCondition: (Exception) -> Boolean = { false },
) : AggregateManager<C, E, S> {
    override suspend fun acceptCommand(
        entityId: String,
        commandId: String,
        command: C,
    ): S {
        try {
            return when (val location = coordinator.locateAggregate(entityId)) {
                is AggregateCoordinator.AggregateLocation.Local -> {
                    logger.info("Executing command locally: $commandId")
                    executeLocally(entityId, commandId, command)
                }

                is AggregateCoordinator.AggregateLocation.Remote -> {
                    logger.info("Executing command remotely to ${location.nodeId}: $commandId")

                    executeRemotely(
                        location.nodeId,
                        entityId,
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
        entityId: String,
        commandId: String,
        command: C,
    ): S {
        try {
            return commandTransport.sendToNode(
                nodeId,
                entityId,
                commandId,
                command,
            )
        } catch (e: Exception) {
            if (localFallbackCondition(e)) {
                return executeLocally(entityId, commandId, command)
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
        entityId: String,
        commandId: String,
        command: C,
    ): S {
        try {
            return localDelegate.acceptCommand(entityId, commandId, command)
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
