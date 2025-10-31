package com.rmpader.eventsourcing

import com.rmpader.eventsourcing.coordination.AggregateCoordinator
import com.rmpader.eventsourcing.coordination.CommandTransport
import com.rmpader.eventsourcing.repository.EventSourcingRepositoryException

abstract class CoordinatedAggregateManager<C, E, S : AggregateEntity<C, E, S>>(
    private val coordinator: AggregateCoordinator,
    private val commandTransport: CommandTransport<C>,
    private val localDelegate: AggregateManager<C, E, S>,
) : AggregateManager<C, E, S> {
    override suspend fun acceptCommand(
        entityId: String,
        command: C,
    ) {
        try {
            when (val location = coordinator.locateAggregate(entityId)) {
                is AggregateCoordinator.AggregateLocation.Local ->
                    executeLocally(entityId, command)

                is AggregateCoordinator.AggregateLocation.Remote ->
                    commandTransport.sendToNode(
                        location.nodeId,
                        entityId,
                        command,
                    )
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

    suspend fun executeLocally(
        entityId: String,
        command: C,
    ) {
        try {
            localDelegate.acceptCommand(entityId, command)
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
}
