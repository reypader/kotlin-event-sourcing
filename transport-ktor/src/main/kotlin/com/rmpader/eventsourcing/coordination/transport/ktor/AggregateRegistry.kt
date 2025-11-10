package com.rmpader.eventsourcing.coordination.transport.ktor

import com.rmpader.eventsourcing.AggregateKey
import com.rmpader.eventsourcing.AggregateManager
import com.rmpader.eventsourcing.AggregateState
import com.rmpader.eventsourcing.Serializer

data class AggregateRegistration<C, E, S : AggregateState<C, E, S>>(
    val commandSerializer: Serializer<C>,
    val stateSerializer: Serializer<S>,
    val aggregateManager: AggregateManager<C, E, S>,
) {
    suspend fun handleCommand(
        aggregateKey: AggregateKey,
        commandId: String,
        commandData: ByteArray,
    ): ByteArray {
        val command = commandSerializer.deserialize(commandData)
        val resultState = aggregateManager.acceptCommand(aggregateKey, commandId, command)
        return stateSerializer.serialize(resultState)
    }
}

class AggregateRegistry {
    private val registrations = mutableMapOf<String, AggregateRegistration<*, *, *>>()

    fun <C, E, S : AggregateState<C, E, S>> register(
        alias: String,
        commandSerializer: Serializer<C>,
        stateSerializer: Serializer<S>,
        aggregateManager: AggregateManager<C, E, S>,
    ) {
        registrations[alias] =
            AggregateRegistration(
                commandSerializer = commandSerializer,
                stateSerializer = stateSerializer,
                aggregateManager = aggregateManager,
            )
    }

    fun get(alias: String): AggregateRegistration<*, *, *> = registrations[alias]!!
}
