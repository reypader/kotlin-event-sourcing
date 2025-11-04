package com.rmpader.eventsourcing

interface AggregateManager<C, E, S : AggregateState<C, E, S>> {
    suspend fun acceptCommand(
        aggregateKey: AggregateKey,
        commandId: String,
        command: C,
    ): S
}
