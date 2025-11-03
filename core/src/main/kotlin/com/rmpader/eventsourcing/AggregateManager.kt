package com.rmpader.eventsourcing

interface AggregateManager<C, E, S : AggregateEntity<C, E, S>> {
    suspend fun acceptCommand(
        entityId: String,
        commandId: String,
        command: C,
    ): S
}
