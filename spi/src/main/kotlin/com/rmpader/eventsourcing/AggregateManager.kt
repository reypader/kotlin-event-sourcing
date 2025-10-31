package com.rmpader.eventsourcing

interface AggregateManager<C, E, S : AggregateEntity<C, E, S>> {
    fun initializeAggregate(entityId: String): S

    suspend fun acceptCommand(
        entityId: String,
        command: C,
    )
}
