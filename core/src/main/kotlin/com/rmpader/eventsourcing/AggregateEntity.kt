package com.rmpader.eventsourcing

interface AggregateEntity<C, E, S : AggregateEntity<C, E, S>> {
    val entityId: String

    fun handleCommand(command: C): E

    fun applyEvent(event: E): S
}
