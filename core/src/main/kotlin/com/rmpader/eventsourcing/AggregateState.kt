package com.rmpader.eventsourcing

interface AggregateState<C, E, S : AggregateState<C, E, S>> {
    val key: AggregateKey

    fun handleCommand(command: C): E

    fun applyEvent(event: E): S
}
