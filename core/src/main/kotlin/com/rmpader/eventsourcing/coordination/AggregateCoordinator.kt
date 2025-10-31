package com.rmpader.eventsourcing.coordination

interface AggregateCoordinator {
    sealed class AggregateLocation {
        data object Local : AggregateLocation()

        data class Remote(
            val nodeId: String,
        ) : AggregateLocation()
    }

    fun start()

    fun stop()

    fun locateAggregate(aggregateId: String): AggregateLocation
}
