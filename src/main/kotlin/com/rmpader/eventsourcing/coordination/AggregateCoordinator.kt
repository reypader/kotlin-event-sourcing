package com.rmpader.eventsourcing.coordination

interface AggregateCoordinator {
    fun start()

    fun stop()

    fun getAggregateOwnner(aggregateId: String): String?
}
