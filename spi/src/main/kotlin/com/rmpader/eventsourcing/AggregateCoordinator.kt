package com.rmpader.eventsourcing

interface AggregateCoordinator {
    fun start()

    fun stop()

    fun getAggregateOwnner(aggregateId: String): String?
}
