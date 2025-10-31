package com.rmpader.eventsourcing

interface AggregateLocator<A> {
    data class AggregateReference<A>(
        val aggregateId: String,
        val nodeId: String? = null, // null = local node
    )

    fun getAggregateReference(aggregateId: String): AggregateReference<A>
}
