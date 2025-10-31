package com.rmpader.eventsourcing

class CoordinatedAggregateLocator<A>(
    private val aggregateCoordinator: AggregateCoordinator,
) : AggregateLocator<A> {
    override fun getAggregateReference(aggregateId: String): AggregateLocator.AggregateReference<A> {
        val owner = aggregateCoordinator.getAggregateOwnner(aggregateId)
        return AggregateLocator.AggregateReference(
            aggregateId = aggregateId,
            nodeId = owner,
        )
    }
}
