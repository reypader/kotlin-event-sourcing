package com.rmpader.eventsourcing.coordination

interface AggregateCoordinator {
    sealed class AggregateLocation {
        data object Local : AggregateLocation()

        data class Remote(
            val nodeId: String,
        ) : AggregateLocation()
    }

    data class HealthStatus(
        val healthy: Boolean,
        val details: Map<String, String> = emptyMap(),
    ) {
        companion object {
            fun healthy(details: Map<String, String> = emptyMap()) = HealthStatus(healthy = true, details = details)

            fun unhealthy(
                reason: String,
                details: Map<String, String> = emptyMap(),
            ) = HealthStatus(
                healthy = false,
                details = details + ("reason" to reason),
            )
        }
    }

    fun start()

    fun stop()

    fun locateAggregate(aggregateId: String): AggregateLocation

    fun checkHealth(): HealthStatus
}
