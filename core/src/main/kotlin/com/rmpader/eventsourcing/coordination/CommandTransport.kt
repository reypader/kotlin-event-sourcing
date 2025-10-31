package com.rmpader.eventsourcing.coordination

interface CommandTransport<C> {
    suspend fun sendToNode(
        nodeId: String,
        entityId: String,
        command: C,
    )
}
