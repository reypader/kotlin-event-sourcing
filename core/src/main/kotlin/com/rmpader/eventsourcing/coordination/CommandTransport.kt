package com.rmpader.eventsourcing.coordination

interface CommandTransport<C, S> {
    suspend fun sendToNode(
        nodeId: String,
        entityId: String,
        commandId: String,
        command: C,
    ): S
}
