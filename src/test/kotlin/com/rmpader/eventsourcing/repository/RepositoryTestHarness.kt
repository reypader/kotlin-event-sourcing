package com.rmpader.eventsourcing.repository

import com.rmpader.eventsourcing.EventSerializer

data class TestEvent(
    val name: String,
    val value: Int,
)

data class TestState(
    val status: String,
    val count: Int,
)

class TestEventSerializer : EventSerializer<TestEvent> {
    override fun serialize(event: TestEvent): String = "${event.name}|${event.value}"

    override fun deserialize(data: String): TestEvent {
        val parts = data.split("|")
        return TestEvent(parts[0], parts[1].toInt())
    }
}

class TestStateSerializer : EventSerializer<TestState> {
    override fun serialize(event: TestState): String = "${event.status}|${event.count}"

    override fun deserialize(data: String): TestState {
        val parts = data.split("|")
        return TestState(parts[0], parts[1].toInt())
    }
}
