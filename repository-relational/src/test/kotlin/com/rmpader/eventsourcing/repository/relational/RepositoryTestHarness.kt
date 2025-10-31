package com.rmpader.eventsourcing.repository.relational

import com.rmpader.eventsourcing.Serializer

data class TestEvent(
    val name: String,
    val value: Int,
)

data class TestState(
    val status: String,
    val count: Int,
)

class TestSerializer : Serializer<TestEvent> {
    override fun serialize(data: TestEvent): String = "${data.name}|${data.value}"

    override fun deserialize(data: String): TestEvent {
        val parts = data.split("|")
        return TestEvent(parts[0], parts[1].toInt())
    }
}

class TestStateSerializer : Serializer<TestState> {
    override fun serialize(data: TestState): String = "${data.status}|${data.count}"

    override fun deserialize(data: String): TestState {
        val parts = data.split("|")
        return TestState(parts[0], parts[1].toInt())
    }
}
