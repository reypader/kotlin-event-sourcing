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
    override fun serialize(data: TestEvent): ByteArray = "${data.name}|${data.value}".toByteArray()

    override fun deserialize(data: ByteArray): TestEvent {
        val parts = String(data).split("|")
        return TestEvent(parts[0], parts[1].toInt())
    }
}

class TestStateSerializer : Serializer<TestState> {
    override fun serialize(data: TestState): ByteArray = "${data.status}|${data.count}".toByteArray()

    override fun deserialize(data: ByteArray): TestState {
        val parts = String(data).split("|")
        return TestState(parts[0], parts[1].toInt())
    }
}
