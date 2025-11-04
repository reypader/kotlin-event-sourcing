package com.rmpader.eventsourcing

interface Serializer<T> {
    fun serialize(data: T): ByteArray

    fun deserialize(data: ByteArray): T
}
