package com.rmpader.eventsourcing

interface Serializer<E> {
    fun serialize(data: E): String

    fun deserialize(data: String): E
}
