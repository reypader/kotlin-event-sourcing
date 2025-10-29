package com.rmpader.com.rmpader.eventsourcing

interface EventSerializer<E> {
    fun serialize(event: E): String
    fun deserialize(data: String): E
}