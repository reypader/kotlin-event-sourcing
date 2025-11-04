package com.rmpader.eventsourcing

data class AggregateKey(
    val entityId: String,
    val entityAlias: String,
) {
    init {
        require(entityId.isNotBlank()) { "entityId cannot be blank." }
        require(!entityId.contains('|')) { "entityId cannot contain |." }
        require(entityAlias.isNotBlank()) { "entityAlias cannot be blank." }
        require(!entityAlias.contains('|')) { "entityAlias cannot contain |." }
    }

    override fun toString(): String = "$entityAlias|$entityId"

    companion object {
        fun from(keyString: String): AggregateKey {
            val split = keyString.split('|')
            return AggregateKey(split[1], split[0])
        }
    }
}
