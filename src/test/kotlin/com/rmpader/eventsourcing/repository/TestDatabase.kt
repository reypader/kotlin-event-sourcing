package com.rmpader.eventsourcing.repository

import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.util.*

object TestDatabase {
    fun createR2dbcConnectionFactory(): ConnectionFactory =
        ConnectionFactories.get(
            ConnectionFactoryOptions
                .builder()
                .option(ConnectionFactoryOptions.DRIVER, "h2")
                .option(ConnectionFactoryOptions.PROTOCOL, "mem")
                .option(ConnectionFactoryOptions.DATABASE, "testdb;DB_CLOSE_DELAY=-1")
                .option(ConnectionFactoryOptions.USER, "sa")
                .option(ConnectionFactoryOptions.PASSWORD, "")
                .build(),
        )

    private fun createJdbcConnection(): Connection =
        DriverManager.getConnection(
            "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1",
            "sa",
            "",
        )

    fun initializeSchema() {
        val schemaSql =
            TestDatabase::class.java.classLoader
                .getResource("h2schema.sql")
                ?.readText()
                ?: throw IllegalStateException("Could not find h2schema.sql in test resources")

        val statements =
            schemaSql
                .split(";")
                .map { it.trim() }
                .filter { it.isNotBlank() && !it.startsWith("--") }

        createJdbcConnection().use { connection ->
            connection.autoCommit = false
            try {
                statements.forEach { statement ->
                    connection.createStatement().use { stmt ->
                        stmt.execute(statement)
                    }
                }
                connection.commit()
            } catch (e: Exception) {
                connection.rollback()
                throw e
            }
        }
    }

    fun cleanDatabase() {
        createJdbcConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("DROP TABLE IF EXISTS EVENT_OUTBOX")
                statement.execute("DROP TABLE IF EXISTS EVENT_JOURNAL")
                statement.execute("DROP TABLE IF EXISTS SNAPSHOTS")
            }
        }
    }

    /**
     * Assert that exactly one event exists in EVENT_JOURNAL with the specified values
     */
    fun assertEventJournalContains(
        entityId: String,
        eventData: String,
        sequenceNumber: Long,
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                SELECT COUNT(*) as CNT
                FROM EVENT_JOURNAL
                WHERE ENTITY_ID = ? AND EVENT_DATA = ? AND SEQUENCE_NUMBER = ?
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, entityId)
                statement.setString(2, eventData)
                statement.setLong(3, sequenceNumber)

                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != 1L) {
                        throw AssertionError(
                            "Expected exactly 1 event in EVENT_JOURNAL with " +
                                "entityId='$entityId', eventData='$eventData', sequenceNumber=$sequenceNumber, " +
                                "but found $count",
                        )
                    }
                }
            }
        }
    }

    /**
     * Assert that exactly one event exists in EVENT_OUTBOX with the specified values
     */
    fun assertEventOutboxContains(
        eventId: String,
        eventData: String,
        isClaimed: Boolean = false,
    ) {
        createJdbcConnection().use { connection ->
            val sql =
                """
                SELECT COUNT(*) as CNT
                FROM EVENT_OUTBOX
                WHERE EVENT_ID = ? AND EVENT_DATA = ?
            """ +
                    if (isClaimed) {
                        " AND CLAIM_ID IS NOT NULL AND CLAIMED_AT IS NOT NULL"
                    } else {
                        ""
                    }

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, eventId)
                statement.setString(2, eventData)

                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != 1L) {
                        throw AssertionError(
                            "Expected exactly 1 event in EVENT_OUTBOX with " +
                                "eventId='$eventId', eventData='$eventData'",
                        )
                    }
                }
            }
        }
    }

    /**
     * Assert that a snapshot exists with specific values
     */
    fun assertSnapshotContains(
        entityId: String,
        stateData: String,
        sequenceNumber: Long,
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                SELECT COUNT(*) as CNT
                FROM SNAPSHOTS
                WHERE ENTITY_ID = ? AND STATE_DATA = ? AND SEQUENCE_NUMBER = ?
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, entityId)
                statement.setString(2, stateData)
                statement.setLong(3, sequenceNumber)

                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != 1L) {
                        throw AssertionError(
                            "Expected snapshot with entityId='$entityId', " +
                                "stateData='$stateData', sequenceNumber=$sequenceNumber, " +
                                "but found $count",
                        )
                    }
                }
            }
        }
    }

    /**
     * Assert the total count of events for an entity in EVENT_JOURNAL
     */
    fun assertEventJournalCount(expectedCount: Long) {
        createJdbcConnection().use { connection ->
            val sql = """
                SELECT COUNT(*) as CNT
                FROM EVENT_JOURNAL
            """

            connection.prepareStatement(sql).use { statement ->
                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != expectedCount) {
                        throw AssertionError(
                            "Expected $expectedCount events in EVENT_JOURNAL, but found $count",
                        )
                    }
                }
            }
        }
    }

    /**
     * Assert the total count of events for an entity in EVENT_OUTBOX
     */
    fun assertEventOutboxCount(expectedCount: Long) {
        createJdbcConnection().use { connection ->
            val sql = """
                SELECT COUNT(*) as CNT
                FROM EVENT_OUTBOX
            """

            connection.prepareStatement(sql).use { statement ->

                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != expectedCount) {
                        throw AssertionError(
                            "Expected $expectedCount events in EVENT_OUTBOX, but found $count",
                        )
                    }
                }
            }
        }
    }

    /**
     * Assert snapshot entry count
     */
    fun assertSnapshotCount(expectedCount: Long) {
        createJdbcConnection().use { connection ->
            val sql = """
                SELECT COUNT(*) as CNT
                FROM SNAPSHOTS
            """

            connection.prepareStatement(sql).use { statement ->
                statement.executeQuery().use { rs ->
                    rs.next()
                    val count = rs.getLong("CNT")
                    if (count != expectedCount) {
                        throw AssertionError(
                            "Expected $expectedCount snapshots, but found $count",
                        )
                    }
                }
            }
        }
    }

    fun insertJournalDirect(
        entityId: String,
        eventData: String,
        sequenceNumber: Long,
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                INSERT INTO EVENT_JOURNAL (ENTITY_ID, SEQUENCE_NUMBER, EVENT_DATA)
                VALUES (?, ?, ?)
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, entityId)
                statement.setLong(2, sequenceNumber)
                statement.setString(3, eventData)
                statement.executeUpdate()
            }
        }
    }

    fun insertUnclaimedOutboxDirect(
        eventId: String,
        eventData: String,
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                INSERT INTO EVENT_OUTBOX (EVENT_ID, EVENT_DATA)
                VALUES (?, ?)
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, eventId)
                statement.setString(2, eventData)
                statement.executeUpdate()
            }
        }
    }

    fun insertClaimedOutboxDirect(
        eventId: String,
        eventData: String,
        claimedAt: OffsetDateTime = OffsetDateTime.now(),
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                INSERT INTO EVENT_OUTBOX (EVENT_ID, EVENT_DATA, CLAIMED_AT, CLAIM_ID)
                VALUES (?, ?, ?, ?)
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, eventId)
                statement.setString(2, eventData)
                statement.setTimestamp(3, Timestamp.from(claimedAt.toInstant()))
                statement.setString(4, UUID.randomUUID().toString())
                statement.executeUpdate()
            }
        }
    }

    fun insertSnapshotDirect(
        entityId: String,
        stateData: String,
        sequenceNumber: Long,
    ) {
        createJdbcConnection().use { connection ->
            val sql = """
                INSERT INTO SNAPSHOTS (ENTITY_ID, SEQUENCE_NUMBER, STATE_DATA, TIMESTAMP)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, entityId)
                statement.setLong(2, sequenceNumber)
                statement.setString(3, stateData)
                statement.executeUpdate()
            }
        }
    }

    suspend fun lockRecordsThen(block: suspend () -> Unit) {
        createJdbcConnection().apply { autoCommit = false }.also {
            val sql = """
                SELECT * FROM EVENT_OUTBOX FOR UPDATE
            """
            it.prepareStatement(sql).use { statement ->
                statement.execute()
            }
            block()
            it.commit()
        }
    }
}
