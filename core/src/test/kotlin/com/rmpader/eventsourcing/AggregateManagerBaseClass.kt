package com.rmpader.eventsourcing

open class AggregateManagerBaseClass {
    sealed interface TestCommand {
        data class CreateOrder(
            val amount: Int,
        ) : TestCommand

        data class InvalidCommand(
            val reason: String,
        ) : TestCommand
    }

    sealed interface TestEvent {
        data class OrderCreated(
            val finalAmount: Int,
            val originalAmount: Int,
            val additionalAmount: Int,
        ) : TestEvent
    }

    data class TestState(
        override val entityId: String,
        val amount: Int = 0,
    ) : AggregateEntity<TestCommand, TestEvent, TestState> {
        override fun handleCommand(command: TestCommand): TestEvent =
            when (command) {
                is TestCommand.CreateOrder ->
                    TestEvent.OrderCreated(
                        amount + command.amount,
                        amount,
                        command.amount,
                    )

                is TestCommand.InvalidCommand ->
                    throw CommandRejectionException(
                        reason = command.reason,
                        errorCode = "INVALID_COMMAND",
                    )
            }

        override fun applyEvent(event: TestEvent): TestState =
            when (event) {
                is TestEvent.OrderCreated -> copy(amount = event.finalAmount)
            }
    }
}
