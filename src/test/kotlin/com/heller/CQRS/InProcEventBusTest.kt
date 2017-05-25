package com.heller.CQRS

import org.junit.Assert
import org.junit.Test
import java.util.concurrent.CountDownLatch

class InProcEventBusTest {

    data class DummyEvent(val id: String) : Event

    @Test
    fun shoudWork() {

        val testee = InProcEventBus()

        val cdl = CountDownLatch(1)

        testee.subscribe(DummyEvent::class,
                { evt ->
                    cdl.countDown()
                })

        testee.dispatch(DummyEvent("1"))

        cdl.await()

    }

}

class InProcCommandBusTest {

    data class DummyCommand(override val aggregateId: String) : Command.WithAggregateId<DummyCommand.DummyResult> {

        data class DummyResult(val aggregateId: String) : Command.Result

    }

    @Test
    fun shouldWork() {

        val testee = InProcCommandBus()

        testee.subscribe(
                DummyCommand::class,
                {
                    cmd ->
                    DummyCommand.DummyResult(cmd.aggregateId)

                }
        )

        val result = testee.ask(DummyCommand("123"))

        Assert.assertNotNull(result)

    }

}

class AggregateTest {

    data class DummyCommand(override val aggregateId: String) : Command.WithAggregateId<DummyCommand.DummyResult> {

        data class DummyResult(val aggregateId: String) : Command.Result

    }

    data class DummyCommandFinished(val data: String): Event

    class DummyAggregate(@Aggregate.AggregateId val id: String) : Aggregate {

        @Aggregate.CommandHandler
        fun handle(cmd: DummyCommand): DummyCommand.DummyResult {

            EventSourcingRepository.apply(DummyCommandFinished("some"), this)

            return DummyCommand.DummyResult(cmd.aggregateId)

        }

        @Aggregate.EventHandler
        protected fun on(evt: DummyCommandFinished){



        }

    }

    @Test
    fun eventRepositoryApply(){

        val aggregate = DummyAggregate("aggregateId");

        aggregate.handle(DummyCommand("one"))


    }

}