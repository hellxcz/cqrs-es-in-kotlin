package com.heller.CQRS

import org.junit.Assert
import org.junit.Test
import java.util.*
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

    data class DummyCommandFinished(val data: String) : Event

    data class DummyCreateCommand(val some: String) : Command.Creational<DummyCreateCommandFinished>

    data class DummyCreateCommandFinished(val aggregateId: String, val some: String) : Event

    class DummyAggregate
     : Aggregate {

        @Aggregate.CreateCommandHandler constructor(cmd: DummyCreateCommand){
            val evt = DummyCreateCommandFinished(
                    aggregateId = UUID.randomUUID().toString(),
                    some = cmd.some)

            EventSourcingRepository.apply(evt, this)
        }

        private constructor(){}

        @Aggregate.AggregateId
        lateinit var id: String

        private lateinit var some: String

        @Aggregate.EventHandler
        protected fun on(evt: DummyCreateCommandFinished) {

            this.id = evt.aggregateId
            this.some = evt.some
        }

        @Aggregate.CommandHandler
        fun handle(cmd: DummyCommand): DummyCommand.DummyResult {

            EventSourcingRepository.apply(DummyCommandFinished("some"), this)

            return DummyCommand.DummyResult(cmd.aggregateId)

        }

        @Aggregate.EventHandler
        protected fun on(evt: DummyCommandFinished) {


        }


    }

    @Test
    fun eventRepositoryApply() {

        val aggregate = DummyAggregate(DummyCreateCommand("one"));


    }

}

class AggregateSupportServiceTest{

    class DummyAggregate : Aggregate{

        var someValue: String = "before"

        @Aggregate.AggregateId
        lateinit var id: String

        constructor()

        @Aggregate.CreateCommandHandler
        constructor(cmd: DummyCreateCommand){
            EventSourcingRepository.apply(
                    DummyCreatedEvent("after"), this
            )
        }

        @Aggregate.EventHandler
        fun on(evt: DummyCreatedEvent){
            someValue = evt.data

            id = "1"
        }

        @Aggregate.CommandHandler
        fun handle(cmd: DummyCommand): DummyCommand.Result{

            return DummyCommand.Result(id)

        }

    }

    data class DummyCreateCommand(val data: String) : Command.Creational<DummyCreatedEvent>

    data class DummyCreatedEvent(val data: String) : Event

    data class DummyCommand(@Command.AggregateId val id: String) : Command.WithResult<DummyCommand.Result>{

        data class Result(val id: String) : Command.Result

    }

    @Test
    fun shouldBeAbleToRegisterAggregate(){

        val eventSourcingReposiory = InProcEventSourcingRepository()

        EventSourcingRepository.eventSourcingRepository = eventSourcingReposiory

        val commandBus=InProcCommandBus()

        AggregateSupportService.register(
                commandBus = commandBus,

                aggregateType = DummyAggregate::class
        )

        val result = commandBus.ask(DummyCreateCommand("some data"))

    }



}

class InProcEventSourcingRepositoryTest {

    class DummyAggregate : Aggregate{

        var someValue: String = "before"

        constructor()

        @Aggregate.CreateCommandHandler
        constructor(cmd: DummyCreateCommand){
            EventSourcingRepository.apply(
                    DummyCreatedEvent("after"), this
            )
        }

        @Aggregate.EventHandler
        fun on(evt: DummyCreatedEvent){
            someValue = evt.data
        }

    }

    data class DummyCreateCommand(val data: String) : Command.Creational<DummyCreatedEvent>

    data class DummyCreatedEvent(val data: String) : Event

    @Test
    fun persist_and_load_shouldWork(){

        val testee = InProcEventSourcingRepository()

        val aggregateId = "3";

        val data = "after"

        val persistentEvent = PersistentEvent(
                aggregateId = aggregateId,
                aggregateType = DummyAggregate::class,
                event = DummyCreatedEvent(data),
                sequence = 0
        )

        testee.persist(persistentEvent)

        val loadedAggregate = testee.load(DummyAggregate::class, aggregateId)!!

        Assert.assertEquals(data, loadedAggregate.someValue)
    }


}