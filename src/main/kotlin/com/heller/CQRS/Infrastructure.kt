package com.heller.CQRS


import kotlin.reflect.KClass

interface Command {

    interface Result {

    }


    interface WithResult<R : Result> : Command

    interface Creational<E : Event> : WithResult<Creational.Result> {
        data class Result(val id: String) : Command.Result // TODO - should contain newly created ID
    }

    interface WithAggregateId<R : Result> : WithResult<R> {
        val aggregateId: String
    }

    @Target(AnnotationTarget.FIELD)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class AggregateId
}

data class PersistentEvent<A, E>(
        val aggregateId: String,
        val aggregateType: KClass<A>,
        val sequence: Int,
        val event: E
) where
A : Aggregate,
E : Event


interface Event {

}

interface CommandBus {

    fun fireAndForget(cmd: Command)
    fun <C, R> ask(cmd: C): R
            where C : Command.WithResult<R>, R : Command.Result

    fun <C, R> subscribe(cmdType: KClass<C>, handler: (C) -> R)
            where C : Command.WithAggregateId<R>, R : Command.Result

    fun <C, E> subscribeCreational(cmdType: KClass<C>, handler: (C) -> Command.Creational.Result)
            where C : Command.Creational<E>, E : Event
}

interface EventBus {

    fun <E : Event> dispatch(event: E)
    fun <E : Event> subscribe(evtType: KClass<E>, handler: (E) -> Unit)

}


interface ApplicationService

interface AggregateSupportService {

    companion object {

        fun <A> register(commandBus: CommandBus, aggregateType: KClass<A>)
                where A : Aggregate {

            aggregateType.java.declaredConstructors
                    .filter {

                        it.getAnnotation(Aggregate.CreateCommandHandler::class.java) != null

                    }
                    .forEach {

                        val constructor = it

                        val commandType = constructor.parameterTypes.get(0).kotlin as KClass<Command.Creational<Event>>

                        val aggregateId = aggregateType.java.declaredFields.find { it.getAnnotation(Aggregate.AggregateId::class.java) != null }

                        commandBus.subscribeCreational(commandType, {
                            cmd ->

                            val newInstance = constructor.newInstance(cmd) as A

                            val aggregateId = aggregateId!!.get(newInstance) as String

                            Command.Creational.Result(aggregateId)

                        })
                    }

            aggregateType.java.declaredMethods
                    .filter {
                        it.getAnnotation(Aggregate.CommandHandler::class.java) != null
                    }
                    .forEach {

                        val method = it

                        val commandType = method.parameterTypes.get(0).kotlin as KClass<Command.WithAggregateId<Command.Result>>

                        commandBus.subscribe(
                                commandType, {

                            cmd ->

                            val aggregate = EventSourcingRepository.eventSourcingRepository.load(
                                    aggregateType = aggregateType,
                                    id = cmd.aggregateId
                            )
                            method.invoke(aggregate, cmd) as Command.Result

                        })
                    }
        }

    }

}

interface EventSourcingRepository {

    companion object {

        lateinit var eventSourcingRepository: EventSourcingRepository

        fun apply(e: Event, aggregate: Aggregate) {

            // apply event on instance - if fails, its bad :D
            // find method using reflection and apply event

            val aggregateJavaType = aggregate.javaClass

            val aggregateKotlinType = aggregateJavaType.kotlin

            applyEvent(e, aggregate)

            // pick aggregate id

            val aggregateId = (aggregateJavaType.declaredFields
                    .find { it.getAnnotation(Aggregate.AggregateId::class.java) != null }
                    ?: throw IllegalArgumentException("AggregateId is not defined"))
                    .get(aggregate) as String

            // try to persist event
            // persistingEvent contains technical data about
            // - sequence, aggregateId, aggregateType

            // find applied sequence id on given aggregate
            val lastEventSequenceId = 1 + eventSourcingRepository.getLastEventSequenceId(aggregateKotlinType, aggregateId)

            val persistentEvent = PersistentEvent(
                    aggregateId = aggregateId,
                    aggregateType = aggregateKotlinType,
                    event = e,
                    sequence = lastEventSequenceId
            )

            eventSourcingRepository.persist(persistentEvent)

        }

        fun applyEvent(e: Event, aggregate: Aggregate) {

            val aggregateJavaType = aggregate.javaClass

            val eventType = e.javaClass;

            val eventHandler = aggregateJavaType.declaredMethods
                    .filter { it.getAnnotation(Aggregate.EventHandler::class.java) != null }
                    .find { it.parameterTypes.size == 1 && it.parameterTypes[0]!! == eventType }
                    ?: throw IllegalArgumentException("Event handler is not defined") // was not found

            eventHandler.invoke(aggregate, e)
        }

    }

    fun <A : Aggregate, E : Event> persist(pEvt: PersistentEvent<A, E>)

    fun <A> load(aggregateType: KClass<A>, id: String): A?
            where A : Aggregate

    fun <A> getLastEventSequenceId(aggregateType: KClass<A>, id: String): Int
            where A : Aggregate

}

open class Aggregate {

    @Target(AnnotationTarget.CONSTRUCTOR)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class CreateCommandHandler

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class CommandHandler

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class EventHandler

    @Target(AnnotationTarget.FIELD)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class AggregateId

    var lastSequenceId: Int = 0

    interface Data {

    }

}
