package com.heller.CQRS


import kotlin.reflect.KClass

interface Command {

    interface Result {

    }

    interface WithResult<R : Result> : Command

    interface WithAggregateId<R : Result> : WithResult<R> {
        val aggregateId: String
    }
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
            where C : Command.WithAggregateId<R>, R : Command.Result

    fun <C, R> subscribe(cmdType: KClass<in C>, handler: (C) -> R)
            where C : Command.WithAggregateId<R>, R : Command.Result

}

interface EventBus {

    fun <E : Event> dispatch(event: E)
    fun <E : Event> subscribe(evtType: KClass<in E>, handler: (E) -> Unit)

}


interface ApplicationService

interface EventSourcingRepository {

    companion object {

        lateinit var eventSourcingRepository: EventSourcingRepository

        fun <E : Event, A : Aggregate> apply(e: E, aggregate: A) {

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
            val lastEventSequenceId = eventSourcingRepository.getLastEventSequenceId(aggregateKotlinType, aggregateId)

            val persistentEvent = PersistentEvent(
                    aggregateId = aggregateId,
                    aggregateType = aggregateKotlinType,
                    event = e,
                    sequence = lastEventSequenceId
            )

            eventSourcingRepository.persist(persistentEvent)

        }

        protected fun <A : Aggregate, E : Event> applyEvent( e: E, aggregate: A) {

            val aggregateJavaType = aggregate.javaClass

            val eventType = e.javaClass;

            val eventHandler = aggregateJavaType.declaredMethods
                    .filter { it.getAnnotation(Aggregate.EventHandler::class.java) != null }
                    .find { it.parameterTypes.size == 1 && it.parameterTypes[0]!! == eventType }
                    ?: throw IllegalArgumentException("Event handler is not defined") // was not found

            eventHandler.invoke(aggregate, e)
        }

    }

    fun <A, E> persist(pEvt: PersistentEvent<A, E>)
            where
            A : Aggregate,
            E : Event

    fun <A> load(aggregateType: KClass<A>, id: String)
            where A : Aggregate

    fun <A> getLastEventSequenceId(aggregateType: KClass<A>, id: String): Int
            where A : Aggregate

}

interface Aggregate {

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class CommandHandler

    @Target(AnnotationTarget.FUNCTION)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class EventHandler

    @Target(AnnotationTarget.FIELD)
    @Retention(AnnotationRetention.RUNTIME)
    annotation class AggregateId

    interface Data {

    }

}
