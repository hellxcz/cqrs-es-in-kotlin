package com.heller.CQRS

//import com.heller.CQRS.EventSourcingRepository.Companion.applyEvent
import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

class InProcEventBus : EventBus {

    private var subscribers: Map<KClass<out Event>, MutableList<(Event) -> Unit>> = mapOf()

    override fun <E : Event> dispatch(event: E) {

        val evtType = event.javaClass.kotlin

        val containsKey = subscribers.containsKey(evtType)

        if (!containsKey) return

        subscribers.get(evtType)!!.forEach { it.invoke(event) }

    }

    override fun <E : Event> subscribe(evtType: KClass<E>, handler: (E) -> Unit) {

        val containsKey = subscribers.containsKey(evtType)

        @Suppress("UNCHECKED_CAST")
        handler as (Event) -> Unit

        @Suppress("UNCHECKED_CAST")
        evtType as KClass<Event>

        if (!containsKey) {
            subscribers = subscribers.plus(Pair(evtType, mutableListOf(handler)))
        } else {
            subscribers[evtType]!!.add(handler)
        }

    }

}

class InProcCommandBus : CommandBus {

    private var handlers: Map<KClass<out Command>, (Command) -> Command.Result> = mapOf()

    override fun fireAndForget(cmd: Command) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <C : Command.WithResult<R>, R : Command.Result> ask(cmd: C): R {

        val cmdType = cmd.javaClass.kotlin

        val containsHandler = handlers.containsKey(cmdType)
        if (!containsHandler) throw IllegalArgumentException()

        @Suppress("UNCHECKED_CAST")
        return handlers[cmdType]!!.invoke(cmd) as R
    }

    override fun <C : Command.Creational<E>, E : Event> subscribeCreational(cmdType: KClass<C>, handler: (C) -> Command.Creational.Result) {

        val containsHandler = handlers.containsKey(cmdType)

        @Suppress("UNCHECKED_CAST")
        cmdType as KClass<Command>

        @Suppress("UNCHECKED_CAST")
        handler as (Command) -> Command.Result

        if (containsHandler) {

            handlers = handlers
                    .minus(cmdType)
                    .plus(Pair<KClass<Command>, (Command) -> Command.Result>(cmdType, handler))

        } else {

            handlers = handlers.plus(Pair<KClass<Command>, (Command) -> Command.Result>(cmdType, handler))

        }
    }

    override fun <C : Command.WithAggregateId<R>, R : Command.Result> subscribe(cmdType: KClass<C>, handler: (C) -> R) {

        val containsHandler = handlers.containsKey(cmdType)

        @Suppress("UNCHECKED_CAST")
        handler as (Command) -> Command.Result

        @Suppress("UNCHECKED_CAST")
        cmdType as KClass<Command>

        if (containsHandler) {

            handlers = handlers
                    .minus(cmdType)
                    .plus(Pair<KClass<Command>, (Command) -> Command.Result>(cmdType, handler))

        } else {

            handlers = handlers.plus(Pair<KClass<Command>, (Command) -> Command.Result>(cmdType, handler))

        }

    }


}

class InProcEventSourcingRepository : EventSourcingRepository {

    data class AggregateIdentity(val aggregateType: KClass<out Aggregate>, val id: String)


    private var eventsStore: Map<AggregateIdentity, List<PersistentEvent<out Aggregate, out Event>>> = mapOf()

    override fun <A : Aggregate, E : Event> persist(pEvt: PersistentEvent<A, E>) {

        val identity = AggregateIdentity(
                aggregateType = pEvt.aggregateType,
                id = pEvt.aggregateId
        )

        if (!eventsStore.containsKey(identity)) {

            val _events = listOf(pEvt)

            eventsStore = eventsStore.plus(Pair(identity, _events))

        } else {

            val _events = eventsStore[identity]!!

            eventsStore = eventsStore.minus(identity)
                    .plus(Pair(identity, _events.plus(pEvt)))

        }

    }

    override fun <A : Aggregate> load(aggregateType: KClass<A>, id: String): A? {

        val identity = AggregateIdentity(
                aggregateType = aggregateType,
                id = id
        )

        if (!eventsStore.containsKey(identity)) {
            return null
        }

        val constructor = aggregateType.java.declaredConstructors.first { it.parameters.isEmpty() }

        val aggregate = constructor.newInstance() as A

        eventsStore[identity]!!.forEach { EventSourcingRepository.applyEvent(it.event, aggregate) }

        return aggregate

    }

    override fun <A : Aggregate> getLastEventSequenceId(aggregateType: KClass<A>, id: String): Int {
        return 3
    }

}
