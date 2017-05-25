package com.heller.CQRS

import java.lang.IllegalArgumentException
import kotlin.reflect.KClass

class InProcEventBus : EventBus {

    private var subscribers: Map<KClass<in Event>, MutableList<(Event) -> Unit>> = mapOf()

    override fun <E : Event> dispatch(event: E) {

        @Suppress("USELESS_CAST")
        val evtType = event.javaClass.kotlin as KClass<in E>

        val containsKey = subscribers.containsKey(evtType)

        if (!containsKey) return

        subscribers.get(evtType)!!.forEach { it.invoke(event) }

    }

    override fun <E : Event> subscribe(evtType: KClass<in E>, handler: (E) -> Unit) {

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

    private var handlers: Map<KClass<in Command>, (Command) -> Command.Result> = mapOf()

    override fun fireAndForget(cmd: Command) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <C : Command.WithAggregateId<R>, R : Command.Result> ask(cmd: C): R {

        @Suppress("USELESS_CAST")
        val cmdType = cmd.javaClass.kotlin as KClass<in C>

        val containsHandler = handlers.containsKey(cmdType)
        if (!containsHandler) throw IllegalArgumentException()

        @Suppress("UNCHECKED_CAST")
        return handlers[cmdType]!!.invoke(cmd) as R


    }

    override fun <C : Command.WithAggregateId<R>, R : Command.Result> subscribe(cmdType: KClass<in C>, handler: (C) -> R) {


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
