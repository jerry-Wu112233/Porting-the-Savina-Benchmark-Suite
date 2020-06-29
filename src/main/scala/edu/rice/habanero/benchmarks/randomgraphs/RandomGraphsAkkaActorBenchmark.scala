package edu.rice.habanero.benchmarks

import java.util

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaMsg}
import gc.Message


object RandomGraphsAkkaActorBenchmark {

  sealed trait RandomGraphsMsg
  final case class Link(ref: ActorRef[RandomGraphsMsg])
  final case class Ping()

  object BenchmarkActor {
    def apply(): Behavior[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setup(context => new BenchmarkActor(context))
    }
  }

  private class BenchmarkActor(context: ActorContext[AkkaMsg[RandomGraphsMsg]])
    extends AkkaActor[RandomGraphsMsg](context) {

    /** a list of references to other actors */
    private var acquaintances: Set[ActorRef[RandomGraphsMsg]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      val child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawn(BenchmarkActor(), "new Actor")
      acquaintances += child
    }

    def forgetActor(ref: ActorRef[RandomGraphsMsg]): Unit = {
      acquaintances -= ref
    }

    def linkActors(owner: ActorRef[RandomGraphsMsg], target: ActorRef[RandomGraphsMsg]): Unit = {
      owner ! Link(target)
    }

    def ping(ref: ActorRef[RandomGraphsMsg]): Unit = {
      ref ! Ping()
    }

    override def process(msg: RandomGraphsMsg): Behavior[AkkaMsg[RandomGraphsMsg]] = {
      msg match {
        case Link(ref) =>
          acquaintances += ref
          Behaviors.same

        case Ping() =>
          Behaviors.same
      }
    }


  }
}
