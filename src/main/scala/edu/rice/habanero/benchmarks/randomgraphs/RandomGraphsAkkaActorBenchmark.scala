package edu.rice.habanero.benchmarks

import java.util

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaMsg}
import edu.rice.hj.runtime.actors.Message


/**
 * To evaluate the performance of our GC, we need a benchmark that randomly spawns lots of actors and creates lots of actor garbage.
 * The first step is to create a BenchmarkActor behavior with the following API:
 *
 * Private fields:
 *
 * acquaintances is a list of references to other actors
 * Message API:
 *
 * Link(ref: ActorRef): add ref to this.acquaintances
 * Ping(): do nothing
 * Methods:
 *
 * spawnActor() spawns a BenchmarkActor and adds the resulting reference to this.acquaintances
 * forgetActor(ref: ActorRef) removes ref from this.acquaintances
 * linkActors(owner: ActorRef, target: ActorRef) sends a Link message to owner, giving it a reference to target
 * ping(ref: ActorRef) sends an empty Ping message to ref.
 * We need to create a benchmark actor for both akka-gc and regular akka. Adding randomness will be a separate issue.
 */
object RandomGraphsAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    def refs = Seq()
  }

  sealed trait RandomGraphsMsg extends Message with NoRefsMessage
  final case class Link(ref: ActorRef[RandomGraphsMsg]) extends RandomGraphsMsg with NoRefsMessage
  final case class Ping() extends RandomGraphsMsg with NoRefsMessage

  def main(args: Array[String]): Unit = {
    BenchmarkRunner.runBenchmark((args, new RandomGraphsAkkaActorBenchmark))
  }

  private final class RandomGraphsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]): Unit = {
    }

  }

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
      val child: ActorRef[RandomGraphsMsg] = context.spawn(BenchmarkActor(context.self), "new Actor")
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
