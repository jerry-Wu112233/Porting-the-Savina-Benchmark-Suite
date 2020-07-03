package edu.rice.habanero.benchmarks

import java.util.concurrent.CountDownLatch

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, AkkaMsg, BenchmarkMessage}
import gc.Message

import scala.util.Random


object RandomGraphsAkkaActorBenchmark {


  sealed trait RandomGraphsMsg extends Message

  final case class Link(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]) extends RandomGraphsMsg {
    override def refs = Seq()
  }

  final case class Ping() extends RandomGraphsMsg {
    def refs = Seq()
  }

  private final class RandomGraphsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]): Unit = {

    }

    def printArgInfo(): Unit = {
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }


    def runIteration(): Unit = {
      var countdownLatch: CountDownLatch = new CountDownLatch(RandomGraphsParam.NumberOfSpawns)

      val system = AkkaActorState.newActorSystem("RandomGraphs", BenchmarkActor(countdownLatch))

      for (x <- 1 to RandomGraphsParam.NumberOfPingsSent) {
        system ! BenchmarkMessage(Ping())
      }
      countdownLatch.await()
    }

  }

  object BenchmarkActor {
    def apply(latch: CountDownLatch): Behavior[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setup(context => new BenchmarkActor(context, latch))
    }
  }

  private class BenchmarkActor(context: ActorContext[AkkaMsg[RandomGraphsMsg]], latch: CountDownLatch)
    extends AkkaActor[RandomGraphsMsg](context) {

    /** a list of references to other actors */
    private var acquaintances: Set[ActorRef[AkkaMsg[RandomGraphsMsg]]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      val child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawn(BenchmarkActor(latch), "new Actor")
      latch.countDown()
      acquaintances += child
    }

    def forgetActor(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      acquaintances -= ref
    }

    def linkActors(owner: ActorRef[AkkaMsg[RandomGraphsMsg]], target: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      owner ! BenchmarkMessage(Link(target))
    }

    def ping(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      ref ! BenchmarkMessage(Ping())
    }

    def doSomeActions(): Unit = {
      /** generates a list size of M of random doubles between 0.0 to 1.0 */
      val probabilities: List[Double] = List.fill(RandomGraphsParam.NumberOfActions)(scala.util.Random.nextDouble())
      import RandomGraphsParam._
      val acquaintancesIsEmpty: Boolean = acquaintances.isEmpty

      for (r <- probabilities) {
        if (r < RandomGraphsParam.ProbabilityToSpawn) {
          spawnActor()
        } else if ((r < ProbabilityToSpawn + ProbabilityToSendRef) && !acquaintancesIsEmpty) {
          linkActors(randomItem(acquaintances), randomItem(acquaintances))
        } else if (r < ProbabilityToSpawn + ProbabilityToSendRef + ProbabilityToReleaseRef && !acquaintancesIsEmpty) {
          forgetActor(randomItem(acquaintances))
        } else if (r < ProbabilityToSpawn + ProbabilityToSendRef + ProbabilityToReleaseRef + ProbabilityToPing && !acquaintancesIsEmpty) {
          ping(randomItem(acquaintances))
        }
      }
    }

    def randomItem(items: Set[ActorRef[AkkaMsg[RandomGraphsMsg]]]): ActorRef[AkkaMsg[RandomGraphsMsg]] = {
      val i = Random.nextInt(items.size)
      items.view.slice(i, i + 1).head
    }

    override def process(msg: RandomGraphsMsg): Behavior[AkkaMsg[RandomGraphsMsg]] = {
      msg match {
        case Link(ref) =>
          acquaintances += ref
          doSomeActions()
          Behaviors.same

        case Ping() =>
          doSomeActions()
          Behaviors.same
      }
    }


  }
}
