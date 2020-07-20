package edu.rice.habanero.benchmarks.randomgraphs

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, AkkaMsg, BenchmarkMessage}
import gc.Message

import scala.util.Random


object RandomGraphsAkkaActorBenchmark {

  def main(args: Array[String]): Unit = {
    BenchmarkRunner.runBenchmark(args, new RandomGraphsAkkaActorBenchmark)
  }

  sealed trait RandomGraphsMsg extends Message

  final case class Link(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]) extends RandomGraphsMsg {
    override def refs = Seq()
  }

  final case class Ping() extends RandomGraphsMsg {
    def refs = Seq()
  }

  private final class Statistics {
    val latch        = new CountDownLatch(RandomGraphsParam.NumberOfSpawns)
    val linkCount    = new AtomicInteger()
    val releaseCount = new AtomicInteger()
    val pingCount    = new AtomicInteger()
    val noopCount    = new AtomicInteger()

    override def toString: String =
      s"""
         |Number of actors created:  ${RandomGraphsParam.NumberOfSpawns}
         |Number of links created:   ${linkCount.get()}
         |Number of links released:  ${releaseCount.get()}
         |Number of pings sent:      ${pingCount.get()}
         |Number of noops performed: ${noopCount.get()}
         |Total actions: ${linkCount.get() + releaseCount.get() + pingCount.get() + noopCount.get()}
         |""".stripMargin
  }

  private final class RandomGraphsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]): Unit = {

    }

    def printArgInfo(): Unit = {
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double): Unit = {
      Thread.sleep(5000) // Give the actor system time to shut down
    }


    def runIteration(): Unit = {
      val stats = new Statistics

      val system = AkkaActorState.newActorSystem("RandomGraphs", BenchmarkActor(stats))

      for (x <- 1 to RandomGraphsParam.NumberOfPingsSent) {
        system ! BenchmarkMessage(Ping())
      }
      try {
        stats.latch.await()
        system.terminate()
        println(stats)
      } catch {
        case ex: InterruptedException =>
          ex.printStackTrace()
      }
    }

  }

  object BenchmarkActor {
    def apply(statistics: Statistics): Behavior[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setup(context => new BenchmarkActor(context, statistics))
    }
  }

  private class BenchmarkActor(context: ActorContext[AkkaMsg[RandomGraphsMsg]], statistics: Statistics)
    extends AkkaActor[RandomGraphsMsg](context) {

    /** a list of references to other actors */
    private var acquaintances: Set[ActorRef[AkkaMsg[RandomGraphsMsg]]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      val child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawnAnonymous(BenchmarkActor(statistics))
      statistics.latch.countDown()
      acquaintances += child
    }

    def forgetActor(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      //statistics.releaseCount.incrementAndGet()
      acquaintances -= ref
    }

    def linkActors(owner: ActorRef[AkkaMsg[RandomGraphsMsg]], target: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      //statistics.linkCount.incrementAndGet()
      owner ! BenchmarkMessage(Link(target))
    }

    def ping(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      //statistics.pingCount.incrementAndGet()
      ref ! BenchmarkMessage(Ping())
    }

    def doSomeActions(): Unit = {
      /** generates a list size of M of random doubles between 0.0 to 1.0 */
      val probabilities: List[Double] = List.fill(RandomGraphsParam.NumberOfActions)(scala.util.Random.nextDouble())
      import RandomGraphsParam._

      for (r <- probabilities) {
        if (r < RandomGraphsParam.ProbabilityToSpawn) {
          spawnActor()
        }
        else if (r < ProbabilityToSpawn + ProbabilityToSendRef && acquaintances.nonEmpty) {
          linkActors(randomItem(acquaintances), randomItem(acquaintances))
        }
        else if (r < ProbabilityToSpawn + ProbabilityToSendRef + ProbabilityToReleaseRef && acquaintances.nonEmpty) {
          forgetActor(randomItem(acquaintances))
        }
        else if (r < ProbabilityToSpawn + ProbabilityToSendRef + ProbabilityToReleaseRef + ProbabilityToPing && acquaintances.nonEmpty) {
          ping(randomItem(acquaintances))
        }
        else {
          //statistics.noopCount.incrementAndGet()
        }
      }
    }

    def randomItem(items: Set[ActorRef[AkkaMsg[RandomGraphsMsg]]]): ActorRef[AkkaMsg[RandomGraphsMsg]] = {
      if (items.nonEmpty) {
        val i = Random.nextInt(items.size)
        items.view.slice(i, i + 1).head
      }
      else {
        null
      }
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
