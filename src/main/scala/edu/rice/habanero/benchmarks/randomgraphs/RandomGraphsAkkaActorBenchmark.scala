package edu.rice.habanero.benchmarks

import java.util
import java.util.concurrent.CountDownLatch

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaMsg}
import edu.rice.hj.runtime.actors.Message
import gc.AppMsg



object RandomGraphsAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    def refs = Seq()
  }

  sealed trait RandomGraphsMsg extends Message with NoRefsMessage
  final case class Link(ref: ActorRef[RandomGraphsMsg]) extends RandomGraphsMsg with NoRefsMessage
  final case class Ping() extends RandomGraphsMsg with NoRefsMessage
  /**
  def main(args: Array[String]): Unit = {
    BenchmarkRunner.runBenchmark((args, new RandomGraphsAkkaActorBenchmark))
  }
  */

  private final class RandomGraphsAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]): Unit = {
    }

    def printArgInfo(): Unit = {
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }


    def runIteration(): Unit = {

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
    private var acquaintances: Set[ActorRef[RandomGraphsMsg]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      val child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawn(BenchmarkActor(latch), "new Actor")
      latch.countDown()
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

    def doSomeActions(): Unit = {

      val probabilities: List[Double] = List.fill(RandomGraphsParam.constantM)(scala.util.Random.nextDouble())
      for (r <- probabilities) {
        r match {
          case (r < RandomGraphsParam.constantP1) =>
            spawnActor()
          case (r < RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2) =>
            linkActors(owner, target)
          case (r < RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2 + RandomGraphsParam.constantP3) =>
            forgetActor()
          case (r <  RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2 + RandomGraphsParam.constantP3 + RandomGraphsParam.constantP4) =>
            AppMsg
          case _ =>

        }
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
