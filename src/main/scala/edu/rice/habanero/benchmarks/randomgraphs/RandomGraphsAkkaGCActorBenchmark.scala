package edu.rice.habanero.benchmarks



import java.util.concurrent.CountDownLatch

import akka.actor.typed.{Behavior => AkkaBehavior}
import edu.rice.habanero.actors.{AkkaActorState, AkkaGCActor, AkkaMsg}
import gc._

import scala.util.Random


object RandomGraphsAkkaGCActorBenchmark {


  sealed trait RandomGraphsMsg extends Message

  final case class Link(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]) extends RandomGraphsMsg {
    def refs = Seq(ref)
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
      var countdownLatch: CountDownLatch = new CountDownLatch(RandomGraphsParam.constantN)

      val system = AkkaActorState.newActorSystem("RandomGraphs", BenchmarkActor.createRoot(countdownLatch))

      AkkaActorState.startActor(system)
      var x = 0
      for (x <- 1 to RandomGraphsParam.constantP) {
        system ! Ping()
      }
    }

  }

  object BenchmarkActor {
    def apply(latch: CountDownLatch): ActorFactory[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setup(context => new BenchmarkActor(context, latch))
    }

    def createRoot(latch: CountDownLatch): AkkaBehavior[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setupReceptionist(context => new BenchmarkActor(context, latch))
    }
  }

  private class BenchmarkActor(context: ActorContext[AkkaMsg[RandomGraphsMsg]], latch: CountDownLatch)
    extends AkkaGCActor[RandomGraphsMsg](context) {

    /** a list of references to other actors */
    private var acquaintances: Set[ActorRef[AkkaMsg[RandomGraphsMsg]]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      var child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawn(BenchmarkActor(latch), "new Actor")
      latch.countDown()
      acquaintances += child

    }

    def forgetActor(ref: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      context.release(ref)
      acquaintances -= ref
    }

    def linkActors(owner: ActorRef[AkkaMsg[RandomGraphsMsg]], target: ActorRef[AkkaMsg[RandomGraphsMsg]]): Unit = {
      owner ! Link(context.createRef(target, owner))

    }

    def ping(ref: ActorRef[RandomGraphsMsg]): Unit = {
      ref ! Ping()
    }

    def doSomeActions(): Unit = {
      /** generates a list size of M of random doubles between 0.0 to 1.0 */
      val probabilities: List[Double] = List.fill(RandomGraphsParam.constantM)(scala.util.Random.nextDouble())
      for (r <- probabilities) {
        r match {
          case (r < RandomGraphsParam.constantP1) =>
            spawnActor()
          case (r < RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2) =>
            linkActors(randomItem(acquaintances), randomItem(acquaintances))
          case (r < RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2 + RandomGraphsParam.constantP3) =>
            forgetActor(randomItem(acquaintances))
          case (r <  RandomGraphsParam.constantP1 + RandomGraphsParam.constantP2 + RandomGraphsParam.constantP3 + RandomGraphsParam.constantP4) =>
            randomItem(acquaintances) ! AppMsg(ping())
          case _ =>

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
          this

        case Ping() =>
          doSomeActions()
          this
      }
    }


  }
}
