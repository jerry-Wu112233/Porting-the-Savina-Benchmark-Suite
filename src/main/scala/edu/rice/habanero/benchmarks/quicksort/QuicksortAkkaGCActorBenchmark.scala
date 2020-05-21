package edu.rice.habanero.benchmarks.quicksort

import java.util

import gc.{ActorContext, ActorFactory, ActorRef, Behavior, Behaviors, Message}
import akka.actor.typed.{Behavior => AkkaBehavior}
import edu.rice.habanero.actors.AkkaImplicits._
import edu.rice.habanero.actors.{AkkaActorState, AkkaGCActor, AkkaMsg}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}


/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object QuicksortAkkaGCActorBenchmark {

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new QuickSortAkkaActorBenchmark)
  }

  private final class QuickSortAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      QuickSortConfig.parseArgs(args)
    }

    def printArgInfo() {
      QuickSortConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("QuickSort", QuicksortGCActor.createRoot())

      val input = QuickSortConfig.randomlyInitArray()


      AkkaActorState.startActor(system)
      system ! SortMessage(None, input)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }
  object QuicksortGCActor {
    def createRoot(): AkkaBehavior[AkkaMsg[QuicksortMsg]] = {
      Behaviors.setupReceptionist(context => new QuicksortGCActor(context, null))
    }
    def apply(position: Position): ActorFactory[AkkaMsg[QuicksortMsg]] = {
      Behaviors.setup(context => new QuicksortGCActor(context, position))
    }
  }
  trait NoRefsMessage extends Message {
    def refs = Seq()
  }
  sealed trait QuicksortMsg extends Message
  abstract class Position
  final case object PositionRight extends Position
  final case object PositionLeft extends Position
  final case object PositionInitial extends Position
  final case class SortMessage(parent: Option[ActorRef[AkkaMsg[ResultMessage]]],
                               data: java.util.List[java.lang.Long])
      extends QuicksortMsg {
    def refs: Iterable[ActorRef[AkkaMsg[ResultMessage]]] = parent.toList
  }
  final case class ResultMessage(data: java.util.List[java.lang.Long], position: Position) extends QuicksortMsg with NoRefsMessage


  private class QuicksortGCActor(context: ActorContext[AkkaMsg[QuicksortMsg]], positionRelativeToParent: Position)
    extends AkkaGCActor[QuicksortMsg](context) {

    private var result: java.util.List[java.lang.Long] = _
    private var numFragments = 0
    private var parent: Option[ActorRef[AkkaMsg[ResultMessage]]] = None

    def notifyParentAndTerminate(): Behavior[AkkaMsg[QuicksortMsg]] = {

      if (positionRelativeToParent eq PositionInitial) {
        QuickSortConfig.checkSorted(result)
      }

      parent match {
        case None =>
        case Some(actor) =>
          actor ! ResultMessage(result, positionRelativeToParent)
          context.release(actor)
      }
      exit()
    }

    override def process(msg: QuicksortMsg): Behavior[AkkaMsg[QuicksortMsg]] = {

      msg match {
        case SortMessage(parent, data) =>
          this.parent = parent

          val dataLength: Int = data.size()
          if (dataLength < QuickSortConfig.T) {

            result = QuickSortConfig.quicksortSeq(data)
            notifyParentAndTerminate()
            this

          } else {

            val dataLengthHalf = dataLength / 2
            val pivot = data.get(dataLengthHalf)

            val leftUnsorted = QuickSortConfig.filterLessThan(data, pivot)
            val leftActor = context.spawn(QuicksortGCActor(PositionLeft), "Actor_Q1")
            AkkaActorState.startActor(leftActor)
            val self1 = context.createRef(context.self, leftActor)
            leftActor ! SortMessage(Some(self1), leftUnsorted)

            val rightUnsorted = QuickSortConfig.filterGreaterThan(data, pivot)
            val rightActor = context.spawn(QuicksortGCActor(PositionRight), "Actor_Q2")
            AkkaActorState.startActor(rightActor)
            val self2 = context.createRef(context.self, rightActor)
            rightActor ! SortMessage(Some(self2), rightUnsorted)

            result = QuickSortConfig.filterEqualsTo(data, pivot)
            numFragments += 1

            this

          }

        case ResultMessage(data, position) =>

          if (!data.isEmpty) {
            if (position eq PositionLeft) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(data)
              temp.addAll(result)
              result = temp

            } else if (position eq PositionRight) {
              val temp = new util.ArrayList[java.lang.Long]()
              temp.addAll(result)
              temp.addAll(data)
              result = temp
            }
          }

          numFragments += 1

          if (numFragments == 3) {
            notifyParentAndTerminate()
          }
          this
      }
    }
  }

}
