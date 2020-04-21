package edu.rice.habanero.benchmarks.fib

import gc.Message
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.AkkaImplicits._
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState, AkkaMsg, BenchmarkMessage}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    def refs = Seq()
  }
  sealed trait FibMessage extends Message
  final case class Request(n: Int) extends FibMessage with NoRefsMessage
  final case class Response(value: Int) extends FibMessage with NoRefsMessage

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciAkkaActorBenchmark)
  }

  private final class FibonacciAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Fibonacci", FibonacciActor(null))

      AkkaActorState.startActor(system)
      system ! BenchmarkMessage(Request(FibonacciConfig.N))

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }
  object FibonacciActor {
    def apply(parent: ActorRef[AkkaMsg[FibMessage]]): Behavior[AkkaMsg[FibMessage]] = {
      Behaviors.setup(context => new FibonacciActor(context, parent))
    }

  }

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(context: ActorContext[AkkaMsg[FibMessage]], parent : ActorRef[AkkaMsg[FibMessage]])
    extends AkkaActor[FibMessage](context) {

    private var result = 0
    private var respReceived = 0

    override def process(msg: FibMessage): Behavior[AkkaMsg[FibMessage]] = {

      msg match {
        case Request(n) =>

          if (n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)

          } else {

            val f1 = context.spawn(FibonacciActor(context.self), "Actor_f1")
            AkkaActorState.startActor(f1)
            f1 ! Request(n - 1)

            val f2 = context.spawn(FibonacciActor(context.self), "Actor_f2")
            AkkaActorState.startActor(f2)
            f2 ! Request(n - 2)

          }

          this

        case Response(value) =>

          respReceived += 1
          result += value

          if (respReceived == 2) {
            processResult(Response(result))
          }
          else {
            this
          }
      }
    }

    private def processResult(response: Response) : Behavior[AkkaMsg[FibMessage]] = {
      if (parent != null) {
        parent ! response
      } else {
        println(" Result = " + result)
      }
      exit()
    }
  }

}
