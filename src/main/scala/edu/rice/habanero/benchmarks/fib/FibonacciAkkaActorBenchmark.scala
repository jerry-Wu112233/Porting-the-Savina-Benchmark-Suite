package edu.rice.habanero.benchmarks.fib


import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import edu.rice.hj.runtime.actors.Message

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    def refs: Iterable[ActorRef[Nothing]] = Seq()
  }
  sealed trait FibMessage extends Message
  final case class Request(n: Int) extends FibMessage
  final case class Response(value: Int) extends FibMessage

  def main(args: Array[String]) {
    BenchmarkRunner.runBenchmark(args, new FibonacciAkkaActorBenchmark)
  }
  def apply() : Behavior[FibMessage] = {
    Behaviors.setup(context => context.spawn(FibonacciActor(context), "Fibonacci"))
  }
  private final class FibonacciAkkaActorBenchmark extends Benchmark {
    def initialize(args: Array[String]) {
      FibonacciConfig.parseArgs(args)
    }

    def printArgInfo() {
      FibonacciConfig.printArgs()
    }

    def runIteration() {

      val system = AkkaActorState.newActorSystem("Fibonacci", FibonacciActor())

      val fjRunner = system.spawn(new FibonacciActor(null))) // spawn() is not a method for the ActorSystem
      AkkaActorState.startActor(fjRunner)
      fjRunner ! Request(FibonacciConfig.N)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(context: ActorContext[FibMessage]) extends AbstractBehavior[FibMessage](context) {
    import FibonacciAkkaActorBenchmark._
    private var result = 0
    private var respReceived = 0

    override def onMessage(msg: FibMessage): Behavior[FibMessage] = {

      msg match {
        case Request(n) =>

          if (n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)
            Behaviors.stopped

          } else {

            val f1 = context.spawn(FibonacciActor(context.self), "Actor_f1")
            f1 ! Request(n - 1)

            val f2 = context.spawn(FibonacciActor(context.self), "Actor_f2")
            f2 ! Request(n - 2)

          }
          Behaviors.same

        case Response(value) =>

          respReceived += 1
          result += value

          if (respReceived == 2) {
            processResult(Response(result))

          }
      }
    }

    private def processResult(response: Response) {
      if (parent != null) {
        parent ! response
      } else {
        println(" Result = " + result)
      }

      Behaviors.stopped
    }
  }

}
