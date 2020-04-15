package edu.rice.habanero.benchmarks.fib



import gc.{AbstractBehavior, ActorContext, ActorFactory, ActorRef, Behavior, Behaviors, Message}
import edu.rice.habanero.actors.{AkkaActor, AkkaActorState}
import edu.rice.habanero.benchmarks.{Benchmark, BenchmarkRunner}
import akka.actor.typed.{Behavior => AkkaBehavior}

/**
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
object FibonacciAkkaActorBenchmark {

  trait NoRefsMessage extends Message {
    override def refs: Iterable[ActorRef[Nothing]] = Seq()
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

      val system = AkkaActorState.newActorSystem("Fibonacci", FibonacciActor.createRoot(null))

      AkkaActorState.startActor(system)
      system ! Request(FibonacciConfig.N)

      AkkaActorState.awaitTermination(system)
    }

    def cleanupIteration(lastIteration: Boolean, execTimeMillis: Double) {
    }
  }
  object FibonacciActor {
    def createRoot(parent: ActorRef[FibMessage]): AkkaBehavior[Any] = {
      Behaviors.setupReceptionist[Any](context => new FibonacciActor(context, parent))
    }
    def apply(parent: ActorRef[FibMessage]): ActorFactory[Any] = {
      Behaviors.setup[Any]((context : ActorContext[Any]) => new FibonacciActor(context, parent))
    }

  }

  private val RESPONSE_ONE = Response(1)


  private class FibonacciActor(context: ActorContext[Any], parent : ActorRef[FibMessage]) extends AkkaActor[FibMessage](context) {

    private var result = 0
    private var respReceived = 0

    override def process(msg: FibMessage): Unit = {

      msg match {
        case Request(n) =>

          if (n <= 2) {

            result = 1
            processResult(RESPONSE_ONE)


          } else {

            val f1 = context.spawn(FibonacciActor(context.self), "Actor_f1")
            f1 ! Request(n - 1)

            val f2 = context.spawn(FibonacciActor(context.self), "Actor_f2")
            f2 ! Request(n - 2)

          }


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
      exit()
    }
  }

}
