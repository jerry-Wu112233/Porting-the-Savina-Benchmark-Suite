package edu.rice.habanero.benchmarks



import gc.{ActorContext, ActorFactory, ActorRef, Behavior, Behaviors, Message}
import edu.rice.habanero.actors.{AkkaActorState, AkkaGCActor, AkkaMsg}



object RandomGraphsAkkaGCActorBenchmark {


  sealed trait RandomGraphsMsg extends Message

  final case class Link(ref: ActorRef[RandomGraphsMsg]) extends RandomGraphsMsg {
    def refs = Seq(ref)
  }

  final case class Ping() extends RandomGraphsMsg {
    def refs = Seq()
  }


  object BenchmarkActor {
    def apply(): ActorFactory[AkkaMsg[RandomGraphsMsg]] = {
      Behaviors.setup(context => new BenchmarkActor(context))
    }

    //createRoot()
  }

  private class BenchmarkActor(context: ActorContext[AkkaMsg[RandomGraphsMsg]])
    extends AkkaGCActor[RandomGraphsMsg](context) {

    /** a list of references to other actors */
    private var acquaintances: Set[ActorRef[RandomGraphsMsg]] = Set()

    /** spawns a BenchmarkActor and adds the resulting reference to this.acquaintances */
    def spawnActor(): Unit = {
      val child: ActorRef[AkkaMsg[RandomGraphsMsg]] = context.spawn(BenchmarkActor(), "new Actor")
      acquaintances += child

    }

    def forgetActor(ref: ActorRef[RandomGraphsMsg]): Unit = {
      context.release(ref)
      acquaintances -= ref
    }

    def linkActors(owner: ActorRef[RandomGraphsMsg], target: ActorRef[RandomGraphsMsg]): Unit = {
      owner ! Link(context.createRef(target, owner))

    }

    def ping(ref: ActorRef[RandomGraphsMsg]): Unit = {
      ref ! Ping()
    }

    override def process(msg: RandomGraphsMsg): Behavior[AkkaMsg[RandomGraphsMsg]] = {

      msg match {
        case Link(ref) =>
          acquaintances += ref
          this

        case Ping() =>
          this
      }
    }


  }
}
