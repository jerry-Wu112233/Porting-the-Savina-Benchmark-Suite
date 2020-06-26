package edu.rice.habanero.benchmarks

object RandomGraphsParam {
  /** M is a configurable fixed parameter that decides the amount of action an Actor will perform */
  val constantM : Int = 40
  /** N is a configurable fixed parameter that decides the amount of Actors will be generated */
  val constantN : Int = 10
  /** P1 is the probability for Action 1: Spawning an Actor */
  val constantP1 : Double = 0.2
  /** P2 is the probability for Action 2: Sending a ref from one Actor to another*/
  val constantP2 : Double = 0.2
  /** P3 is the probability for Action 3: Releasing refereces to Actors */
  val constantP3 : Double = 0.2
  /** P4 is the probability for Action 4: Sending Application Messages to Actors */
  val constantP4 : Double = 0.2

}
