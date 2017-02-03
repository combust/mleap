package ml.combust.mleap.serving

import akka.actor.ActorSystem

/**
  * Created by hollinwilkins on 1/30/17.
  */
object Boot extends App {
  val system = ActorSystem("MleapServing")
  MleapServer(system)
}
