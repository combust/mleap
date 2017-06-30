package ml.combust.mleap.serving

import akka.actor.ActorSystem

/**
  * Created by hollinwilkins on 1/30/17.
  */
object Boot extends App {
  if (args.length == 1) {
    sys.props("ml.combust.mleap.serving.model") = args(0)
  }

  val system = ActorSystem("MleapServing")
  MleapServer(system)
}
