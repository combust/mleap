package ml.combust.mleap.serving

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.Config


/**
  * Created by hollinwilkins on 1/30/17.
  */
object MleapServer extends ExtensionId[MleapServer]
  with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): MleapServer = {
    new MleapServer(system.settings.config)(system)
  }

  override def lookup(): ExtensionId[_ <: Extension] = MleapServer
}

class MleapServer(tConfig: Config)
                 (implicit val system: ExtendedActorSystem) extends Extension {
  import system.dispatcher
  implicit val materializer = ActorMaterializer()

  val config = MleapConfig(tConfig.getConfig("ml.combust.mleap.serving"))

  val service = new MleapService()
  val resource = new MleapResource(service)
  val routes = resource.routes

  Http().bindAndHandle(routes, config.http.bindHostname, config.http.bindPort)
}
