package ml.combust.mleap.serving

import akka.actor.ActorSystem
import com.typesafe.config.Config

class RunServer(config: Config) {
  def run(): Unit = {
    implicit val system: ActorSystem = ActorSystem("MleapGrpc")

    new ml.combust.mleap.grpc.server.RunServer(config.getConfig("ml.combust.mleap.grpc.server")).run()
    new ml.combust.mleap.springboot.RunServer(Some(system)).run()
  }
}
