package ml.combust.mleap.serving

import com.typesafe.config.Config

class RunServer(config: Config) {
  def run(): Unit = {
    new ml.combust.mleap.grpc.server.RunServer(config.getConfig("ml.combust.mleap.grpc.server")).run()
    new ml.combust.mleap.springboot.RunServer().run()
  }
}
