package ml.combust.mleap.serving

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import ml.combust.mleap.BuildInfo

/**
  * Created by hollinwilkins on 1/30/17.
  */
object Boot extends App {
  val info = BuildInfo()

  val parser = new scopt.OptionParser[Config](info.name) {
    head(info.name, info.version)
    help("help").text("prints this usage text")

    opt[Int]( "grpc-port").text("specify grpc port number").action {
      (port, config) => config.withValue("ml.combust.mleap.grpc.server.port", ConfigValueFactory.fromAnyRef(port))
    }
  }

  parser.parse(args, ConfigFactory.load()) match {
    case Some(config) => new RunServer(config).run()
    case None =>
  }
}
