package ml.combust.mleap.grpc.server

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import ml.combust.mleap.BuildInfo

object Boot extends App {
  val info = BuildInfo()

  val parser = new scopt.OptionParser[Config](info.name) {
    head(info.name, info.version)
    help("help").text("prints this usage text")

    opt[Int]('p', "port").text("specify port number").action {
      (port, config) => config.withValue("port", ConfigValueFactory.fromAnyRef(port))
    }
  }

  parser.parse(args, ConfigFactory.load().getConfig("ml.combust.mleap.grpc.server")) match {
    case Some(config) => new RunServer(config).run()
    case None =>
  }
}
