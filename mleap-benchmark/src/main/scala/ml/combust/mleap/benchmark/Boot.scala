package ml.combust.mleap.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import ml.combust.mleap.BuildInfo

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 2/4/17.
  */
object Boot extends App {
  val defaultMap: Map[String, Integer] = Map(
    "start" -> 10,
    "end" -> 100,
    "step" -> 10
  )
  val defaultConfig = ConfigFactory.parseMap(defaultMap.asJava)

  val parser = new scopt.OptionParser[Config]("mleap-benchmark") {
    head("mleap-benchmark", BuildInfo().version)

    def modelPath = opt[String]("model-path").
      required().
      text("path to MLeap bundle containing model").action {
      (path, config) => config.withValue("model-path", fromAnyRef(path))
    }

    def framePath = opt[String]("frame-path").
      required().
      text("path to frame containing data for transform").action {
      (path, config) => config.withValue("frame-path", fromAnyRef(path))
    }

    cmd("mleap-transform").text("standard MLeap transform benchmark").action {
      (_, config) => config.withValue("benchmark", fromAnyRef("ml.combust.mleap.benchmark.MleapTransformBenchmark"))
    }.children(modelPath, framePath)
  }

  parser.parse(args, defaultConfig) match {
    case Some(config) =>
      Class.forName(config.getString("benchmark")).
        newInstance().
        asInstanceOf[Benchmark].benchmark(config)
    case None => // do nothing
  }
}
