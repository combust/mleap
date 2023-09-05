package ml.combust.mleap.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigValueFactory.fromAnyRef
import ml.combust.mleap.BuildInfo

/**
  * Created by hollinwilkins on 2/4/17.
  */
object Boot extends App {
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
      (_, config) =>
        config.withValue("benchmark", fromAnyRef("ml.combust.mleap.benchmark.MleapTransformBenchmark")).
          withFallback(ConfigFactory.load("mleap.conf"))
    }.children(modelPath, framePath)

    cmd("mleap-row").text("MLeap row transform benchmark").action {
      (_, config) =>
        config.withValue("benchmark", fromAnyRef("ml.combust.mleap.benchmark.MleapRowTransformBenchmark")).
          withFallback(ConfigFactory.load("mleap.conf"))
    }.children(modelPath, framePath)

    cmd("spark-transform").text("standard Spark transform benchmark").action {
      (_, config) =>
        config.withValue("benchmark", fromAnyRef("ml.combust.mleap.benchmark.SparkTransformBenchmark")).
          withFallback(ConfigFactory.load("spark.conf"))
    }.children(modelPath, framePath)
  }

  parser.parse(args, ConfigFactory.empty()).foreach { config =>
      Class.forName(config.getString("benchmark"))
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[Benchmark]
        .benchmark(config)
  }
}
