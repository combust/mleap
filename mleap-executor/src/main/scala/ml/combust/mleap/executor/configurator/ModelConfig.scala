package ml.combust.mleap.executor.configurator

import java.net.URI

import scala.concurrent.duration.FiniteDuration

case class ModelConfig(uri: URI,
                       memoryTimeout: FiniteDuration,
                       diskTimeout: FiniteDuration)
