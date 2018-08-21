package ml.combust.mleap.executor

import java.util.concurrent.TimeUnit

import akka.stream.ThrottleMode
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

object ExecutorConfig {
  def throttleFromConfig(config: Config): Throttle = {
    val mode = config.getString("mode") match {
      case "shaping" => ThrottleMode.shaping
      case "enforcing" => ThrottleMode.enforcing
    }

    Throttle(
      elements = config.getInt("elements"),
      maxBurst = config.getInt("max-burst"),
      duration = FiniteDuration(config.getDuration("duration").toMillis, TimeUnit.MILLISECONDS),
      mode = mode
    )
  }
}

class ExecutorStreamConfig(config: Config) {
  val defaultIdleTimeout: Option[FiniteDuration] = if (config.hasPath("default-idle-timeout")) {
    Some(FiniteDuration(config.getDuration("default-idle-timeout").toMillis, TimeUnit.MILLISECONDS))
  } else { None }

  val defaultTransformDelay: Option[FiniteDuration] = if (config.hasPath("default-idle-timeout")) {
    Some(FiniteDuration(config.getDuration("default-transform-delay").toMillis, TimeUnit.MILLISECONDS))
  } else { None }

  val defaultThrottle: Option[Throttle] = if (config.hasPath("default-throttle")) {
    Some(ExecutorConfig.throttleFromConfig(config.getConfig("default-throttle")))
  } else { None }

  val defaultParallelism: Parallelism = config.getInt("default-parallelism")
  val defaultBufferSize: Int = config.getInt("default-buffer-size")
}

class ExecutorFlowConfig(config: Config) {
  val defaultIdleTimeout: Option[FiniteDuration] = if (config.hasPath("default-idle-timeout")) {
    Some(FiniteDuration(config.getDuration("default-idle-timeout").toMillis, TimeUnit.MILLISECONDS))
  } else { None }

  val defaultTransformDelay: Option[FiniteDuration] = if (config.hasPath("default-idle-timeout")) {
    Some(FiniteDuration(config.getDuration("default-transform-delay").toMillis, TimeUnit.MILLISECONDS))
  } else { None }

  val defaultThrottle: Option[Throttle] = if (config.hasPath("default-throttle")) {
    Some(ExecutorConfig.throttleFromConfig(config.getConfig("default-throttle")))
  } else { None }

  val defaultParallelism: Parallelism = config.getInt("default-parallelism")
}

class ExecutorConfig(config: Config) {
  val defaultMemoryTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration("default-memory-timeout").toMillis, TimeUnit.MILLISECONDS)
  }

  val defaultDiskTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration("default-memory-timeout").toMillis, TimeUnit.MILLISECONDS)
  }

  val stream: ExecutorStreamConfig = new ExecutorStreamConfig(config.getConfig("stream"))
  val flow: ExecutorFlowConfig = new ExecutorFlowConfig(config.getConfig("flow"))
}
