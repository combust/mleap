package ml.combust.mleap.executor

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class ExecutorConfig(config: Config) {
  val defaultMemoryTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration("default-memory-timeout").toMillis, TimeUnit.MILLISECONDS)
  }

  val defaultDiskTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration("default-memory-timeout").toMillis, TimeUnit.MILLISECONDS)
  }
}
