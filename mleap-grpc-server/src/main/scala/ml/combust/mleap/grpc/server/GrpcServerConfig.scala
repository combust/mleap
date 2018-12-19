package ml.combust.mleap.grpc.server

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

class GrpcServerConfig(config: Config) {
  val defaultStreamInitTimeout: FiniteDuration = {
    FiniteDuration(config.getDuration("default-stream-init-timeout").toMillis, TimeUnit.MILLISECONDS)
  }
}
