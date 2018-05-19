package ml.combust.mleap.serving

import com.typesafe.config.Config
import scala.util.Properties

/**
  * Created by hollinwilkins on 1/31/17.
  */
case class HttpConfig(config: Config) {
  val bindHostname = scala.util.Properties.envOrElse("MLEAP_SERVER_HOSTNAME", config.getString("bind-hostname"))
  val bindPort = scala.util.Properties.envOrElse("MLEAP_SERVER_PORT", config.getString("bind-port")).toInt
}

case class MleapConfig(config: Config) {
  val http = HttpConfig(config.getConfig("http"))
  val model = if(config.hasPath("model")) Some(config.getString("model")) else None
}
