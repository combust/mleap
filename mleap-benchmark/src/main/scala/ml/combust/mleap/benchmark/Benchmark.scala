package ml.combust.mleap.benchmark

import com.typesafe.config.Config

/**
  * Created by hollinwilkins on 2/4/17.
  */
trait Benchmark {
  def benchmark(config: Config): Unit
}
