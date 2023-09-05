package ml.combust.mleap.benchmark

import com.typesafe.config.Config
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.runtime.frame.Transformer

import java.nio.file.Path
import scala.util.Using

/**
  * Created by hollinwilkins on 2/4/17.
  */
trait Benchmark {
  def benchmark(config: Config): Unit

  protected def mleapBundleForPath(path: Path): Bundle[Transformer] = {
    Using(BundleFile(path)) { bf =>
      bf.loadMleapBundle()
    }.flatten.get
  }
}
