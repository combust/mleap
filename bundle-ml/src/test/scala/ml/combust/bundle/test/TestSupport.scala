package ml.combust.bundle.test

import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.bundle.test.ops.Transformer

/**
  * Created by hollinwilkins on 12/24/16.
  */
trait TestSupport {
  implicit class TestTransformerOps(transformer: Transformer) {
    def write: BundleWriter[TestContext, Transformer] = BundleWriter(transformer)
  }

  implicit class BundleFileOps(file: BundleFile) {
    def load()(implicit context: TestContext) = file.loadBundle[TestContext, Transformer]()
  }
}
object TestSupport extends TestSupport
