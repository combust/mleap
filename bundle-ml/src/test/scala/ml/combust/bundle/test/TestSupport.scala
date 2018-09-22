package ml.combust.bundle.test

import java.net.URI

import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.bundle.test.ops.Transformer

import resource._

/**
  * Created by hollinwilkins on 12/24/16.
  */
trait TestSupport {
  implicit class TestTransformerOps(transformer: Transformer) {
    def writeBundle: BundleWriter[TestContext, Transformer] = BundleWriter(transformer)
  }

  implicit class BundleFileOps(file: BundleFile) {
    def loadBundle()(implicit context: TestContext) = file.load[TestContext, Transformer]()
  }

  implicit class URIFileOps(uri: URI) {
    def loadBundle()(implicit context: TestContext) = {
      (for (bf <- managed(BundleFile.load(uri))) yield {
        bf.load[TestContext, Transformer]().get
      }).tried
    }
  }
}
object TestSupport extends TestSupport
