package ml.combust.bundle.test

import ml.combust.bundle.dsl.Bundle

import java.net.URI
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.bundle.test.ops.Transformer

import scala.util.{Try, Using}

/**
  * Created by hollinwilkins on 12/24/16.
  */
trait TestSupport {
  implicit class TestTransformerOps(transformer: Transformer) {
    def writeBundle: BundleWriter[TestContext, Transformer] = BundleWriter(transformer)
  }

  implicit class BundleFileOps(file: BundleFile) {
    def loadBundle()(implicit context: TestContext): Try[Bundle[Transformer]] = file.load[TestContext, Transformer]()
  }

  implicit class URIFileOps(uri: URI) {
    def loadBundle()(implicit context: TestContext): Try[Bundle[Transformer]] = {
      Using(BundleFile.load(uri)) { bf =>
        bf.load[TestContext, Transformer]().get
      }
    }
  }
}
object TestSupport extends TestSupport
