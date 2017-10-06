package ml.combust.mleap.runtime

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.mleap.core.MleapCoreSupport
import ml.combust.mleap.runtime.transformer.Transformer

import scala.util.Try

/** Object for support classes for easily working with Bundle.ML and DefaultLeapFrame.
  */
trait MleapSupport extends MleapCoreSupport {
  implicit class MleapTransformerOps(transformer: Transformer) {
    def writeBundle: BundleWriter[MleapContext, Transformer] = BundleWriter(transformer)
  }

  implicit class MleapBundleFileOps(file: BundleFile) {
    def loadMleapBundle()
                       (implicit context: MleapContext): Try[Bundle[Transformer]] = file.load()
  }
}
object MleapSupport extends MleapSupport
