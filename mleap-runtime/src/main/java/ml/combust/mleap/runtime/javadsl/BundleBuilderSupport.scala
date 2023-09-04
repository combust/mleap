package ml.combust.mleap.runtime.javadsl

import java.io.File

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.MleapSupport._
import scala.util.Using

/**
  * Created by hollinwilkins on 4/21/17.
  */
class BundleBuilderSupport {
  def load(file: File, context: MleapContext): Bundle[Transformer] = {
    implicit val c: MleapContext = context

    Using(BundleFile(file)) { bf =>
      bf.loadMleapBundle()(context).get
    }.get
  }

  def save(transformer: Transformer, file: File, context: MleapContext): Unit = {
    implicit val c: MleapContext = context

    Using(BundleFile(file)) { bf =>
      transformer.writeBundle.save(bf)(context).get
    }.get
  }
}
