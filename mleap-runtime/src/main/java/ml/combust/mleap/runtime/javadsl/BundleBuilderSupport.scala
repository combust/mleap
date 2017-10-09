package ml.combust.mleap.runtime.javadsl

import java.io.File

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.MleapSupport._
import resource._

/**
  * Created by hollinwilkins on 4/21/17.
  */
class BundleBuilderSupport {
  def load(file: File, context: MleapContext): Bundle[Transformer] = {
    (for(bf <- managed(BundleFile(file))) yield {
      bf.loadMleapBundle()(context).get
    }).tried.get
  }

  def save(transformer: Transformer, file: File, context: MleapContext): Unit = {
    (for(bf <- managed(BundleFile(file))) yield {
      transformer.writeBundle.save(bf)(context).get
    }).tried.get
  }
}
