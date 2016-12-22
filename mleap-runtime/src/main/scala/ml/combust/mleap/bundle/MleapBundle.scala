package ml.combust.mleap.bundle

import java.io.File

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/23/16.
  */
object MleapBundle {
  def readTransformer(path: File)
                     (implicit context: MleapContext): Bundle[Transformer] = {
    BundleSerializer(context, path).read[Transformer]()
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit context: MleapContext): Unit = {
    BundleSerializer(context, path).write[Transformer](Bundle(transformer.uid,
      format,
      transformer))
  }
}
