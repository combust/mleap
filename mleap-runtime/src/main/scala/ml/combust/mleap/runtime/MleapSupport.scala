package ml.combust.mleap.runtime

import java.io.File

import ml.combust.mleap.runtime.serialization.bundle.{MleapBundle, MleapRegistry}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.bundle.BundleDef.BundleDef
import ml.bundle.dsl.{AttributeList, Bundle}
import ml.bundle.serializer._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapSupport {
  implicit val mleapRegistry: BundleRegistry = MleapRegistry.instance

  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit registry: BundleRegistry): Unit = {
      MleapBundle.writeTransformer(transformer, path, list)(registry)
    }
  }

  implicit class FileOps(path: File) {
    def deserializeBundleDef(): BundleDef = MleapBundle.readBundleDef(path)
    def deserializeBundle(): (Bundle, Transformer) = MleapBundle.readTransformer(path)
  }
}
