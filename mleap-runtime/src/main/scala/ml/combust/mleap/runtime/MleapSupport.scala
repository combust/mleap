package ml.combust.mleap.runtime

import java.io.File

import ml.combust.mleap.runtime.serialization.bundle.{MleapBundle, MleapRegistry}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.bundle.BundleDef.BundleDef
import ml.bundle.dsl.{AttributeList, Bundle}
import ml.bundle.serializer._

/** Object for support classes for easily working with Bundle.ML.
  */
object MleapSupport {
  /** Default registry for Bundle.ML.
    */
  implicit val mleapRegistry: BundleRegistry = MleapRegistry.instance

  /** Wrapper for [[Transformer]].
    *
    * Makes it easy to serialize the wrapped transformer to Bundle.ML.
    *
    * @param transformer transform to wrap
    */
  implicit class TransformerOps(transformer: Transformer) {
    /** Serialize the transformer to a Bundle.ML directory.
      *
      * @param path path to Bundle.ML
      * @param list optional custom Bundle Attributes
      * @param format serialization format
      * @param registry bundle registry
      */
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit registry: BundleRegistry): Unit = {
      MleapBundle.writeTransformer(transformer, path, list)(registry)
    }
  }

  /** Wrapper for {@code java.io.File}.
    *
    * Makes it easy to deserialize a [[Transformer]] from the file.
    *
    * @param path file to wrap
    */
  implicit class FileOps(path: File) {
    /** Deserialize the bundle definition.
      *
      * @return bundle definition
      */
    def deserializeBundleDef(): BundleDef = MleapBundle.readBundleDef(path)

    /** Deserialize the Bundle.ML to MLeap.
      *
      * @return (bundle, MLeap transformer)
      */
    def deserializeBundle(): (Bundle, Transformer) = MleapBundle.readTransformer(path)
  }
}
