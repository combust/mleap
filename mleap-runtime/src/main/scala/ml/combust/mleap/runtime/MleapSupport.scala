package ml.combust.mleap.runtime

import java.io.File

import ml.combust.mleap.bundle.MleapBundle
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.bundle.dsl.{AttributeList, Bundle, BundleMeta}
import ml.combust.bundle.serializer._

/** Object for support classes for easily working with Bundle.ML.
  */
object MleapSupport {
  /** Wrapper for [[ml.combust.mleap.runtime.transformer.Transformer]].
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
      * @param hr bundle registry
      */
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit hr: HasBundleRegistry): Unit = {
      MleapBundle.writeTransformer(transformer, path, list)
    }
  }

  /** Wrapper for java.io.File.
    *
    * Makes it easy to deserialize a [[ml.combust.mleap.runtime.transformer.Transformer]] from the file.
    *
    * @param path file to wrap
    */
  implicit class FileOps(path: File) {
    /** Deserialize the bundle definition.
      *
      * @param hr bundle registry
      * @return bundle meta data
      */
    def deserializeBundleMeta()
                             (implicit hr: HasBundleRegistry): BundleMeta = BundleSerializer(MleapContext(), path).readMeta()

    /** Deserialize the Bundle.ML to MLeap.
      *
      * @param hr bundle registry
      * @return (bundle, MLeap transformer)
      */
    def deserializeBundle()
                         (implicit hr: HasBundleRegistry): (Bundle, Transformer) = MleapBundle.readTransformer(path)
  }
}
