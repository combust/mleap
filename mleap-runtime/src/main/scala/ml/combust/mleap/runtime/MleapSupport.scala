package ml.combust.mleap.runtime

import java.io.File

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.bundle.MleapBundle
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.bundle.dsl.{Bundle, BundleInfo}
import ml.combust.bundle.serializer._

import scala.util.Try

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
      * @param format serialization format
      * @param hr bundle registry
      */
    def serializeToBundle(path: File,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit hr: HasBundleRegistry = BundleRegistry("mleap"),
                          context: MleapContext = MleapContext()): Try[Bundle[Transformer]] = {
      MleapBundle.writeTransformer(transformer, path, format)
    }
  }

  /** Wrapper for java.io.File.
    *
    * Makes it easy to deserialize a [[ml.combust.mleap.runtime.transformer.Transformer]] from the file.
    *
    * @param path file to wrap
    */
  implicit class FileOps(path: File) {
    /** Deserialize the Bundle.ML to MLeap.
      *
      * @return (bundle, MLeap transformer)
      */
    def deserializeBundle()
                         (implicit context: MleapContext = MleapContext()): Try[Bundle[Transformer]] = MleapBundle.readTransformer(path)
  }
}
