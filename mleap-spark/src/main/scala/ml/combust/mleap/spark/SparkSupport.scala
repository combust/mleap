package ml.combust.mleap.spark

import java.io.File

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer._
import ml.combust.mleap.runtime.transformer.{Transformer => MleapTransformer}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.{SparkBundle, SparkBundleContext}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit hr: HasBundleRegistry = BundleRegistry("spark"),
                          context: SparkBundleContext = SparkBundleContext()): Unit = {
      SparkBundle.writeTransformer(transformer, path, list, format)(hr)
    }
  }

  implicit class MleapTransformerOps(transformer: MleapTransformer) {
    def transform(dataset: DataFrame): DataFrame = {
      transformer.transform(SparkTransformBuilder(dataset)).map(_.dataset).get
    }
  }

  implicit class FileOps(path: File) {
    def deserializeBundleMeta()
                             (implicit hr: HasBundleRegistry = BundleRegistry("spark"),
                              context: SparkBundleContext = SparkBundleContext()): BundleMeta = BundleSerializer(context, path).readMeta()

    def deserializeBundle()
                         (implicit hr: HasBundleRegistry = BundleRegistry("spark"),
                          context: SparkBundleContext = SparkBundleContext()): (Bundle, Transformer) = SparkBundle.readTransformer(path)
  }
}
object SparkSupport extends SparkSupport
