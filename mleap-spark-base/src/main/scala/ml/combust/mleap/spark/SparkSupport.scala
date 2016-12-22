package ml.combust.mleap.spark

import java.io.File

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
  implicit class SparkTransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit context: SparkBundleContext = SparkBundleContext()): Unit = {
      SparkBundle.writeTransformer(transformer, path, format)(context)
    }
  }

  implicit class MleapTransformerOps[T <: MleapTransformer](transformer: T) {
    def sparkTransform(dataset: DataFrame): DataFrame = {
      transformer.transform(SparkTransformBuilder(dataset)).map(_.dataset).get
    }
  }

  implicit class FileOps(path: File) {
    def deserializeBundleMeta()
                             (implicit context: SparkBundleContext = SparkBundleContext()): BundleMeta = {
      BundleSerializer(context, path).readMeta()
    }

    def deserializeBundle()
                         (implicit context: SparkBundleContext = SparkBundleContext()): Bundle[Transformer] = {
      SparkBundle.readTransformer(path)
    }
  }
}
object SparkSupport extends SparkSupport
