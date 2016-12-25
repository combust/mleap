package ml.combust.mleap.spark

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.mleap.runtime.transformer.{Transformer => MleapTransformer}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit class SparkTransformerOps(transformer: Transformer) {
    def write: BundleWriter[SparkBundleContext, Transformer] = BundleWriter(transformer)
  }

  implicit class SparkBundleFileOps(file: BundleFile) {
    def load()
            (implicit context: SparkBundleContext): Try[Bundle[Transformer]] = file.loadBundle()
  }

  implicit class MleapSparkTransformerOps[T <: MleapTransformer](transformer: T) {
    def sparkTransform(dataset: DataFrame): DataFrame = {
      transformer.transform(SparkTransformBuilder(dataset)).map(_.dataset).get
    }
  }
}
object SparkSupport extends SparkSupport
