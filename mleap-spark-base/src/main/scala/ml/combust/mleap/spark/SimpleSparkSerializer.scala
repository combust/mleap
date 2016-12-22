package ml.combust.mleap.spark

import java.io.File

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.Transformer

/**
  * Created by mikhail on 11/5/16.
  *
  */
class SimpleSparkSerializer() {
  implicit val hr: HasBundleRegistry = BundleRegistry("spark")

  def serializeToBundle(transformer: Transformer, path : String): Unit = {
    transformer.serializeToBundle(new File(path))
  }

  def deserializeFromBundle(path: String): Transformer = {
    new File(path).deserializeBundle().root
  }

}
