package ml.combust.mleap.spark

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}
import org.apache.spark.ml.Transformer
import SparkSupport._
import java.io.File

/**
  * Created by mikhail on 11/5/16.
  *
  *
  */
class PythonSerializer() {
  implicit val  hr: HasBundleRegistry = BundleRegistry("spark")

  def serializeToBundle(transformer : Transformer, path : String): Unit = {
    transformer.serializeToBundle(new File(path))
  }

  def deserializeFromBundle(path : String): Transformer = {
    val (bundle, tf) = new File(path).deserializeBundle()
    println(tf.uid)
    tf
  }

}
