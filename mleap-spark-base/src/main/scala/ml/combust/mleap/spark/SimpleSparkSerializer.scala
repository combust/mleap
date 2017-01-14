package ml.combust.mleap.spark

import ml.combust.bundle.{BundleFile, BundleRegistry, HasBundleRegistry}
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.Transformer
import resource._

/**
  * Created by mikhail on 11/5/16.
  *
  */
class SimpleSparkSerializer() {
  implicit val hr: HasBundleRegistry = BundleRegistry("spark")

  def serializeToBundle(transformer: Transformer, path: String): Unit = {
    for(file <- managed(BundleFile(path))) {
      transformer.writeBundle.save(file)
    }
  }

  def deserializeFromBundle(path: String): Transformer = {
    (for(file <- managed(BundleFile(path))) yield {
      file.load().get.root
    }).either.either match {
      case Right(root) => root
      case Left(errors) => throw errors.head
    }
  }
}
