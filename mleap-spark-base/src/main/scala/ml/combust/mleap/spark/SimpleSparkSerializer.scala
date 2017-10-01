package ml.combust.mleap.spark

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.DataFrame
import resource._

/**
  * Created by mikhail on 11/5/16.
  *
  */
class SimpleSparkSerializer() {
  def serializeToBundle(transformer: Transformer, path: String, dataset: DataFrame): Unit = {
    implicit val context: SparkBundleContext = Option(dataset).
      map(d => SparkBundleContext.defaultContext.withDataset(d)).
      getOrElse(SparkBundleContext.defaultContext)

    (for(file <- managed(BundleFile(path))) yield {
      transformer.writeBundle.save(file).get
    }).tried.get
  }

  def deserializeFromBundle(path: String): Transformer = {
    implicit val context: SparkBundleContext = SparkBundleContext.defaultContext

    (for(file <- managed(BundleFile(path))) yield {
      file.loadSparkBundle().get.root
    }).tried.get
  }
}