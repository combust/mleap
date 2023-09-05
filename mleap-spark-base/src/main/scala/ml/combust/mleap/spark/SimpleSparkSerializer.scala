package ml.combust.mleap.spark

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.bundle.serializer.SerializationFormat
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.DataFrame
import scala.util.Using

/**
  * Created by mikhail on 11/5/16.
  *
  */
class SimpleSparkSerializer() {
  def serializeToBundle(transformer: Transformer, path: String, dataset: DataFrame): Unit = {
    serializeToBundleWithFormat(transformer = transformer, path = path, dataset = dataset, format = SerializationFormat.Json)
  }

  def serializeToBundleWithFormat(transformer: Transformer, path: String, dataset: DataFrame, format: SerializationFormat = SerializationFormat.Json): Unit = {
    implicit val context: SparkBundleContext = Option(dataset).
      map(d => SparkBundleContext.defaultContext.withDataset(d)).
      getOrElse(SparkBundleContext.defaultContext)

    Using(BundleFile.load(path)) { file =>
      transformer.writeBundle.format(format).save(file)
    }.flatten.get
  }

  def deserializeFromBundle(path: String): Transformer = {
    implicit val context: SparkBundleContext = SparkBundleContext.defaultContext

    Using(BundleFile.load(path)) { file =>
      file.loadSparkBundle()
    }.flatten.get.root
  }
}