package ml.combust.mleap.spark

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.mleap.core
import ml.combust.mleap.core.frame.Row
import ml.combust.mleap.core.types
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit class SparkTransformerOps(transformer: Transformer) {
    def writeBundle: BundleWriter[SparkBundleContext, Transformer] = BundleWriter(transformer)
  }

  implicit class SparkBundleFileOps(file: BundleFile) {
    def loadSparkBundle()
                       (implicit context: SparkBundleContext): Try[Bundle[Transformer]] = file.load()
  }

  implicit class MleapSparkTransformerOps[T <: core.Transformer](transformer: T) {
    def sparkTransform(dataset: DataFrame): DataFrame = {
      transformer.transform(dataset.toSparkLeapFrame).get.toSpark
    }
  }

  implicit class SparkDataFrameOps(dataset: DataFrame) {
    def toSparkLeapFrame: SparkLeapFrame = {
      val spec = dataset.schema.fields.
        map(f => TypeConverters.sparkToMleapConverter(dataset, f))
      val schema = types.StructType(spec.map(_._1)).get
      val converters = spec.map(_._2)
      val data = dataset.rdd.map(r => {
        val values = r.toSeq.zip(converters).map {
          case (v, c) => c(v)
        }
        Row(values: _*)
      })

      SparkLeapFrame(schema, data, dataset.sqlContext)
    }

    def mleapSchema: types.StructType = TypeConverters.sparkSchemaToMleapSchema(dataset)
  }

  implicit class MleapSchemaOps(schema: types.StructType) {
    def toSpark: StructType = TypeConverters.mleapSchemaToSparkSchema(schema)
  }
}

object SparkSupport extends SparkSupport
