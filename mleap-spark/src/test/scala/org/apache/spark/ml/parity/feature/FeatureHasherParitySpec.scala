package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.util.Random

class FeatureHasherParitySpec extends SparkParityBase {

  val categories = Seq(
    "spark",
    "and",
    "mleap",
    "are",
    "super",
    "dope",
    "together"
  )

  def randomRow(): Row = Row(Random.nextDouble(), Random.nextBoolean(), Random.nextInt(20), Random.nextInt(20).toString,
    Random.shuffle(categories).head)

  val rows = spark.sparkContext.parallelize(Seq.tabulate(100) { _ => randomRow() })
  val schema = new StructType()
    .add("real", DoubleType, nullable = false)
    .add("bool", BooleanType, nullable = false)
    .add("int", IntegerType, nullable = false)
    .add("stringNum", StringType, nullable = true)
    .add("string", StringType, nullable = true)

  override val dataset: DataFrame = spark.sqlContext.createDataFrame(rows, schema)

  override val sparkTransformer: Transformer = new FeatureHasher()
    .setInputCols("real", "bool", "int", "stringNum", "string")
    .setOutputCol("features")
    .setNumFeatures(1 << 17)
    .setCategoricalCols(Array("int"))

}
