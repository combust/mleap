package org.apache.spark.ml.mleap.parity.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.mleap.feature.Imputer
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.util.Random

/**
  * Created by hollinwilkins on 1/5/17.
  */
class ImputerParitySpec extends SparkParityBase {
  def randomRow(): Row = {
    if(Random.nextBoolean()) {
      if(Random.nextBoolean()) {
        Row(23.4)
      } else { Row(Random.nextDouble()) }
    } else {
      Row(33.2)
    }
  }
  val rows = spark.sparkContext.parallelize(Seq.tabulate(100) { i => randomRow() })
  val schema = new StructType().add("mv", DoubleType, nullable = true)

  override val dataset: DataFrame = spark.sqlContext.createDataFrame(rows, schema)
  override val sparkTransformer: Transformer = new Imputer(uid = "imputer").
    setInputCol("mv").
    setOutputCol("mv_imputed").
    setMissingValue(23.4).
    setStrategy("mean").fit(dataset)
}
