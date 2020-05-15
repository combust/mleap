package org.apache.spark.ml.mleap.parity.feature

import ml.combust.mleap.core.feature.{HandleInvalid, StringMapModel}
import org.apache.spark.ml.mleap.feature.StringMap
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructType}

class StringMapParitySpec extends SparkParityBase {
  val names = Seq("alice", "andy", "kevin")
  val rows = spark.sparkContext.parallelize(Seq.tabulate(3) { i => Row(names(i)) })
  val schema = new StructType().add("name", StringType, nullable = false)

  override val dataset: DataFrame = spark.sqlContext.createDataFrame(rows, schema)

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(
    new StringMap(uid = "string_map", model = new StringMapModel(
      Map("alice" -> 0, "andy" -> 1, "kevin" -> 2)
    )).setInputCol("name").setOutputCol("index"),
    new StringMap(uid = "string_map2", model = new StringMapModel(
      // This map is missing the label "kevin". Exception is thrown unless HandleInvalid.Keep is set.
      Map("alice" -> 0, "andy" -> 1),
      handleInvalid = HandleInvalid.Keep, defaultValue = 1.0
    )).setInputCol("name").setOutputCol("index2")

  )).fit(dataset)
}
