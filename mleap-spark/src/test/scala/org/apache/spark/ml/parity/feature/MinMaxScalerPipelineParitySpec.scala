package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, MinMaxScaler, QuantileDiscretizer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

class MinMaxScalerPipelineParitySpec extends SparkParityBase {

  private val getKeys: Map[String, Double] => Seq[String] = { input: Map[String, Double] => input.keySet.toSeq }

  val keyUdf = functions.udf(getKeys)

  override val dataset = spark.createDataFrame(Seq(
    (Array("1"), 1.0, Map("a" -> 0.1, "b" -> 0.2, "c" -> 0.3), 1),
    (Array("2"), 10.0, Map("d" -> 0.1, "e" -> 0.2, "c" -> 0.3), 0),
    (Array("3"), 20.0, Map("x" -> 0.1, "a" -> 0.2, "b" -> 0.3), 0),
    (Array("4"), 15.0, Map("c" -> 0.1, "b" -> 0.2, "w" -> 0.3), 0),
    (Array("5"), 18.0, Map("c" -> 0.1, "b" -> 0.2, "w" -> 0.3), 0),
    (Array("6"), 25.0, Map("c" -> 0.1, "b" -> 0.2, "w" -> 0.3), 1),
    (Array("6"), 5.0, Map("a" -> 0.1, "b" -> 0.2, "d" -> 0.3), 0),
    (Array("7"), 30.0, Map("c" -> 0.1, "b" -> 0.2, "w" -> 0.3), 0))
  )
    .toDF("book_id", "pv", "myInputCol0", "label")
    .withColumn("myInputCol", keyUdf(functions.col("myInputCol0")))
    .drop("myInputCol0")

  override val sparkTransformer = new Pipeline()
    .setStages(Array(new CountVectorizer()
      .setInputCol("book_id")
      .setOutputCol("book_id_vec")
      .setMinDF(1)
      .setMinTF(1)
      .setBinary(true),
      new QuantileDiscretizer()
        .setInputCol("pv")
        .setOutputCol("pv_bucket")
        .setNumBuckets(3),
      new CountVectorizer()
        .setInputCol("myInputCol")
        .setOutputCol("myInputCol1_vec")
        .setMinDF(1)
        .setMinTF(1)
        .setBinary(true),
      new VectorAssembler()
        .setInputCols(Array("pv_bucket", "book_id_vec", "myInputCol1_vec"))
        .setOutputCol("vectorFeature"),
      new MinMaxScaler().setInputCol("vectorFeature").setOutputCol("scaledFeatures"))).fit(dataset)
}
