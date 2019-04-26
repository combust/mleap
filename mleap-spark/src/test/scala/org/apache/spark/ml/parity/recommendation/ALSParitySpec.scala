package org.apache.spark.ml.parity.recommendation

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

class ALSParitySpec extends SparkParityBase {
  override val dataset: DataFrame = recommendationDataset
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(
    new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
  )).fit(dataset)

  override def equalityTest(sparkDataset: DataFrame, mleapDataset: DataFrame): Unit =
    super.equalityTest(sparkDataset.orderBy("userId", "movieId"), mleapDataset.orderBy("userId", "movieId"))

  //TODO: maybe coldStartStrategy should be serialized
  override val unserializedParams = Set("coldStartStrategy")
}
