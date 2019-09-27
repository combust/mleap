package org.apache.spark.ml.parity.regression

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 10/30/16.
  */
class RandomForestRegressionParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "loan_amount")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new RandomForestRegressor().
      setFeaturesCol("features").
      setLabelCol("loan_amount").
      setPredictionCol("prediction"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType", "labelCol", "seed")
}
