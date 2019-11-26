package org.apache.spark.ml.parity.regression

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.regression.IsotonicRegression
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "loan_amount").sample(withReplacement = true, 0.05)
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new VectorAssembler().
      setInputCols(Array("dti")).
      setOutputCol("features"),
    new IsotonicRegression().
      setFeaturesCol("dti").
      setLabelCol("loan_amount").
      setPredictionCol("prediction"))).fit(dataset)

  override val unserializedParams = Set("labelCol")
}
