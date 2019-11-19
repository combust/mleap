package org.apache.spark.ml.parity.regression

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/28/16.
  */
class GeneralizedLinearRegressionParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "loan_amount")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new OneHotEncoderEstimator().
      setInputCols(Array("fico_index")).
      setOutputCols(Array("fico")),
    new VectorAssembler().
      setInputCols(Array("fico", "dti")).
      setOutputCol("features"),
    new GeneralizedLinearRegression().
      setFamily("gaussian").
      setLink("log").
      setFeaturesCol("features").
      setLabelCol("loan_amount").
      setPredictionCol("prediction"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType", "labelCol", "maxIter", "tol", "regParam", "solver", "variancePower")
}
