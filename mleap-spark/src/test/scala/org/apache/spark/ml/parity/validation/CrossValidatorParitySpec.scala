package org.apache.spark.ml.parity.validation

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.regression.{DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

class CrossValidatorParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "loan_amount")
  override val sparkTransformer: Transformer = {
    val regressor = new RandomForestRegressor().
      setFeaturesCol("features").
      setLabelCol("loan_amount").
      setPredictionCol("prediction")
    val paramGrid = new ParamGridBuilder()
      .addGrid(regressor.numTrees, Array(2, 3, 4))
      .build()

    new Pipeline().setStages(Array(new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index"),
      new VectorAssembler().
        setInputCols(Array("fico_index", "dti")).
        setOutputCol("features"),
      new CrossValidator().
        setEvaluator(new RegressionEvaluator().
          setLabelCol("loan_amount").
          setPredictionCol("prediction")).
        setEstimator(regressor).
        setEstimatorParamMaps(paramGrid))).fit(dataset)
  }
}
