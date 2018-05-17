package org.apache.spark.ml.parity.classification

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 10/30/16.
  */
class LogisticRegressionParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new LogisticRegressionModel(uid = "logr",
      coefficients = Vectors.dense(0.44, 0.77),
      intercept = 0.66).setThreshold(0.5).setFeaturesCol("features"))).fit(dataset)
}
