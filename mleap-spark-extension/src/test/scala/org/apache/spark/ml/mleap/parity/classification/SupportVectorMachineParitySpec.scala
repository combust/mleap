package org.apache.spark.ml.mleap.parity.classification

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.mleap.classification.SVMModel
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 10/30/16.
  */
class SupportVectorMachineParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new SVMModel(uid = "svm",
      model = new mllib.classification.SVMModel(weights = Vectors.dense(0.53, 0.67), intercept = 0.77)).setRawPredictionCol("raw_prediction").setProbabilityCol("probability"))).fit(dataset)
}
