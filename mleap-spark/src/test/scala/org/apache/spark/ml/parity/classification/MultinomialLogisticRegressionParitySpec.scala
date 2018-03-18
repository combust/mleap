package org.apache.spark.ml.parity.classification

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

class MultinomialLogisticRegressionParitySpec extends SparkParityBase {

  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new LogisticRegression().
      setLabelCol("fico_index").
      setFeaturesCol("features").
      setPredictionCol("prediction"))).fit(dataset)
}
