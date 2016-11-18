package org.apache.spark.ml.parity.clustering

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class GaussianMixtureParitySpec extends SparkParityBase {
  override val dataset: DataFrame = {
    val training = baseDataset.select("dti", "loan_amount", "fico_score_group_fnl")
    val pipeline = new Pipeline().setStages(Array(new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index"),
      new VectorAssembler().
        setInputCols(Array("fico_index", "dti")).
        setOutputCol("features"))).fit(training)
    pipeline.transform(training)
  }
  override val sparkTransformer: Transformer = {
    new GaussianMixture().
      setFeaturesCol("features").
      setPredictionCol("prediction").fit(dataset)
  }
}
