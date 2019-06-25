package org.apache.spark.ml.parity.classification

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by fshabbir on 12/29/16.
  */
class NaiveBayesClassifierParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index")).
      setOutputCol("features"),
    new StringIndexer().
      setInputCol("approved").
      setOutputCol("label"),
    new NaiveBayes(uid = "nb").
      setModelType("multinomial").
      setThresholds(Array(0.4)).
      setFeaturesCol("features").
      setLabelCol("label"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType", "labelCol", "smoothing")
}
