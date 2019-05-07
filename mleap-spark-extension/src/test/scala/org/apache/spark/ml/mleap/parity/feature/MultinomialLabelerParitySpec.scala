package org.apache.spark.ml.mleap.parity.feature

import ml.combust.mleap.core.feature.{MultinomialLabelerModel, ReverseStringIndexerModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.mleap.feature.MultinomialLabeler
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabelerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new MultinomialLabeler(uid = "multinomial_labeler", model = MultinomialLabelerModel(threshold = 0.1,
      indexer = ReverseStringIndexerModel(Seq("fico", "dtizy")))).
      setFeaturesCol("features").
      setProbabilitiesCol("probabilities").
      setLabelsCol("labels"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType")
}
