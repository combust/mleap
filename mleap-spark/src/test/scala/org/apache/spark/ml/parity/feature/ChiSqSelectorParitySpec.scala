package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.feature.{ChiSqSelectorModel, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.mllib.feature
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/27/16.
  */
class ChiSqSelectorParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti", "loan_amount")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new VectorAssembler().
    setInputCols(Array("dti", "loan_amount")).
    setOutputCol("features"),
    new ChiSqSelectorModel(uid = "chi_sq_selector", chiSqSelector = new feature.ChiSqSelectorModel(Array(1))).
      setFeaturesCol("features").
      setOutputCol("filter_features"))).fit(dataset)
}
