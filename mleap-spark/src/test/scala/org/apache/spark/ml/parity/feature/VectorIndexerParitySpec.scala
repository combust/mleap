package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorIndexerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti", "loan_amount", "state")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("state").
    setOutputCol("state_index"),
    new VectorAssembler().
      setInputCols(Array("dti", "loan_amount", "state_index")).
      setOutputCol("features"),
    new VectorIndexer().
      setInputCol("features").
      setOutputCol("scaled_features").
      setHandleInvalid("skip"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType")
}
