package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class StringIndexerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")
  override val unserializedParams = Set("stringOrderType")

  // setting to handle invalid to true
  override val sparkTransformer: Transformer = new StringIndexer().
    setInputCol("state").
    setOutputCol("state_index").setHandleInvalid("keep").
    fit(dataset)
}
class StringIndexerNoOutputColParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")
  override val unserializedParams = Set("stringOrderType")

  // setting to handle invalid to true
  override val sparkTransformer: Transformer = new StringIndexer().
    setInputCol("state").
    setHandleInvalid("keep").
    fit(dataset)
}

class MIOStringIndexerParitySpec extends  SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state", "loan_title")
  override val unserializedParams = Set("stringOrderType")
  override val sparkTransformer: Transformer =  new StringIndexer().
      setInputCols(Array("state", "loan_title")).
      setOutputCols(Array("state_index", "loan_tile_index")).
      setHandleInvalid("keep").
      fit(dataset)
}
