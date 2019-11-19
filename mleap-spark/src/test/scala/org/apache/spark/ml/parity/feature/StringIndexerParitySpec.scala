package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class StringIndexerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")

  // setting to handle invalid to true
  override val sparkTransformer: Transformer = new StringIndexer().
    setInputCol("state").
    setOutputCol("state_index").setHandleInvalid("keep").
    fit(dataset)

  override val unserializedParams = Set("stringOrderType")

}
