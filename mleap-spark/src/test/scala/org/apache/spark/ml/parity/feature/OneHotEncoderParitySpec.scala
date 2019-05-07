package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.feature.{
  OneHotEncoderEstimator,
  StringIndexer
}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class OneHotEncoderParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")
  override val sparkTransformer: Transformer =
      new Pipeline()
        .setStages(Array(
          new StringIndexer().setInputCol("state").setOutputCol("state_index"),
          new StringIndexer().setInputCol("state").setOutputCol("state_index2"),
          new OneHotEncoderEstimator()
            .setInputCols(Array("state_index", "state_index2"))
            .setOutputCols(Array("state_oh", "state_oh2"))
        ))
        .fit(dataset)

  override val unserializedParams = Set("stringOrderType")
}
