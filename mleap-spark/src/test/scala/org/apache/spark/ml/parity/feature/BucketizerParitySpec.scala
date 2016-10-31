package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class BucketizerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_amount")
  override val sparkTransformer: Transformer = new Bucketizer().
    setInputCol("loan_amount").
    setOutputCol("loan_amount_bucket").
    setSplits(Array(0.0, 1000.0, 10000.0, 9999999.0))
}
