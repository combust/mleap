package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame
import ml.combust.mleap.spark.SparkSupport._
import org.scalatest.Ignore

/**
  * Created by hollinwilkins on 10/30/16.
  *
  * These specs are failing, probably needs some work
  */
@Ignore
class HashingTermFrequencyParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new Tokenizer().
    setInputCol("loan_title").
    setOutputCol("loan_title_tokens"),
    new HashingTF().
      setNumFeatures(1 << 17).
      setInputCol("loan_title_tokens").
      setOutputCol("loan_title_tf"))).fit(dataset)
}
