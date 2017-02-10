package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame

class RegexTokenizerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new RegexTokenizer()
    .setInputCol("loan_title")
    .setOutputCol("loan_title_tokens")
    .setGaps(true)
    .setToLowercase(true)
    .setMinTokenLength(2)
    .setPattern("\\s")
}
