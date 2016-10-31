package ml.combust.mleap.spark.parity.feature

import ml.combust.mleap.spark.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class TokenizerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new Tokenizer().
    setInputCol("loan_title").
    setOutputCol("loan_title_tokens")
}
