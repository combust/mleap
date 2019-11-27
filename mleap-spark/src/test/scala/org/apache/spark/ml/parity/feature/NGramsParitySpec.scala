package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{NGram, Tokenizer}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class NGramsParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new Tokenizer().
    setInputCol("loan_title").
    setOutputCol("loan_title_tokens"),
    new NGram().
      setInputCol("loan_title_tokens").
      setOutputCol("loan_title_ngram").
      setN(3))).fit(dataset)

}
