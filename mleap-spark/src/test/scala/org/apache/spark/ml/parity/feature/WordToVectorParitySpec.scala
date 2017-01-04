package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.feature.{Tokenizer, Word2Vec}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/28/16.
  */
class WordToVectorParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new Tokenizer().
    setInputCol("loan_title").
    setOutputCol("loan_title_tokens"),
    new Word2Vec(uid = "words").
      setInputCol("loan_title_tokens").
      setOutputCol("loan_title_token_counts"))).fit(dataset)
}
