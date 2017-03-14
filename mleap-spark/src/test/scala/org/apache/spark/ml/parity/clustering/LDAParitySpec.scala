package org.apache.spark.ml.parity.clustering

import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by mageswarand on 14/3/17.
  */
class LDAParitySpec extends SparkParityBase {
  override val dataset: DataFrame = textDataset.select("text")

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

  val remover = new StopWordsRemover()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("words_filtered")

  val cv = new CountVectorizer().setInputCol("words_filtered").setOutputCol("features").setVocabSize(50000)

  val lda = new LDA().setK(5).setMaxIter(2)

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(tokenizer, remover, cv, lda)).fit(dataset)
}
