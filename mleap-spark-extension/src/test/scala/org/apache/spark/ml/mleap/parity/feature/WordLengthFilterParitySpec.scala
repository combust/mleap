package org.apache.spark.ml.mleap.parity.feature

import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.mleap.feature.WordLengthFilter
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by mageswarand on 14/3/17.
  */
class WordLengthFilterParitySpec extends SparkParityBase {
  override val dataset: DataFrame = textDataset.select("text")

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

  val remover = new StopWordsRemover()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("words_filtered")

  val cv = new CountVectorizer().setInputCol("words_filtered").setOutputCol("features").setVocabSize(50000)

  val filterWords = new WordLengthFilter().setInputCol("words_filtered").setOutputCol("filteredWords").setWordLength(3)

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(tokenizer, remover, cv, filterWords)).fit(dataset)
}
