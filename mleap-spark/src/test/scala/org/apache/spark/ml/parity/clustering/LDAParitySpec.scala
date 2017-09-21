package org.apache.spark.ml.parity.clustering

import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame
import org.scalatest.Ignore

/**
  * Created by mageswarand on 14/3/17.
  *
  * These specs are failing, LDAModel probably needs some work
  */
@Ignore
class LDAParitySpec extends SparkParityBase {
  override val dataset: DataFrame = textDataset.select("text")

  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

  val remover = new StopWordsRemover()
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("words_filtered")

  val cv = new CountVectorizer().setInputCol("words_filtered").setOutputCol("features").setVocabSize(50000)

  val lda = new LDA().setK(5).setMaxIter(2)

  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(tokenizer, remover, cv, lda)).fit(dataset)

  override def equalityTest(sparkDataset: DataFrame,
                            mleapDataset: DataFrame): Unit = {
    val sparkPredictionCol = sparkDataset.schema.fieldIndex("topicDistribution")
    val mleapPredictionCol = mleapDataset.schema.fieldIndex("topicDistribution")

    sparkDataset.collect().zip(mleapDataset.collect()).foreach {
      case (sv, mv) =>
        val sparkPrediction = sv.getAs[Vector](sparkPredictionCol)
        val mleapPrediction = mv.getAs[Vector](mleapPredictionCol)

        sparkPrediction.toArray.zip(mleapPrediction.toArray).foreach {
          case (s, m) => assert(Math.abs(m - s) < 0.001)
        }
    }
  }
}
