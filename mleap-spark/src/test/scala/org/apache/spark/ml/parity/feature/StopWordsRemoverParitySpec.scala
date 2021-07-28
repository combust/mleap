package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class StopWordsRemoverParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("loan_title")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new Tokenizer().
    setInputCol("loan_title").
    setOutputCol("loan_title_tokens"),
    new StopWordsRemover().
      setInputCol("loan_title_tokens").
      setOutputCol("loan_title_stop").
      setStopWords(Array("loan")))).fit(dataset)

  it("serializes/deserializes the Spark model properly with multiple in/out columns"){
    bundleCache = None
    // outputCol has a default value of "<uid>__output, so we ignore it in this test
    // since the uid will be different
    val additionalIgnoreParams = Set("outputCol")

    val multiColTransformer = new Pipeline().setStages(Array(
      new Tokenizer().
        setInputCol("loan_title").
        setOutputCol("loan_title_tokens"),
      new Tokenizer().
        setInputCol("loan_title").
        setOutputCol("duplicate_tokens"),
      new StopWordsRemover().
        setInputCols(Array("loan_title_tokens", "duplicate_tokens")).
        setOutputCols(Array("loan_title_stop", "duplicate_stop")).
        setStopWords(Array("loan"))
    )).fit(dataset)
    val sparkTransformed = multiColTransformer.transform(baseDataset)
    implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
    val deserializedTransformer = deserializedSparkTransformer(multiColTransformer)
    checkEquality(multiColTransformer, deserializedTransformer, additionalIgnoreParams)
    equalityTest(sparkTransformed, deserializedTransformer.transform(baseDataset))

    bundleCache = None
  }
}
