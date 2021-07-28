package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class StringIndexerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")

  // setting to handle invalid to true
  override val sparkTransformer: Transformer = new StringIndexer().
    setInputCol("state").
    setOutputCol("state_index").setHandleInvalid("keep").
    fit(dataset)

  override val unserializedParams = Set("stringOrderType")

  it("serializes/deserializes the Spark model properly with multiple in/out columns"){
    bundleCache = None
    // outputCol has a default value of "<uid>__output, so we ignore it in this test
    // since the uid will be different
    val additionalIgnoreParams = Set("outputCol")

    val multiColTransformer = new StringIndexer().
      setInputCols(Array("state", "loan_title")).
      setOutputCols(Array("state_index", "loan_tile_index")).
      setHandleInvalid("keep").
      fit(baseDataset)
    val sparkTransformed = multiColTransformer.transform(baseDataset)
    implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
    val deserializedTransformer = deserializedSparkTransformer(multiColTransformer)
    checkEquality(multiColTransformer, deserializedTransformer, additionalIgnoreParams)
    equalityTest(sparkTransformed, deserializedTransformer.transform(baseDataset))

    bundleCache = None
  }
}
