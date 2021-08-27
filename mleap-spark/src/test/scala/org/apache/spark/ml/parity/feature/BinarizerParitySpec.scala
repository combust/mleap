package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.{Binarizer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new VectorAssembler().
    setInputCols(Array("dti")).
    setOutputCol("features"),
    new Binarizer().
      setThreshold(0.12).
      setInputCol("dti").
      setOutputCol("thresholded_features_double"),
    new Binarizer().
      setThreshold(0.12).
      setInputCol("features").
      setOutputCol("thresholded_features"))).fit(dataset)


  it("serializes/deserializes the Spark model properly with multiple in/out columns"){
    bundleCache = None
    // outputCol has a default value of "<uid>__output, so we ignore it in this test
    // since the uid will be different
    val additionalIgnoreParams = Set("outputCol")

    val multiColTransformer = new Pipeline().setStages(Array(
      new VectorAssembler().
        setInputCols(Array("dti")).
        setOutputCol("features"),
      new Binarizer().
        setThresholds(Array(0.12, 0.12)).
        setInputCols(Array("dti", "features")).
        setOutputCols(Array("thresholded_features_double", "thresholded_features"))
    )).fit(dataset)
    val sparkTransformed = multiColTransformer.transform(baseDataset)
    implicit val sbc = SparkBundleContext().withDataset(sparkTransformed)
    val deserializedTransformer = deserializedSparkTransformer(multiColTransformer)
    checkEquality(multiColTransformer, deserializedTransformer, additionalIgnoreParams)
    equalityTest(sparkTransformed, deserializedTransformer.transform(baseDataset))

    bundleCache = None
  }
}
