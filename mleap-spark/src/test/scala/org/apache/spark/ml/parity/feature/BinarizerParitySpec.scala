package org.apache.spark.ml.parity.feature

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
}
