package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.mleap.feature.Imputer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 1/5/17.
  */
class ImputerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti")
  override val sparkTransformer: Transformer = new Imputer(uid = "imputer").
    setInputCol("dti").
    setOutputCol("dti_imputed").
    setMissingValue(23.4).
    setStrategy("mean").fit(dataset)
}
