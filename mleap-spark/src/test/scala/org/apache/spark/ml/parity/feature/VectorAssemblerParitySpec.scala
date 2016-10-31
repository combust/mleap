package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class VectorAssemblerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti", "loan_amount")
  override val sparkTransformer: Transformer = new VectorAssembler().
    setInputCols(Array("dti", "loan_amount")).
    setOutputCol("features")
}
