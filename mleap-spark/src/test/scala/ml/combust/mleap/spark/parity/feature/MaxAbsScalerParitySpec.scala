package ml.combust.mleap.spark.parity.feature

import ml.combust.mleap.spark.parity.SparkParityBase
import org.apache.spark.ml.feature.{MaxAbsScaler, VectorAssembler}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class MaxAbsScalerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("dti", "loan_amount")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new VectorAssembler().
    setInputCols(Array("dti", "loan_amount")).
    setOutputCol("features"),
    new MaxAbsScaler().
      setInputCol("features").
      setOutputCol("scaled_features"))).fit(dataset)
}
