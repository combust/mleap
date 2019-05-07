package org.apache.spark.ml.mleap.parity.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.core.feature.UnaryOperation.Tan
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.mleap.feature.MathUnary
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnaryParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new MathUnary(uid = "math_unary", model = MathUnaryModel(Tan)).
      setInputCol("dti").
      setOutputCol("dti_tan")
  )).fit(dataset)

  override val unserializedParams = Set("stringOrderType")
}
