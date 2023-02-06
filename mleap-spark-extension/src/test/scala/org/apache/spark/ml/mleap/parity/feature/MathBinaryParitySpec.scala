package org.apache.spark.ml.mleap.parity.feature

import ml.combust.mleap.core.feature.BinaryOperation.Multiply
import ml.combust.mleap.core.feature.MathBinaryModel
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.mleap.feature.MathBinary
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql._

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinaryParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti", "approved")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new MathBinary(uid = "math_bin", model = MathBinaryModel(Multiply)).
      setInputA("fico_index").
      setInputB("dti").
      setOutputCol("bin_out")
  )).fit(dataset)

  describe("has valid inputs") {
    val model = MathBinaryModel(Multiply, da=Some(4.0), db=Some(5.0))
    it("Only one of inputA or defaultA") {
      val invalidSparkTransformer: Transformer = new MathBinary(uid = "math_bin", model = model).
        setInputA("dti").
        setOutputCol("bin_out")
      assertThrows[RuntimeException] {
        invalidSparkTransformer.transform(dataset)
      }
    }
    it("Only one of inputB or defaultB") {
      val invalidSparkTransformer: Transformer = new MathBinary(uid = "math_bin", model = model).
        setInputB("dti").
        setOutputCol("bin_out")
      assertThrows[RuntimeException] {
        invalidSparkTransformer.transform(dataset)
      }
    }
  }

  override val unserializedParams = Set("stringOrderType")
}
