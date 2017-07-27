package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/18/16.
  */
class MinMaxScalerModelSpec extends FunSpec{
  describe("min max scaler model") {
    val scaler = MinMaxScalerModel(Vectors.dense(Array(1.0, 0.0, 5.0, 10.0)), Vectors.dense(Array(15.0, 10.0, 15.0, 20.0)))

    it("scales vector based on min/max range"){
      val inputArray = Array(15.0, 5.0, 5.0, 19.0)
      val expectedVector = Array(1.0, 0.5, 0.0, 0.9)

      assert(scaler(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }

    it("has the right input schema") {
      assert(scaler.inputSchema.fields == Seq(StructField("input", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(scaler.outputSchema.fields == Seq(StructField("output", TensorType.Double())))
    }
  }
}
