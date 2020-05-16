package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

import org.apache.spark.ml.util.TestingUtils._

/**
  * Created by mikhail on 9/18/16.
  */
class MaxAbsScalerModelSpec extends FunSpec {
  describe("Max Abs Scaler Model") {
    val scaler = MaxAbsScalerModel(Vectors.dense(Array(20.0, 10.0, 10.0, 20.0)))

    it("Scales the vector based on absolute max value"){
      val inputVector = Vectors.dense(15.0, -5.0, 5.0, 19.0)
      val expectedVector = Vectors.dense(0.75, -0.5, 0.5, 0.95)
      assert(scaler(inputVector) ~= expectedVector relTol 1E-9)
    }

    it("Has the right input schema") {
      assert(scaler.inputSchema.fields == Seq(StructField("input", TensorType.Double(4))))
    }

    it("Has the right output schema") {
      assert(scaler.outputSchema.fields == Seq(StructField("output", TensorType.Double(4))))
    }
  }
}
