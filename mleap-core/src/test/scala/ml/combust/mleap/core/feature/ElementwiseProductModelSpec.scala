package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by mikhail on 9/25/16.
  */
class ElementwiseProductModelSpec extends org.scalatest.funspec.AnyFunSpec{
  describe("elementwise product model") {
    val scaler = ElementwiseProductModel(Vectors.dense(Array(0.5, 1.0, 1.0)))

    it("multiplies each input vector by a provided weight vector"){
      val inputArray = Array(15.0, 10.0, 10.0)
      val expectedVector = Array(7.5, 10.0, 10.0)

      assert(scaler(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }

    it("has the right input schema") {
      assert(scaler.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(scaler.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }
  }
}
