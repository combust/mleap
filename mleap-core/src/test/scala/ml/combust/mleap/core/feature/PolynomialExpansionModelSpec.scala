package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionModelSpec extends FunSpec {
  describe("polynomial expansion mode;") {
    val model = PolynomialExpansionModel(2)

    it("performs polynomial expansion on an input vector") {
      val inputArray = Array(2.0,3.0)
      val expectedVector = Array(2.0, 4.0, 3.0, 6.0, 9.0)

      assert(model(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }

    it("has the right input schema") {
      assert(model.inputSchema.fields == Seq(StructField("input", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields == Seq(StructField("output", TensorType.Double())))
    }
  }
}
