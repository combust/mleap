package ml.combust.mleap.core.sklearn

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class PolynomialFeaturesModelSpec extends FunSpec {

  val model = new PolynomialFeaturesModel("[x0,x1,x0^2,x0 x1,x1^2,x0^3,x0^2 x1,x0 x1^2,x1^3]")

  describe("sklearn polynomial features") {

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(2))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(9))))
    }

    it("calculates the polynomial features based off given combinations") {
      val result = model(Vectors.dense(3, 4))
      assert(result == Vectors.dense(3.0, 4.0, 9.0, 12.0, 16.0, 27.0, 36.0, 48.0, 64.0))
    }
  }
}
