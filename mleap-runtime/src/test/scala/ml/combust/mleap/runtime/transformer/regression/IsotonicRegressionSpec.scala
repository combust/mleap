package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class IsotonicRegressionSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs without feature index") {
      val transformer = IsotonicRegression(shape = NodeShape.regression(),
      model = IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
        predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
        isotonic = true,
        featureIndex = None))
      assert(transformer.schema.fields ==
        Seq(StructField("features", ScalarType.Double.nonNullable),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with feature index") {
      val transformer = IsotonicRegression(shape = NodeShape.regression(),
        model = IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
          predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
          isotonic = true,
          featureIndex = Some(2)))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
