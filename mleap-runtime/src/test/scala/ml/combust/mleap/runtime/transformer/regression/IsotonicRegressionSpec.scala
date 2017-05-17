package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class IsotonicRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs without feature index") {
      val transformer = new IsotonicRegression("transformer", "features", "prediction",
      IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
        predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
        isotonic = true,
        featureIndex = None))
      assert(transformer.getFields().get ==
        Seq(StructField("features", DoubleType()),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with feature index") {
      val transformer = new IsotonicRegression("transformer", "features", "prediction",
        IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
          predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
          isotonic = true,
          featureIndex = Some(2)))
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}
