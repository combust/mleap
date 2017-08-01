package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs without quantilesCol") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression(3),
        model = new AFTSurvivalRegressionModel(null, 23, null, 5, Seq(ScalarShape())))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with quantilesCol") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression(3)
        .withOutput("quantiles", "quantiles"),
        model = new AFTSurvivalRegressionModel(null, 23, null, 5, Seq(ScalarShape(), TensorShape(6))))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double),
          StructField("quantiles", TensorType.Double(6))))
    }
  }
}
