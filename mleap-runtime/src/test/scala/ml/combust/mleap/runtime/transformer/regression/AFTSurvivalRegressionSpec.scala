package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.AFTSurvivalRegressionModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("input/output schema") {

    it("has the correct inputs and outputs") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression(3)
        .withOutput("quantiles", "quantiles"),
        model = new AFTSurvivalRegressionModel(null, 23, null, 5))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double),
          StructField("quantiles", TensorType.Double())))
    }
  }
}
