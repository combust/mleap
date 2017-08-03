package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.FunSpec

class AFTSurvivalRegressionModelSpec extends FunSpec {

  describe("aft survival regression model") {
    val model = new AFTSurvivalRegressionModel(null, 2, null, 3)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features",TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Double),
          StructField("quantiles", TensorType.Double())))
    }
  }
}
