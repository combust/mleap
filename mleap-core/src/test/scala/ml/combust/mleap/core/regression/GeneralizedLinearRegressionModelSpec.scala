package ml.combust.mleap.core.regression

import ml.combust.mleap.core.types.{ScalarShape, ScalarType, StructField, TensorType}
import org.scalatest.FunSpec

class GeneralizedLinearRegressionModelSpec extends FunSpec {

  describe("generalized linear regression model") {
    val model = new GeneralizedLinearRegressionModel(null, 23, null)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features",TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Double),
           StructField("link_prediction", ScalarType.Double)))
    }
  }
}