package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class GeneralizedLinearRegressionSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs with prediction column only") {
      val transformer = GeneralizedLinearRegression(shape = NodeShape.regression(3),
        model = new GeneralizedLinearRegressionModel(null, 23, null,
        Seq(ScalarShape())))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as linkPrediction column") {
      val transformer = GeneralizedLinearRegression(shape = NodeShape.regression(3).
              withOutput("link_prediction", "lp"),
        model = new GeneralizedLinearRegressionModel(null, 23, null, Seq(ScalarShape(), ScalarShape())))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double()),
          StructField("prediction", ScalarType.Double),
          StructField("lp", ScalarType.Double)))
    }
  }
}
