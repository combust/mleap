package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class GeneralizedLinearRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with prediction column only") {
      val transformer = GeneralizedLinearRegression(shape = NodeShape.regression(3), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as linkPrediction column") {
      val transformer = GeneralizedLinearRegression(shape = NodeShape.regression(3).
        withOutput("link_prediction", "lp", ScalarType.Double), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double),
          StructField("lp", ScalarType.Double)))
    }
  }
}
