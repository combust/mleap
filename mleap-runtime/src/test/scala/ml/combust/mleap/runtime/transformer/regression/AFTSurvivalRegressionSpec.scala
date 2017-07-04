package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs without quantilesCol") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression(3), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with quantilesCol") {
      val transformer = AFTSurvivalRegression(shape = NodeShape.regression(3).withOutput("quantiles", "quantiles", TensorType(BasicType.Double, Seq(6))), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double),
          StructField("quantiles", TensorType(BasicType.Double, Seq(6)))))
    }
  }
}
