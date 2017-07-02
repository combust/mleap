package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs without quantilesCol") {
      val transformer = new AFTSurvivalRegression("transformer", "features", "prediction", None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with quantilesCol") {
      val transformer = new AFTSurvivalRegression("transformer", "features", "prediction", Some("quantiles"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double),
          StructField("quantiles", TensorType(BasicType.Double))))
    }
  }
}
