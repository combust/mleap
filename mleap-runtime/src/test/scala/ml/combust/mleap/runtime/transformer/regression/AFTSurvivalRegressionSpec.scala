package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class AFTSurvivalRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs without quantilesCol") {
      val transformer = new AFTSurvivalRegression("transformer", "features", "prediction", None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with quantilesCol") {
      val transformer = new AFTSurvivalRegression("transformer", "features", "prediction", Some("quantiles"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType()),
          StructField("quantiles", TensorType(DoubleType()))))
    }
  }
}
