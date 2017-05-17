package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class GeneralizedLinearRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with prediction column only") {
      val transformer = new GeneralizedLinearRegression("transformer", "features", "prediction", None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with prediction column as well as linkPrediction column") {
      val transformer = new GeneralizedLinearRegression("transformer", "features", "prediction", Some("linkPrediction"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType()),
          StructField("linkPrediction", DoubleType())))
    }
  }
}
