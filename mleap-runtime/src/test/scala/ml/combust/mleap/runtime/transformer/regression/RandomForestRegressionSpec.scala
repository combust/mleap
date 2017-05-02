package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class RandomForestRegressionSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new RandomForestRegression("transformer", "features", "prediction", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}