package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class RandomForestRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new RandomForestRegression("transformer", "features", "prediction", null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }
  }
}