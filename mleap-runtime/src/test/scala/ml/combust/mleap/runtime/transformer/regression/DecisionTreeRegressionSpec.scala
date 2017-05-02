package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class DecisionTreeRegressionSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new DecisionTreeRegression("transformer", "features", "prediction", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}