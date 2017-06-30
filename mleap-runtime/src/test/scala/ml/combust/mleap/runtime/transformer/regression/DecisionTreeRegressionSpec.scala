package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class DecisionTreeRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new DecisionTreeRegression("transformer", "features", "prediction", null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}