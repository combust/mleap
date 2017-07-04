package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DecisionTreeRegressionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = DecisionTreeRegression(shape = NodeShape.regression(3), model = null)

      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }
  }
}