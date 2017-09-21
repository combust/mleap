package ml.combust.mleap.runtime.transformer.regression

import ml.combust.mleap.core.regression.RandomForestRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.test.TestUtil
import org.scalatest.FunSpec

class RandomForestRegressionSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
      val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
      val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)

      val regression = RandomForestRegressionModel(Seq(tree1, tree2, tree3), 5)

      val transformer = RandomForestRegression(shape = NodeShape.regression(5),
        model = regression)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(5)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}