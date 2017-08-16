package ml.combust.mleap.core.regression

import ml.combust.mleap.core.test.TestUtil
import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class RandomForestRegressionModelSpec extends FunSpec {
  val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
  val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
  val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)

  val regression = RandomForestRegressionModel(Seq(tree1, tree2, tree3), 5)

  describe("#predict") {
    it("uses the forest to make a prediction") {
      val features = Vectors.dense(Array(0.2, 0.8, 0.4))

      assert(tree1.predict(features) == 0.5)
      assert(tree2.predict(features) == 0.75)
      assert(tree3.predict(features) == 0.1)
      assert(regression.predict(features) == (0.5 + 0.75 + 0.1) / 3)
    }
  }

  describe("input/output schema") {
    it("has the right input schema") {
      assert(regression.inputSchema.fields == Seq(StructField("features", TensorType.Double(5))))
    }

    it("has the right output schema") {
      assert(regression.outputSchema.fields == Seq(StructField("prediction", ScalarType.Double)))
    }
  }

}
