package ml.combust.mleap.core.regression

import ml.combust.mleap.core.test.TestUtil
import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class RandomForestRegressionModelSpec extends FunSpec {
  describe("#predict") {
    it("uses the forest to make a prediction") {
      val tree1 = TestUtil.buildDecisionTreeRegression(.5, 0, goLeft = true)
      val tree2 = TestUtil.buildDecisionTreeRegression(.75, 1, goLeft = false)
      val tree3 = TestUtil.buildDecisionTreeRegression(.1, 2, goLeft = true)

      val regression = RandomForestRegressionModel(Seq(tree1, tree2, tree3), 5)
      val features = Vectors.dense(Array(.2, .8, .4))

      assert(tree1.predict(features) == .5)
      assert(tree2.predict(features) == .75)
      assert(tree3.predict(features) == .1)
      assert(regression.predict(features) == (.5 + .75 + .1) / 3)
    }
  }


}
