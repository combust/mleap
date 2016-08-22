package ml.combust.mleap.core.regression

import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class RandomForestRegressionSpec extends FunSpec {
  describe("#predict") {
    it("uses the forest to make a prediction") {
      val tree1 = buildDecisionTree(.5, 0, goLeft = true)
      val tree2 = buildDecisionTree(.75, 1, goLeft = false)
      val tree3 = buildDecisionTree(.1, 2, goLeft = true)

      val regression = RandomForestRegressionModel(Seq(tree1, tree2, tree3), 5)
      val features = Vectors.dense(Array(.2, .8, .4))

      assert(tree1.predict(features) == .5)
      assert(tree2.predict(features) == .75)
      assert(tree3.predict(features) == .1)
      assert(regression.predict(features) == (.5 + .75 + .1) / 3)
    }
  }

  private def buildDecisionTree(prediction: Double, featureIndex: Int, goLeft: Boolean): DecisionTreeRegressionModel = {
    val node1 = LeafNode(prediction, None)
    val node2 = LeafNode(42.34, None)
    val split = ContinuousSplit(featureIndex, .5)

    val node = if(goLeft) {
      InternalNode(node1, node2, split)
    } else {
      InternalNode(node2, node1, split)
    }
    DecisionTreeRegressionModel(node, 5)
  }
}
