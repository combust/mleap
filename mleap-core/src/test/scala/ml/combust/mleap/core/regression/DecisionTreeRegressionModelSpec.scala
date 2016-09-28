package ml.combust.mleap.core.regression

import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class DecisionTreeRegressionModelSpec extends FunSpec {
  describe("#predict") {
    it("returns the prediction for the decision tree") {
      val leftNode = LeafNode(.78, None)
      val rightNode = LeafNode(.34, None)
      val split = ContinuousSplit(0, .5)
      val node = InternalNode(leftNode, rightNode, split)
      val features = Vectors.dense(Array(0.3, 1.0, 43.23, -21.2, 66.7))
      val regression = DecisionTreeRegressionModel(node, 5)

      assert(regression.predict(features) == .78)
    }
  }
}
