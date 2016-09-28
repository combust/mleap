package ml.combust.mleap.core.test

import ml.combust.mleap.core.regression.DecisionTreeRegressionModel
import ml.combust.mleap.core.tree.{ContinuousSplit, InternalNode, LeafNode, Node}

/**
  * Created by hollinwilkins on 9/28/16.
  */
object TestUtil {
  def buildDecisionTreeRegression(prediction: Double,
                                  featureIndex: Int,
                                  goLeft: Boolean): DecisionTreeRegressionModel = {
    DecisionTreeRegressionModel(buildTri(prediction, featureIndex, goLeft), 5)
  }

  def buildTri(prediction: Double, featureIndex: Int, goLeft: Boolean): Node = {
    val node1 = LeafNode(prediction, None)
    val node2 = LeafNode(42.34, None)
    val split = ContinuousSplit(featureIndex, .5)

    if(goLeft) {
      InternalNode(node1, node2, split)
    } else {
      InternalNode(node2, node1, split)
    }
  }
}
