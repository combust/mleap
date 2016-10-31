package ml.combust.mleap.core.classification

import ml.combust.mleap.core.test.TestUtil
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class GBTClassifierModelSpec extends FunSpec {
  describe("#apply") {
    val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
    val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
    val tree3 = TestUtil.buildDecisionTreeRegression(-0.1, 2, goLeft = true)

    val classifier = GBTClassifierModel(trees = Seq(tree1, tree2, tree3),
      treeWeights = Seq(0.5, 2.0, 1.0),
      numFeatures = 5)
    val features = Vectors.dense(Array(0.2, 0.8, 0.4))

    it("predicts the class based on the features") {
      assert(classifier(features) == 1.0)
    }
  }
}
