package ml.combust.mleap.core.regression

import ml.combust.mleap.core.test.TestUtil
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class GBTRegressionModelSpec extends FunSpec {
  describe("#apply") {
    val tree1 = TestUtil.buildDecisionTreeRegression(0.5, 0, goLeft = true)
    val tree2 = TestUtil.buildDecisionTreeRegression(0.75, 1, goLeft = false)
    val tree3 = TestUtil.buildDecisionTreeRegression(0.1, 2, goLeft = true)

    val regression = GBTRegressionModel(Seq(tree1, tree2, tree3), Seq(0.5, 2.0, 1.0), 5)
    val features = Vectors.dense(Array(0.2, 0.8, 0.4))

    assert(tree1.predict(features) == 0.5)
    assert(tree2.predict(features) == 0.75)
    assert(tree3.predict(features) == 0.1)
    assert(regression.predict(features) == (0.5 * 0.5 + 0.75 * 2.0 + 0.1 * 1.0))
  }
}
