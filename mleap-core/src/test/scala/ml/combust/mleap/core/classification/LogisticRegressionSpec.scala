package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 5/23/17.
  */
class LogisticRegressionSpec extends FunSpec {
  describe("BinaryLogisticRegression") {
    describe("issue210: Logistic function not being applied") {
      it("applies the logit function for prediction") {
        val weights = Vectors.dense(1.0, 2.0, 4.0)
        val intercept = 0.7
        val features = Vectors.dense(-1.0, 1.0, -0.5)
        val lr = BinaryLogisticRegressionModel(weights, intercept, 0.4)

        assert(lr.predict(features) == 1.0)
      }
    }
  }
}
