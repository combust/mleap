package ml.combust.mleap.core.regression

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionModelSpec extends FunSpec {
  describe("#apply") {
    it("applies the linear regression to a feature vector") {
      val regression = IsotonicRegressionModel(boundaries = Array(0.0, 4.0, 5.0, 7.0, 8.0),
        predictions = Seq(100.0, 200.0, 300.0, 400.0, 500.0),
        isotonic = true,
        featureIndex = Some(2))

      assert(regression(4.0) == 200.0)
      assert(regression(4.5) == 250.0)
      assert(regression(Vectors.dense(Array(1.0, 2.3, 7.2))) == 420.0)
    }
  }
}
