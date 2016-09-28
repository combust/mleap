package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerModelSpec extends FunSpec {
  describe("#apply") {
    it("normalizes the feature vector using the p normalization value") {
      val normalizer = NormalizerModel(20.0)
      val features = Vectors.dense(Array(0.0, 20.0, 40.0))
      val norm = normalizer(features).toArray

      assert(norm(0) < 0.0001 && norm(0) > -0.0001)
      assert(norm(1) < 0.5001 && norm(1) > 0.49999)
      assert(norm(2) < 1.0001 && norm(2) > 0.99999)
    }
  }
}
