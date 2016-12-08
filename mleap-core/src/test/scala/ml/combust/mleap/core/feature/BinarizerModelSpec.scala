package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerModelSpec extends FunSpec {
  describe("#apply"){
    it("Makes a value 0 or 1 based on the threshold") {
      val binarizer = BinarizerModel(0.3)
      val features = Vectors.dense(Array(0.1, 0.4, 0.3))
      val binFeatures = binarizer(features).toArray

      assert(binFeatures(0) == 0.0)
      assert(binFeatures(1) == 1.0)
      assert(binFeatures(2) == 0.0)
    }
  }
}
