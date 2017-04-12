package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 4/12/17.
  */
class DCTModelSpec extends FunSpec {
  describe("#apply") {
    val model = DCTModel(false)
    describe("issue167") {
      it("should not modify input features") {
        val expected = Array(123.4, 23.4, 56.7)
        val features = Vectors.dense(Array(123.4, 23.4, 56.7))
        val dctFeatures = model(features)

        assert(features.toArray.sameElements(expected))
        assert(!features.toArray.sameElements(dctFeatures.toArray))
        assert(features.toArray != dctFeatures.toArray)
      }
    }
  }
}
