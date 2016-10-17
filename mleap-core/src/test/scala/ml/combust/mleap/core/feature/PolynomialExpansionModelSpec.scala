package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionModelSpec extends FunSpec {
  describe("#apply") {
    it("performs polynomial expansion on an input vector") {
      val transformer = PolynomialExpansionModel(2)
      val inputArray = Array(2.0,3.0)
      val expectedVector = Array(2.0, 4.0, 3.0, 6.0, 9.0)

      assert(transformer(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }
  }
}
