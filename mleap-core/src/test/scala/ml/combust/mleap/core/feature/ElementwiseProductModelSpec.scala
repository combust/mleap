package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/25/16.
  */
class ElementwiseProductModelSpec extends FunSpec{
  describe("#apply") {
    it("multiplies each input vector by a provided weight vector"){
      val scaler = ElementwiseProductModel(Vectors.dense(Array(0.5, 1.0, 1.0)))
      val inputArray = Array(15.0, 10.0, 10.0)
      val expectedVector = Array(7.5, 10.0, 10.0)

      assert(scaler(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }
  }
}
