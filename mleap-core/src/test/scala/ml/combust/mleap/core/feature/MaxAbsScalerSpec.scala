package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/18/16.
  */
class MaxAbsScalerSpec extends FunSpec{

  describe("#apply") {
    it("Scales the vector based on absolute max value"){
      val scaler = MaxAbsScalerModel(Vectors.dense(Array(20.0, 10.0, 10.0, 20.0)))

      val inputArray = Array(15.0, -5.0, 5.0, 19.0)

      val expectedVector = Array(0.75, -0.5, 0.5, 0.95)

      assert(scaler(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }
  }

}
