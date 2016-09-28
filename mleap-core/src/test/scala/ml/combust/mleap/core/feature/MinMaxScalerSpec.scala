package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/18/16.
  */
class MinMaxScalerSpec extends FunSpec{
  describe("#apply") {
    it("scales vector based on min/max range"){
      val scaler = MinMaxScalerModel(Vectors.dense(Array(1.0, 0.0, 5.0, 10.0)), Vectors.dense(Array(15.0, 10.0, 15.0, 20.0)))
      val inputArray = Array(15.0, 5.0, 5.0, 19.0)
      val expectedVector = Array(1.0, 0.5, 0.0, 0.9)

      assert(scaler(Vectors.dense(inputArray)).toArray.sameElements(expectedVector))
    }
  }
}
