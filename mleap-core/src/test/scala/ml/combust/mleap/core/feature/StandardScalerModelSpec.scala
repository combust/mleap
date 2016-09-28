package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class StandardScalerModelSpec extends FunSpec {
  describe("#apply") {
    describe("with mean") {
      it("scales based off of the mean") {
        val scaler = StandardScalerModel(None, Some(Vectors.dense(Array(50.0, 20.0, 30.0))))
        val expectedVector = Array(5.0, 5.0, 3.0)

        assert(scaler(Vectors.dense(Array(55.0, 25.0, 33.0))).toArray.sameElements(expectedVector))
      }
    }

    describe("with stdev") {
      it("scales based off the standard deviation") {
        val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))), None)
        val expectedVector = Array(1.6, .4375, 1.0)

        assert(scaler(Vectors.dense(Array(4.0, 3.5, 10.0))).toArray.sameElements(expectedVector))
      }
    }

    describe("with mean and stdev") {
      it("scales based off the mean and standard deviation") {
        val scaler = StandardScalerModel(Some(Vectors.dense(Array(2.5, 8.0, 10.0))),
          Some(Vectors.dense(Array(50.0, 20.0, 30.0))))
        val expectedVector = Array(1.6, .4375, 1.0)

        assert(scaler(Vectors.dense(Array(54.0, 23.5, 40.0))).toArray.sameElements(expectedVector))
      }
    }
  }
}
