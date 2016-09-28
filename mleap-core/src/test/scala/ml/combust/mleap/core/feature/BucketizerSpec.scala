package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/19/16.
  */
class BucketizerSpec extends FunSpec {
  describe("#apply") {
    val bucketizer = BucketizerModel(Array(0.0, 10.0, 20.0, 100.0))

    it("assigns a value to a bucket based on an input range"){
      val inputs = Array(11.0, 0.0, 55.0)
      val expectedOutputs = Array(1.0, 0.0, 2.0)

      assert(inputs.map(bucketizer.apply) sameElements expectedOutputs)
    }

    describe("with invalid feature") {
      it("throws an error") {
        assertThrows[Exception](bucketizer(-23.0))
      }
    }
  }
}
