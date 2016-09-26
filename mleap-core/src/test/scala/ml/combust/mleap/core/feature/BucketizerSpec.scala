package ml.combust.mleap.core.feature

import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/19/16.
  */
class BucketizerSpec extends FunSpec {
  describe("#apply") {
    it("assigns a value to a bucket based on an input range. Bucket 1"){
      val bucketizer = BucketizerModel(Array(0.0, 10.0, 20.0, 100.0))
      val input = 11.0
      val expectedOutput = 1.0

      assert(bucketizer(input).toDouble.equals(expectedOutput))
    }

    it("assigns a value to a bucket based on an input range. Bucket -1"){
      val bucketizer = BucketizerModel(Array(0.0, 10.0, 20.0, 100.0))
      val input = 0.0
      val expectedOutput = 0.0

      assert(bucketizer(input).toDouble.equals(expectedOutput))
    }
  }
}
