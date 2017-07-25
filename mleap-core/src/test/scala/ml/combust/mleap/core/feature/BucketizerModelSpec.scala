package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ScalarType, StructField}
import org.scalatest.FunSpec

/**
  * Created by mikhail on 9/19/16.
  */
class BucketizerModelSpec extends FunSpec {
  describe("bucketizer") {
    val bucketizer = BucketizerModel(Array(0.0, 10.0, 20.0, 100.0))

    it("assigns a value to a bucket based on an input range"){
      val inputs = Array(11.0, 0.0, 55.0)
      val expectedOutputs = Array(1.0, 0.0, 2.0)

      assert(inputs.map(bucketizer.apply) sameElements expectedOutputs)
    }

    it("has the right input schema") {
      assert(bucketizer.inputSchema.fields ==
        Seq(StructField("input", ScalarType(BasicType.Double))))
    }

    it("has the right output schema") {
      assert(bucketizer.outputSchema.fields ==
        Seq(StructField("output", ScalarType(BasicType.Double))))
    }

    describe("with invalid feature") {
      it("throws an error") {
        assertThrows[Exception](bucketizer(-23.0))
      }
    }
  }
}
