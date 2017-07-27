package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ScalarShape, ScalarType, StructField}
import org.scalatest.FunSpec

class HashingTermFrequencyModelSpec extends FunSpec {

  describe("Hashing Term Frequency Model") {
    val binarizer = BinarizerModel(0.3, ScalarShape())

    it("Has the right input schema") {
      assert(binarizer.inputSchema.fields ==
        Seq(StructField("input", ScalarType(BasicType.Double))))
    }

    it("Has the right output schema") {
      assert(binarizer.outputSchema.fields ==
        Seq(StructField("output", ScalarType(BasicType.Double))))
    }
  }
}
