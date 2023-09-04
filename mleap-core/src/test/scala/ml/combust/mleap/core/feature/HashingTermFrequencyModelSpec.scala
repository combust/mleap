package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class HashingTermFrequencyModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("Hashing Term Frequency Model") {
    val model = HashingTermFrequencyModel()

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType(BasicType.String))))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(262144))))
    }
  }
}
