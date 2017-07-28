package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, StructField, TensorType}
import org.scalatest.FunSpec

class WordToVectorModelSpec extends FunSpec {

  describe("word to vector model") {
    val model = WordToVectorModel(Map("test" -> 1), Array(12))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double())))
    }
  }
}
