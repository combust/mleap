package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class StringMapModelSpec extends FunSpec {

  describe("string map model") {
    val model = StringMapModel(Map("index1" -> 1.0, "index2" -> 1.0, "index3" -> 2.0))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double)))
    }
  }
}
