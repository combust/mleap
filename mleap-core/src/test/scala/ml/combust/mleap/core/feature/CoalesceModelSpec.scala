package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class CoalesceModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema"){
    val model = CoalesceModel(Seq(true, true, true))

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input0", ScalarType.Double),
          StructField("input1", ScalarType.Double),
          StructField("input2", ScalarType.Double)))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double)))
    }
  }
}
