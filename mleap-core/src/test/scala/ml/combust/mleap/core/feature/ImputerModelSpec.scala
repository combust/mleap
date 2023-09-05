package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

class ImputerModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema"){
    val model = ImputerModel(12, 23.4, "mean")

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.Double)))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double.nonNullable)))
    }
  }
}
