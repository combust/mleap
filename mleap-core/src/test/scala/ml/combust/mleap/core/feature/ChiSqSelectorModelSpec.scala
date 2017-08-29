package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.scalatest.FunSpec

class ChiSqSelectorModelSpec extends FunSpec {

  describe("input/output schema"){
    val model = new ChiSqSelectorModel(Seq(1,2,3), 3)

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(3))))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }
  }
}
