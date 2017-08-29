package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class CoalesceModelSpec extends FunSpec {

  describe("input/output schema"){
    val model = CoalesceModel(Seq(true, true, true))

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input0", ScalarType.Double.asNullable),
          StructField("input1", ScalarType.Double.asNullable),
          StructField("input2", ScalarType.Double.asNullable)))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double.asNullable)))
    }
  }
}
