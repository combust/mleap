package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ScalarType, StructField}
import org.scalatest.FunSpec
import ml.combust.mleap.core.feature.UnaryOperation.Log

class MathUnaryModelSpec extends FunSpec {

  describe("math unary model") {
    val model = MathUnaryModel(Log)

    it("Has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType(BasicType.Double))))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType(BasicType.Double))))
    }
  }
}
