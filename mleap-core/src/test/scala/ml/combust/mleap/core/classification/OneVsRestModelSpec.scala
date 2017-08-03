package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.FunSpec

class OneVsRestModelSpec extends FunSpec {

  describe("one vs rest model") {
    val model = new OneVsRestModel(null, 3)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(
          StructField("probability", ScalarType.Double),
          StructField("prediction", ScalarType.Double)
        ))
    }
  }
}
