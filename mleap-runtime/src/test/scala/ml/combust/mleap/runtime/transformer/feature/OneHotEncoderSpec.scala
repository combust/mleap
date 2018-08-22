package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneHotEncoderSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = OneHotEncoder(
        shape = NodeShape.feature("input0", "output0", "input0", "output0"),
        model = OneHotEncoderModel(Array(5)))
      assert(
        transformer.schema.fields ==
          Seq(StructField("input0", ScalarType.Double.nonNullable),
              StructField("output0", TensorType.Double(5))))
    }
  }
}
