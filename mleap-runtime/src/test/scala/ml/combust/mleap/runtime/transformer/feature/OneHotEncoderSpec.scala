package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneHotEncoderSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs in a single-input/output context") {
      val transformer = OneHotEncoder(
        shape = NodeShape.feature("input0", "output0", "input0", "output0"),
        model = OneHotEncoderModel(Array(5)))
      assert(
        transformer.schema.fields ==
          Seq(StructField("input0", ScalarType.Double.nonNullable),
              StructField("output0", TensorType.Double(5))))
    }

    it("has the correct inputs and output in a multi-input/output context") {
      val shp = NodeShape()
        .withInput("input0", "input0")
        .withInput("input1", "input1")
        .withOutput("output0", "output0")
        .withOutput("output1", "output1")
      val transformer = OneHotEncoder(shape = shp, model = OneHotEncoderModel(Array(5)))
      assert(transformer.schema.fields ==
        Seq(StructField("input0", ScalarType.Double.nonNullable),
          StructField("output0", TensorType.Double(5))))
    }
  }
}
