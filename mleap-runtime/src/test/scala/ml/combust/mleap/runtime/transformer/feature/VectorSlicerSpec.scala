package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorSlicerModel
import ml.combust.mleap.core.types._

class VectorSlicerSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = VectorSlicer(shape = NodeShape.feature(),
        model = VectorSlicerModel(indices = Array(1), inputSize = 3))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(1))))
    }
  }
}