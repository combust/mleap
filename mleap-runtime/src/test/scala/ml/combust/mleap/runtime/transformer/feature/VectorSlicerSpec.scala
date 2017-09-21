package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorSlicerModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class VectorSlicerSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = VectorSlicer(shape = NodeShape.vector(3, 1),
        model = VectorSlicerModel(indices = Array(1), inputSize = 3))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(1))))
    }
  }
}