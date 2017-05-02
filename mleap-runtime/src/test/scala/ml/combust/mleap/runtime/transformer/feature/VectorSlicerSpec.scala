package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class VectorSlicerSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new VectorSlicer("transformer", "input", "output", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("input", TensorType(DoubleType())),
          StructField("output", TensorType(DoubleType()))))
    }
  }
}