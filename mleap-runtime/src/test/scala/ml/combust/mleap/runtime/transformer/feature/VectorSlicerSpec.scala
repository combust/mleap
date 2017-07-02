package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class VectorSlicerSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new VectorSlicer("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", TensorType(BasicType.Double)),
          StructField("output", TensorType(BasicType.Double))))
    }
  }
}