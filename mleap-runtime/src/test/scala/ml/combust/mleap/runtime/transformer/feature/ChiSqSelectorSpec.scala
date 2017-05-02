package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class ChiSqSelectorSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new ChiSqSelector("transformer", "features", "output", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("output", TensorType(DoubleType()))))
    }
  }
}