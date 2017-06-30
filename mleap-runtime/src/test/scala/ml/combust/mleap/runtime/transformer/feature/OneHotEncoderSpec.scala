package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class OneHotEncoderSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new OneHotEncoder("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", DoubleType()),
          StructField("output", TensorType(DoubleType()))))
    }
  }
}