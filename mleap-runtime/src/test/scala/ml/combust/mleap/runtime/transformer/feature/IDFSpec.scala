package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class IDFSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new IDF("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", TensorType(DoubleType())),
          StructField("output", TensorType(DoubleType()))))
    }
  }
}