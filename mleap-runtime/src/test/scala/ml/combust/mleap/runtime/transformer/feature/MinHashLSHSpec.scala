package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types.{DoubleType, ListType, StructField, TensorType}
import org.scalatest.FunSpec

class MinHashLSHSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new MinHashLSH("transformer", "input", "output", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("input", TensorType(DoubleType())),
          StructField("output", ListType(TensorType(DoubleType())))))
    }
  }
}