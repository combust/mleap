package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types.{DoubleType, ListType, StructField, TensorType}
import org.scalatest.FunSpec

class BucketedRandomProjectionLSHSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new BucketedRandomProjectionLSH("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", TensorType(DoubleType())),
          StructField("output", ListType(TensorType(DoubleType())))))
    }
  }
}