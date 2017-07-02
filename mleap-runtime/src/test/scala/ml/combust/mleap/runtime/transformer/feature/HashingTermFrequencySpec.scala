package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class HashingTermFrequencySpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new HashingTermFrequency("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType(BasicType.Double))))
    }
  }
}