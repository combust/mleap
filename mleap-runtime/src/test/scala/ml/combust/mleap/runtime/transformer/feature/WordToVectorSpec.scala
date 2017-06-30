package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class WordToVectorSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new WordToVector("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", ListType(StringType())),
          StructField("output", TensorType(DoubleType()))))
    }
  }
}