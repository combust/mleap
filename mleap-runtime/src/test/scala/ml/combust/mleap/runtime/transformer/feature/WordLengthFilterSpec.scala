package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class WordLengthFilterSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new WordLengthFilter("transformer", "input", "output", null)
      assert(transformer.getFields().get ==
        Seq(StructField("input", ListType(StringType())),
          StructField("output", ListType(StringType()))))
    }
  }
}