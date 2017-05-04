package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class WordLengthFilterSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new WordLengthFilter("transformer", "input", "output", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("input", ListType(StringType())),
          StructField("output", ListType(StringType()))))
    }
  }
}