package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class TokenizerSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new Tokenizer("transformer", "input", "output")
      assert(transformer.getSchema().get ==
        Seq(StructField("input", StringType()),
          StructField("output", ListType(StringType()))))
    }
  }
}