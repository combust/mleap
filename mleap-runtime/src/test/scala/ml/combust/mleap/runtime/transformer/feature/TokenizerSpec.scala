package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types.{ListType, StringType, StructField}
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

class TokenizerSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new Tokenizer("transformer", "input", "output")
      assert(transformer.getFields().get ==
        Seq(StructField("input", StringType()),
          StructField("output", ListType(StringType()))))
    }
  }
}