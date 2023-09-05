package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.TokenizerModel
import ml.combust.mleap.core.types._

class TokenizerSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input") {
    it("has the correct inputs and outputs") {
      val transformer = Tokenizer(shape = NodeShape().withStandardInput("input").
        withStandardOutput("output"), model = TokenizerModel.defaultTokenizer)
      assert(transformer.schema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable),
          StructField("output", ListType(BasicType.String))))
    }
  }
}