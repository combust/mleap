package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class RegexTokenizerModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("regex tokenizer model") {
    val model = RegexTokenizerModel(regex = """\s""".r, matchGaps = true,
      tokenMinLength = 3, lowercaseText = true)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.String.nonNullable)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ListType(BasicType.String))))
    }
  }
}
