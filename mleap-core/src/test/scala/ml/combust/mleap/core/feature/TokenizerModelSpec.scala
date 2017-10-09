package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, StructField}
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class TokenizerModelSpec extends FunSpec {

  val tokenizer = TokenizerModel()

  describe("tokenizer") {

    it("returns the correct tokens from the given input string") {
      assert(tokenizer("hello there dude").sameElements(Array("hello", "there", "dude")))
    }

    it("has the right input schema") {
      assert(tokenizer.inputSchema.fields == Seq(StructField("input", ScalarType.String.nonNullable)))
    }

    it("has the right output schema") {
      assert(tokenizer.outputSchema.fields == Seq(StructField("output", ListType(BasicType.String))))
    }
  }
}
