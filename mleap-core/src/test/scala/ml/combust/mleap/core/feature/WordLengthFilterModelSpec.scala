package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class WordLengthFilterModelSpec extends FunSpec {

  describe("word length filter model") {
    val model = new WordLengthFilterModel(5)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ListType(BasicType.String))))
    }
  }

}
