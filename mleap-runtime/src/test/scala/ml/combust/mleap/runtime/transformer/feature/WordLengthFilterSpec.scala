package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordLengthFilterModel
import ml.combust.mleap.core.types._

class WordLengthFilterSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = WordLengthFilter(shape = NodeShape().withStandardInput("input").
        withStandardOutput("output"),
        model = new WordLengthFilterModel(5))

      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", ListType(BasicType.String))))
    }
  }
}