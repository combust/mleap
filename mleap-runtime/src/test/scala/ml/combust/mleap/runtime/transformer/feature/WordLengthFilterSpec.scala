package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class WordLengthFilterSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = WordLengthFilter(shape = NodeShape().withStandardInput("input", ListType(BasicType.String)).
        withStandardOutput("output", ListType(BasicType.String)), model = null)

      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", ListType(BasicType.String))))
    }
  }
}