package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

class ReverseStringIndexerModelSpec extends FunSpec {

  describe("reverse string indexer model") {
    val model = new ReverseStringIndexerModel(Seq("one", "two", "three"))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ScalarType.Double.nonNullable)))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", ScalarType.String)))
    }
  }
}
