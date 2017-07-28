package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.scalatest.FunSpec

class VectorIndexerModelSpec extends FunSpec {

  describe("vector indexer model") {
    val model = VectorIndexerModel(3, Map())

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double())))
    }
  }
}
