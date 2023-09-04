package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.scalatest.funspec.AnyFunSpec

class VectorIndexerModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("vector indexer model") {
    val model = VectorIndexerModel(3, Map())

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }
  }
}
