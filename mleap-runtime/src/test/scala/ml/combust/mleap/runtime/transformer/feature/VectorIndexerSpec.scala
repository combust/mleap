package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorIndexerModel
import ml.combust.mleap.core.types._

class VectorIndexerSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = VectorIndexer(shape = NodeShape.feature(),
        model = VectorIndexerModel(3, Map()))

      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(3))))
    }
  }
}