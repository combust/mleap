package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorIndexerModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class VectorIndexerSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = VectorIndexer(shape = NodeShape.vector(3, 20),
        model = VectorIndexerModel(3, Map()))

      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double()),
          StructField("output", TensorType.Double())))
    }
  }
}