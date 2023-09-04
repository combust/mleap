package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MinHashLSHModel
import ml.combust.mleap.core.types._

class MinHashLSHSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = MinHashLSH(shape = NodeShape.feature(),
        model = MinHashLSHModel(Seq(), 3))

      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(3, 1))))
    }
  }
}