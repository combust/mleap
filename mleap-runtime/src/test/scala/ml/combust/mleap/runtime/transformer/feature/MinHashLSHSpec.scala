package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MinHashLSHModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class MinHashLSHSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = MinHashLSH(shape = NodeShape().
                    withStandardInput("input").
              withStandardOutput("output"), model = MinHashLSHModel(Seq()))

      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double()),
          StructField("output", TensorType.Double())))
    }
  }
}