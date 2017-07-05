package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class MinHashLSHSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = MinHashLSH(shape = NodeShape().
                    withStandardInput("input").
              withStandardOutput("output"), model = null)

      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType(BasicType.Double, Seq(3))),
          StructField("output", TensorType(BasicType.Double, Seq(3, 1)))))
    }
  }
}