package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class StandardScalerSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = StandardScaler(shape = NodeShape.vector(3, 3), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType(BasicType.Double, Seq(3))),
          StructField("output", TensorType(BasicType.Double, Seq(3)))))
    }
  }
}