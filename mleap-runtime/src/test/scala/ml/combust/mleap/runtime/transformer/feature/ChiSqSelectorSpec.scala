package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ChiSqSelectorModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class ChiSqSelectorSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = ChiSqSelector(shape = NodeShape.feature(inputCol = "features"),
        model = new ChiSqSelectorModel(Seq(1,2,3), 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("output", TensorType(BasicType.Double, Seq(3)))))
    }
  }
}