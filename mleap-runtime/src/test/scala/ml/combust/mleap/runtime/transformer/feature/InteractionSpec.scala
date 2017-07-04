package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class InteractionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = Interaction("transformer", Array("feature1", "feature2"), "features",
        InteractionModel(Array(Array(1), Array(2)), Seq(ScalarShape(), TensorShape(2), TensorShape(2))))
      assert(transformer.getFields().get ==
        Seq(StructField("feature1", ScalarType.Double),
          StructField("feature2", TensorType(BasicType.Double)),
          StructField("features", TensorType(BasicType.Double))))
    }
  }
}
