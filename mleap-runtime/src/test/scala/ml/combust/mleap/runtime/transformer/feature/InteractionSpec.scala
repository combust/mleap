package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class InteractionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new Interaction("transformer", Array("feature1", "feature2"),
        Some(Array(DoubleType(), TensorType(DoubleType()))), "features" , null)
      assert(transformer.getFields().get ==
        Seq(StructField("feature1", DoubleType()),
          StructField("feature2", TensorType(DoubleType())),
          StructField("features", TensorType(DoubleType()))))
    }
  }
}
