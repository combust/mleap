package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class InteractionSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = Interaction(shape = NodeShape().
                    withInput("input0", "feature1").
                    withInput("input1", "feature2").
              withStandardOutput("features"),
        model = InteractionModel(Array(Array(1), Array(1, 1), Array(2, 2)),
          Seq(ScalarShape(), TensorShape(2), TensorShape(2))))

      assert(transformer.schema.fields ==
        Seq(StructField("feature1", ScalarType.Double),
          StructField("feature2", TensorType(BasicType.Double, Some(Seq(2)))),
          StructField("features", TensorType(BasicType.Double, Some(Seq(2))))))
    }
  }
}
