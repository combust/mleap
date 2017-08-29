package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.OneVsRestModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneVsRestSpec extends FunSpec {
  describe("input/output schema") {
    it("has the correct inputs and outputs without probability column") {
      val transformer = OneVsRest(shape = NodeShape.basicClassifier(3),
        model = new OneVsRestModel(null, 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double)))
    }
  }

  it("has the correct inputs and outputs with probability column") {
    val transformer = OneVsRest(shape = NodeShape().withInput("features", "features").
          withOutput("probability", "prob").
          withOutput("prediction", "prediction"),
          model = new OneVsRestModel(null, 3))
    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType.Double(3)),
        StructField("prob", ScalarType.Double),
        StructField("prediction", ScalarType.Double)))
  }
}
