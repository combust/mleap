package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class OneVsRestSpec extends FunSpec {
  describe("#getFields") {
    it("has the correct inputs and outputs without probability column") {
      val transformer = OneVsRest(shape = NodeShape.basicClassifier(3), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }
  }

  it("has the correct inputs and outputs with probability column") {
    val transformer = OneVsRest(shape = NodeShape().withInput("features", "features", TensorType(BasicType.Double, Seq(3))).
      withOutput("probability", "prob", ScalarType.Double).
      withOutput("prediction", "prediction", ScalarType.Double), model = null)
    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
        StructField("prob", ScalarType.Double),
        StructField("prediction", ScalarType.Double)))
  }
}
