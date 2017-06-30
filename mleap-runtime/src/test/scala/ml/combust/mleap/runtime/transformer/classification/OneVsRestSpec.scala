package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class OneVsRestSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs without probability column") {
      val transformer = new OneVsRest("transformer", "features", "prediction", None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }

  it("has the correct inputs and outputs with probability column") {
    val transformer = new OneVsRest("transformer", "features", "prediction", Some("prob"), null)
    assert(transformer.getFields().get ==
      Seq(StructField("features", TensorType(DoubleType())),
        StructField("prob", DoubleType()),
        StructField("prediction", DoubleType())))
  }
}
