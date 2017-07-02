package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class MultiLayerPerceptronClassifierSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer =
        new MultiLayerPerceptronClassifier("transformer", "features", "prediction", null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }
  }
}