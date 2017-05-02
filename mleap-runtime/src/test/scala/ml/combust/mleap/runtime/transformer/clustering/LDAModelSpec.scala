package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class LDAModelSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new LDAModel("transformer", "features", "topicDistribution", null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("topicDistribution", TensorType(DoubleType()))))
    }
  }
}