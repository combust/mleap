package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class LDAModelSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new LDAModel("transformer", "features", "topicDistribution", null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("topicDistribution", TensorType(BasicType.Double))))
    }
  }
}