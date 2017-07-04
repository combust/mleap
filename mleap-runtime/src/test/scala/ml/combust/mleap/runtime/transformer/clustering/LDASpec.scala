package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class LDASpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = LDA(shape = NodeShape.basicCluster(3, outputType = TensorType(BasicType.Double, Seq(2))), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", TensorType(BasicType.Double, Seq(2)))))
    }
  }
}