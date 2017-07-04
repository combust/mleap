package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class GaussianMixtureSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = GaussianMixture(shape = NodeShape.probabilisticCluster(3), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Int)))
    }

    it("has the correct inputs and outputs with only prediction column as well as probability column") {
      val transformer = GaussianMixture(shape = NodeShape.probabilisticCluster(3, probabilityCol = Some("probability")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", ScalarType.Double),
          StructField("prediction", ScalarType.Int)))
    }
  }
}
