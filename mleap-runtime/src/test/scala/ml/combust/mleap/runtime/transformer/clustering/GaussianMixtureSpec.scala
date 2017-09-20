package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class GaussianMixtureSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = GaussianMixture(shape = NodeShape.probabilisticCluster(3),
        model = new GaussianMixtureModel(Array(null, null), Array(1, 2, 3)))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Int.nonNullable)))
    }

    it("has the correct inputs and outputs with only prediction column as well as probability column") {
      val transformer = GaussianMixture(shape = NodeShape.probabilisticCluster(3, probabilityCol = Some("probability")),
        model = new GaussianMixtureModel(Array(null, null, null, null), Array(1, 2, 3)))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Int.nonNullable),
          StructField("probability", TensorType.Double(4))))
    }
  }
}
